import os
import shutil

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from settings import (
    CHECKPOINT_DIR,
    ENRICHED_TOPIC,
    EVENT_RESULTS_TOPIC,
    JDBC_OPTIONS,
    KAFKA_BOOTSTRAP_SERVERS,
    OUTPUT_TABLE,
    create_spark,
)

spark = create_spark("AthleteStreamingPipeline")
shutil.rmtree(CHECKPOINT_DIR, ignore_errors=True)

# Етап 1: зчитування біологічних даних атлетів з MySQL
athlete_bio_df = (
    spark.read.format("jdbc")
    .options(**JDBC_OPTIONS, dbtable="athlete_bio")
    .load()
)

# Етап 2: фільтрація записів, де зріст або вага порожні чи не є числами
athlete_bio_df = (
    athlete_bio_df
    .withColumn("height", F.expr("try_cast(height AS DOUBLE)"))
    .withColumn("weight", F.expr("try_cast(weight AS DOUBLE)"))
    .where(F.col("height").isNotNull() & F.col("weight").isNotNull())
    .select("athlete_id", "sex", "height", "weight")
)

# Етап 3: зчитування результатів змагань з Kafka-топіку і розбір JSON на колонки
event_schema = StructType([
    StructField("edition", StringType()),
    StructField("edition_id", IntegerType()),
    StructField("country_noc", StringType()),
    StructField("sport", StringType()),
    StructField("event", StringType()),
    StructField("result_id", IntegerType()),
    StructField("athlete", StringType()),
    StructField("athlete_id", IntegerType()),
    StructField("pos", StringType()),
    StructField("medal", StringType()),
    StructField("isTeamSport", StringType()),
])

event_results_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", EVENT_RESULTS_TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 60000)
    .load()
    .select(F.from_json(F.col("value").cast("string"), event_schema).alias("data"))
    .select("data.*")
)

# Етап 4: об'єднання результатів змагань з біологічними даними за athlete_id
enriched_stream = event_results_stream.join(
    athlete_bio_df.withColumn("athlete_id", F.col("athlete_id").cast("int")),
    on="athlete_id",
    how="inner",
)

# Етап 5: середній зріст і вага для кожного виду спорту, медалі (або її відсутності), статі та країни
aggregated_stream = (
    enriched_stream
    .withColumn("medal", F.coalesce(F.col("medal"), F.lit("nan")))
    .groupBy("sport", "medal", "sex", "country_noc")
    .agg(
        F.avg("height").alias("avg_height"),
        F.avg("weight").alias("avg_weight"),
    )
    .withColumn("timestamp", F.current_timestamp())
)


def write_batch(batch_df, batch_id):
    batch_df.persist()
    print(f"Batch {batch_id}: {batch_df.count()} rows")

    # Етап 6.а): запис у вихідний Kafka-топік
    (
        batch_df
        .select(F.to_json(F.struct(*batch_df.columns)).alias("value"))
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", ENRICHED_TOPIC)
        .save()
    )

    # Етап 6.b): запис у базу даних
    (
        batch_df
        .write
        .format("jdbc")
        .options(**JDBC_OPTIONS, dbtable=OUTPUT_TABLE)
        .mode("append")
        .save()
    )

    batch_df.unpersist()


query = (
    aggregated_stream
    .writeStream
    .foreachBatch(write_batch)
    .outputMode("update")
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "athlete_enriched_agg"))
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
