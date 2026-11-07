from pyspark.sql import functions as F

from settings import EVENT_RESULTS_TOPIC, JDBC_OPTIONS, KAFKA_BOOTSTRAP_SERVERS, create_spark

spark = create_spark("EventResultsToKafka")

# Етап 3: зчитування athlete_event_results з MySQL і запис у Kafka-топік athlete_event_results
event_results_df = (
    spark.read.format("jdbc")
    .options(**JDBC_OPTIONS, dbtable="athlete_event_results")
    .load()
)

(
    event_results_df
    .select(
        F.col("athlete_id").cast("string").alias("key"),
        F.to_json(F.struct(*event_results_df.columns)).alias("value"),
    )
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("topic", EVENT_RESULTS_TOPIC)
    .save()
)

print(f"Sent {event_results_df.count()} records to '{EVENT_RESULTS_TOPIC}'")
spark.stop()
