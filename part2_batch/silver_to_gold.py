from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

athlete_bio_df = spark.read.parquet("silver/athlete_bio")
event_results_df = spark.read.parquet("silver/athlete_event_results")

athlete_bio_df = (
    athlete_bio_df
    .select(
        "athlete_id",
        "sex",
        F.col("height").cast("double").alias("height"),
        F.col("weight").cast("double").alias("weight"),
    )
    .where(F.col("height").isNotNull() & F.col("weight").isNotNull())
)

avg_stats_df = (
    event_results_df
    .join(athlete_bio_df, on="athlete_id", how="inner")
    .groupBy("sport", "medal", "sex", "country_noc")
    .agg(
        F.avg("weight").alias("avg_weight"),
        F.avg("height").alias("avg_height"),
    )
    .withColumn("timestamp", F.current_timestamp())
)

avg_stats_df.write.mode("overwrite").parquet("gold/avg_stats")

print(f"gold/avg_stats: {avg_stats_df.count()} rows")
avg_stats_df.show()

spark.stop()
