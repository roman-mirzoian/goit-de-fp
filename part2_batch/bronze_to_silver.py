import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

TABLES = ["athlete_bio", "athlete_event_results"]
NUMERIC_COLUMNS = {"height", "weight"}


def clean_text(text):
    if text is None:
        return None
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))


clean_text_udf = udf(clean_text, StringType())

spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

for table in TABLES:
    df = spark.read.parquet(f"bronze/{table}")

    # Зріст і вага - числові показники: значення на кшталт "58-60" стають null, а не "5860"
    for col_name in NUMERIC_COLUMNS & set(df.columns):
        df = df.withColumn(col_name, F.expr(f"try_cast({col_name} AS DOUBLE)"))

    for col_name, col_type in df.dtypes:
        if col_type == "string":
            df = df.withColumn(col_name, clean_text_udf(df[col_name]))

    df = df.dropDuplicates()
    df.write.mode("overwrite").parquet(f"silver/{table}")

    print(f"silver/{table}: {df.count()} rows")
    df.show()

spark.stop()
