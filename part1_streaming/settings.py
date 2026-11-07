import glob
import os
import sys

from pyspark.sql import SparkSession

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JARS_DIR = os.path.join(ROOT_DIR, "jars")


def load_env(path=os.path.join(ROOT_DIR, ".env")):
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())


load_env()
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]

JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/olympic_dataset"
JDBC_OPTIONS = {
    "url": JDBC_URL,
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
}

OUTPUT_TABLE = "roman_mirzoian.athlete_enriched_agg"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENT_RESULTS_TOPIC = "athlete_event_results"
ENRICHED_TOPIC = "athlete_enriched_agg"

CHECKPOINT_DIR = os.path.join(ROOT_DIR, "checkpoints")


def create_spark(app_name):
    jars = os.pathsep.join(glob.glob(os.path.join(JARS_DIR, "*.jar")))
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.extraClassPath", jars)
        .config("spark.executor.extraClassPath", jars)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
    )
    # Без winutils.exe Hadoop на Windows не може змінювати права доступу до файлів checkpoint-ів
    if os.name == "nt":
        builder = (
            builder
            .config("spark.hadoop.fs.file.impl", "local.windows.NoPermissionLocalFileSystem")
            .config(
                "spark.sql.streaming.checkpointFileManagerClass",
                "org.apache.spark.sql.execution.streaming.checkpointing.FileSystemBasedCheckpointFileManager",
            )
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
