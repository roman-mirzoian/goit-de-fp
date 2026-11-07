import os

import requests
from pyspark.sql import SparkSession

FTP_URL = "https://ftp.goit.study/neoversity/"
TABLES = ["athlete_bio", "athlete_event_results"]


def download_data(local_file_path):
    downloading_url = FTP_URL + os.path.basename(local_file_path) + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    if response.status_code == 200:
        with open(local_file_path + ".csv", "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}.csv")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")


spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

os.makedirs("landing", exist_ok=True)

for table in TABLES:
    local_path = os.path.join("landing", table)
    download_data(local_path)

    df = spark.read.csv(f"{local_path}.csv", header=True, multiLine=True, escape='"')
    df.write.mode("overwrite").parquet(f"bronze/{table}")

    print(f"bronze/{table}: {df.count()} rows")
    df.show()

spark.stop()
