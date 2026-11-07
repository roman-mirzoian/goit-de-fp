"""Завантаження athlete_bio з FTP курсу в локальну MySQL (olympic_dataset.athlete_bio)."""
import os

import requests

from settings import JDBC_OPTIONS, ROOT_DIR, create_spark

URL = "https://ftp.goit.study/neoversity/athlete_bio.csv"
CSV_PATH = os.path.join(ROOT_DIR, "data", "athlete_bio.csv")

if not os.path.exists(CSV_PATH):
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    response = requests.get(URL)
    response.raise_for_status()
    with open(CSV_PATH, "wb") as file:
        file.write(response.content)

spark = create_spark("LoadAthleteBio")

df = spark.read.csv(CSV_PATH, header=True, multiLine=True, escape='"')

df.write.format("jdbc").options(**JDBC_OPTIONS, dbtable="athlete_bio").mode("overwrite").save()

print(f"athlete_bio: {df.count()} rows")
spark.stop()
