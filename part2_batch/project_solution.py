import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dags_dir = os.path.dirname(os.path.abspath(__file__))

with DAG(
    dag_id="roman_mirzoian_fp_datalake",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["roman_mirzoian"],
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        application=os.path.join(dags_dir, "landing_to_bronze.py"),
        task_id="landing_to_bronze",
        conn_id="spark-default",
        verbose=1,
    )

    bronze_to_silver = SparkSubmitOperator(
        application=os.path.join(dags_dir, "bronze_to_silver.py"),
        task_id="bronze_to_silver",
        conn_id="spark-default",
        verbose=1,
    )

    silver_to_gold = SparkSubmitOperator(
        application=os.path.join(dags_dir, "silver_to_gold.py"),
        task_id="silver_to_gold",
        conn_id="spark-default",
        verbose=1,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
