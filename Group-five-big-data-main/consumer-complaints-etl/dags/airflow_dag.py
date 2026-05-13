import sys
from datetime import datetime, timedelta
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from airflow import DAG
from airflow.operators.python import PythonOperator
from src.etl_spark import (
    extract_complaints,
    extract_population,
    extract_locations,
    transform_data,
    load_duckdb,
    export_outputs,
    build_spark_session,
)

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def run_extract():
    spark = build_spark_session()
    extract_complaints(spark)
    extract_population(spark)
    extract_locations(spark)
    spark.stop()

def run_transform():
    from src.etl_spark import COMPLAINTS_PARQUET, POPULATION_PARQUET, LOCATION_STAGING_PARQUET
    spark = build_spark_session()
    transform_data(spark, COMPLAINTS_PARQUET, POPULATION_PARQUET, LOCATION_STAGING_PARQUET)
    spark.stop()

def run_load():
    from src.etl_spark import FINAL_PARQUET, DUCKDB_FILE
    from pathlib import Path
    spark = build_spark_session()
    load_duckdb(Path(FINAL_PARQUET))
    export_outputs(spark, Path(FINAL_PARQUET))
    spark.stop()

with DAG(
    dag_id="consumer_complaints_etl",
    default_args=DEFAULT_ARGS,
    description="Consumer complaints ETL — extract, transform, load",
    schedule_interval="@daily",
    start_date=datetime(2026, 5, 11),
    catchup=False,
    tags=["consumer", "etl", "duckdb"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=run_load,
    )

    extract_task >> transform_task >> load_task