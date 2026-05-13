from pathlib import Path

from prefect import flow, task

from etl_spark import (
    build_spark_session,
    extract_complaints,
    extract_locations,
    extract_population,
    load_duckdb,
    transform_data,
)


@task
def extract_complaints_task() -> Path:
    spark = build_spark_session()
    try:
        return extract_complaints(spark)
    finally:
        spark.stop()


@task
def extract_population_task() -> Path:
    spark = build_spark_session()
    try:
        return extract_population(spark)
    finally:
        spark.stop()


@task
def extract_locations_task() -> Path:
    spark = build_spark_session()
    try:
        return extract_locations(spark)
    finally:
        spark.stop()


@task
def transform_task(complaints_path: Path, population_path: Path, location_path: Path) -> Path:
    spark = build_spark_session()
    try:
        return transform_data(spark, complaints_path, population_path, location_path)
    finally:
        spark.stop()


@task
def load_duckdb_task(parquet_path: Path) -> Path:
    return load_duckdb(parquet_path)


@flow(name="Consumer Complaints ETL Flow")
def consumer_complaints_etl_flow() -> None:
    complaints_path = extract_complaints_task()
    population_path = extract_population_task()
    location_path = extract_locations_task()
    final_path = transform_task(complaints_path, population_path, location_path)
    load_duckdb_task(final_path)


if __name__ == "__main__":
    consumer_complaints_etl_flow()
