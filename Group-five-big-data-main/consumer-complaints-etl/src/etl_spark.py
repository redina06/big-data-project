import os
import sys
import json
from pathlib import Path

# Setup Spark environment before importing PySpark
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] = r"C:\Program Files\Java\jdk-17\bin" + ";" + os.environ.get("PATH", "")
os.environ["SPARK_HOME"] = r"C:\Users\Hybrid Technology\Downloads\spark-4.1.1-bin-hadoop3\spark-4.1.1-bin-hadoop3"

spark_python = os.path.join(os.environ["SPARK_HOME"], "python")
py4j = os.path.join(os.environ["SPARK_HOME"], "python", "lib", "py4j-0.10.9.9-src.zip")
pyspark_zip = os.path.join(os.environ["SPARK_HOME"], "python", "lib", "pyspark.zip")
sys.path.insert(0, spark_python)
sys.path.insert(0, py4j)
sys.path.insert(0, pyspark_zip)

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False
    duckdb = None

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
DATA_DIR = ROOT_DIR / "data" / "raw"
OUTPUT_DIR = ROOT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

COMPLAINTS_CSV = DATA_DIR / "Consumer_Complaints.csv"
POPULATION_JSON = DATA_DIR / "us-states-population.json"
LOCATION_PARQUET = DATA_DIR / "US_States_Long_Lat.parquet"

COMPLAINTS_PARQUET = OUTPUT_DIR / "complaints.parquet"
POPULATION_PARQUET = OUTPUT_DIR / "population.parquet"
LOCATION_STAGING_PARQUET = OUTPUT_DIR / "locations.parquet"
FINAL_PARQUET = OUTPUT_DIR / "consumer_insights.parquet"
DUCKDB_FILE = OUTPUT_DIR / "consumer_analytics.duckdb"
CSV_EXPORT = OUTPUT_DIR / "consumer_insights.csv"
JSON_EXPORT = OUTPUT_DIR / "consumer_insights.json"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ConsumerComplaintsETL")
        .master("local[2]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def extract_complaints(spark: SparkSession) -> Path:
    csv_path = COMPLAINTS_CSV.as_posix()
    print(f"⏳ Reading complaints from {csv_path}")
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(csv_path)
    )
    count = df.count()
    print(f"✅ Extracted {count} complaints")
    df.write.mode("overwrite").parquet(COMPLAINTS_PARQUET.as_posix())
    print(f"✅ Wrote complaints parquet: {COMPLAINTS_PARQUET}")
    return COMPLAINTS_PARQUET


def extract_population(spark: SparkSession) -> Path:
    with open(POPULATION_JSON, "r", encoding="utf-8") as handle:
        population_data = json.load(handle)

    rows = [
        {"state_abbrv": key, "state_name": value["state"], "pop_2014": value["pop_2014"]}
        for key, value in population_data.items()
    ]
    df = spark.createDataFrame(rows)
    count = df.count()
    print(f"✅ Extracted {count} population records")
    df.write.mode("overwrite").parquet(str(POPULATION_PARQUET))
    print(f"✅ Wrote population parquet: {POPULATION_PARQUET}")
    return POPULATION_PARQUET


def extract_locations(spark: SparkSession) -> Path:
    parquet_path = LOCATION_PARQUET.as_posix()
    print(f"⏳ Reading location parquet from {parquet_path}")
    df = spark.read.parquet(parquet_path)
    count = df.count()
    print(f"✅ Extracted {count} location rows")
    df.write.mode("overwrite").parquet(LOCATION_STAGING_PARQUET.as_posix())
    print(f"✅ Wrote locations parquet: {LOCATION_STAGING_PARQUET}")
    return LOCATION_STAGING_PARQUET


def transform_data(spark: SparkSession, complaints_path: Path, population_path: Path, location_path: Path) -> Path:
    complaints_df = spark.read.parquet(complaints_path.as_posix())
    population_df = spark.read.parquet(population_path.as_posix())
    location_df = spark.read.parquet(location_path.as_posix())

    complaints_summary = (
        complaints_df.groupBy("State")
        .agg(count("Complaint ID").alias("complaints_count"))
        .withColumnRenamed("State", "state_abbrv")
    )
    summary_count = complaints_summary.count()
    print(f"✅ Computed complaints summary for {summary_count} states")

    population_clean = population_df.select(
        col("state_abbrv"),
        col("state_name"),
        col("pop_2014"),
    )
    location_clean = location_df.select(
        col("state&teritory").alias("state_abbrv"),
        col("latitude"),
        col("longitude"),
    )

    joined = (
        complaints_summary.join(population_clean, "state_abbrv", "inner")
        .join(location_clean, "state_abbrv", "inner")
        .select(
            col("state_abbrv"),
            col("state_name"),
            col("complaints_count"),
            col("pop_2014"),
            col("latitude"),
            col("longitude"),
        )
        .dropDuplicates(["state_abbrv"])
    )
    joined_count = joined.count()
    joined.write.mode("overwrite").parquet(str(FINAL_PARQUET))
    print(f"✅ Transformed and joined data for {joined_count} states")
    print(f"✅ Wrote final parquet: {FINAL_PARQUET}")
    return FINAL_PARQUET


def load_duckdb(parquet_path: Path, duckdb_path: Path = DUCKDB_FILE) -> Path:
    if not HAS_DUCKDB:
        print(f"INFO: DuckDB not installed, skipping database loading. Parquet data available at: {parquet_path}")
        return parquet_path
    conn = duckdb.connect(str(duckdb_path))
    conn.execute("DROP TABLE IF EXISTS consumer_insights")
    parquet_uri = parquet_path.as_posix()
    sql = f"CREATE TABLE consumer_insights AS SELECT * FROM read_parquet('{parquet_uri}')"
    conn.execute(sql)
    conn.close()
    print(f"✅ Loaded data into DuckDB: {duckdb_path}")
    return duckdb_path


def export_outputs(spark: SparkSession, parquet_path: Path) -> None:
    final_df = spark.read.parquet(str(parquet_path))
    row_count = final_df.count()
    final_df.coalesce(1).write.mode("overwrite").csv(str(CSV_EXPORT), header=True)
    final_df.coalesce(1).write.mode("overwrite").json(str(JSON_EXPORT))
    print(f"✅ Exported {row_count} final rows to CSV and JSON")
    print(f"✅ CSV export path: {CSV_EXPORT}")
    print(f"✅ JSON export path: {JSON_EXPORT}")


def run_pipeline() -> None:
    spark = build_spark_session()
    try:
        complaints_path = extract_complaints(spark)
        population_path = extract_population(spark)
        location_path = extract_locations(spark)
        final_path = transform_data(spark, complaints_path, population_path, location_path)
        load_duckdb(final_path)
        export_outputs(spark, final_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()
