from pathlib import Path

from etl_spark import DUCKDB_FILE, FINAL_PARQUET, load_duckdb


def main() -> None:
    parquet_path = Path(FINAL_PARQUET)
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Final parquet file not found: {parquet_path}. Run src/etl_spark.py first."
        )
    duckdb_path = load_duckdb(parquet_path)
    print(f"Loaded final dataset into DuckDB: {duckdb_path}")


if __name__ == "__main__":
    main()
