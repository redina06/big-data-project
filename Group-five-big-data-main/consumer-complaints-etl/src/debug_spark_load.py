import os
import traceback
import findspark

os.environ["JAVA_HOME"] = r"C:/Program Files/Java/jdk-17"
os.environ["PATH"] = os.path.join(os.environ["JAVA_HOME"], "bin") + ";" + os.environ.get("PATH", "")
os.environ["SPARK_HOME"] = r"C:/Users/Hybrid Technology/Downloads/spark-4.1.1-bin-hadoop3/spark-4.1.1-bin-hadoop3"
findspark.init()

from pyspark.sql import SparkSession

try:
    spark = SparkSession.builder.appName("Debug_Spark_Load").config("spark.sql.adaptive.enabled", "true").getOrCreate()
    print("Spark initialized", spark.version)
    try:
        csv_df = spark.read.option("header", True).option("inferSchema", True).csv("data/raw/Consumer_Complaints.csv")
        print("CSV schema:")
        csv_df.printSchema()
        print("CSV count", csv_df.count())
    except Exception:
        print("CSV load failed")
        traceback.print_exc()
    try:
        json_df = spark.read.option("multiline", True).json("data/raw/us-states-population.json")
        print("JSON loaded")
    except Exception:
        print("JSON load failed")
        traceback.print_exc()
    try:
        parquet_df = spark.read.parquet("data/raw/US_States_Long_Lat.parquet")
        print("Parquet loaded")
    except Exception:
        print("Parquet load failed")
        traceback.print_exc()
finally:
    spark.stop()
