import os
import sys

# Change to the project root directory (parent of src/)
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
os.chdir(project_root)
print(f"Current working directory: {os.getcwd()}")

# Set environment variables (these should already be set system-wide)
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["SPARK_HOME"] = r"C:\Users\Hybrid Technology\Downloads\spark-4.1.1-bin-hadoop3\spark-4.1.1-bin-hadoop3"
os.environ["HADOOP_HOME"] = os.environ["SPARK_HOME"]

# Add HADOOP_HOME/bin to PATH for winutils.exe
hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PATH"] = hadoop_bin + ";" + os.environ.get("PATH", "")

print(f"JAVA_HOME: {os.environ['JAVA_HOME']}")
print(f"SPARK_HOME: {os.environ['SPARK_HOME']}")
print(f"HADOOP_HOME: {os.environ['HADOOP_HOME']}")
print(f"Python executable: {sys.executable}")
print(f"Python version: {sys.version}")

print("About to import SparkSession")
from pyspark.sql import SparkSession
print("SparkSession imported")

# Create SparkSession with minimal configuration
spark = SparkSession.builder \
    .appName("Consumer_Complaints_ETL_Test") \
    .master("local[1]") \
    .getOrCreate()

print("SUCCESS: Spark initialized successfully!")
print(f"Spark version: {spark.version}")

spark.stop()
print("Spark session stopped.")
print("SUCCESS: Basic PySpark connection test passed!")

