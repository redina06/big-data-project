import os
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
os.chdir(project_root)
print('CWD:', os.getcwd())

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] = r"C:\Program Files\Java\jdk-17\bin" + ";" + os.environ.get("PATH", "")
os.environ["SPARK_HOME"] = r"C:\Users\Hybrid Technology\Downloads\spark-4.1.1-bin-hadoop3\spark-4.1.1-bin-hadoop3"

# Instead of findspark, set PYTHONPATH manually
spark_python = os.path.join(os.environ["SPARK_HOME"], "python")
py4j = os.path.join(os.environ["SPARK_HOME"], "python", "lib", "py4j-0.10.9.9-src.zip")
os.environ["PYTHONPATH"] = spark_python + ";" + py4j + ";" + os.environ.get("PYTHONPATH", "")

# import findspark
# findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test").getOrCreate()
print('Spark version:', spark.version)
spark.stop()