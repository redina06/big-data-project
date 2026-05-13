import os
import sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
os.chdir(ROOT)
sys.path.insert(0, os.path.join(ROOT, 'src'))

import etl_spark
from pyspark.sql import SparkSession
spark = etl_spark.build_spark_session()
print('DEBUG URI: spark built')
try:
    csv_path = 'file:///' + etl_spark.COMPLAINTS_CSV.as_posix()
    print('DEBUG URI: csv path', csv_path)
    df = spark.read.option('header', True).option('inferSchema', True).csv(csv_path)
    print('DEBUG URI: schema', df.schema)
    rows = df.limit(5).collect()
    print('DEBUG URI: sample rows', len(rows))
finally:
    spark.stop()
