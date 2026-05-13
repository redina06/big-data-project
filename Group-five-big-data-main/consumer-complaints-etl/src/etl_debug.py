import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
os.chdir(ROOT)
sys.path.insert(0, os.path.join(ROOT, 'src'))

print('DEBUG: cwd', os.getcwd())

import etl_spark

spark = etl_spark.build_spark_session()
print('DEBUG: spark built')
try:
    path = etl_spark.extract_complaints(spark)
    print('DEBUG: complaints extracted to', path)
finally:
    spark.stop()
