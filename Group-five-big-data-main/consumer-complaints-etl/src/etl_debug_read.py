import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
os.chdir(ROOT)
sys.path.insert(0, os.path.join(ROOT, 'src'))

print('DEBUG READ: cwd', os.getcwd())
import etl_spark

spark = etl_spark.build_spark_session()
print('DEBUG READ: spark built')
try:
    csv_path = etl_spark.COMPLAINTS_CSV.as_posix()
    print('DEBUG READ: csv path', csv_path)
    df = spark.read.option('header', True).option('inferSchema', True).csv(csv_path)
    print('DEBUG READ: schema', df.schema)
    rows = df.limit(5).collect()
    print('DEBUG READ: sample rows', len(rows))
finally:
    spark.stop()
