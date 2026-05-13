import os
import sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
os.chdir(ROOT)
sys.path.insert(0, os.path.join(ROOT, 'src'))
import etl_spark
spark = etl_spark.build_spark_session()
print('DEBUG NOINFER: spark built')
try:
    csv_path = 'file:///' + etl_spark.COMPLAINTS_CSV.as_posix()
    print('DEBUG NOINFER: csv path', csv_path)
    df = spark.read.option('header', True).option('inferSchema', False).csv(csv_path)
    print('DEBUG NOINFER: schema', df.schema)
    rows = df.limit(5).collect()
    print('DEBUG NOINFER: sample rows', len(rows))
finally:
    spark.stop()
