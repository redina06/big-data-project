import os
import sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
os.chdir(ROOT)
sys.path.insert(0, os.path.join(ROOT, 'src'))
import etl_spark
spark = etl_spark.build_spark_session()
print('DEBUG TEXT: spark built')
try:
    csv_path = etl_spark.COMPLAINTS_CSV.as_posix()
    print('DEBUG TEXT: csv path', csv_path)
    df = spark.read.text(csv_path)
    print('DEBUG TEXT: text schema', df.schema)
    rows = df.limit(5).collect()
    print('DEBUG TEXT: sample rows', len(rows))
    for r in rows:
        print('ROW', r[0][:100])
finally:
    spark.stop()
