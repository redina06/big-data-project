import os
import runpy

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
os.chdir(ROOT)
print(f'Running ETL from {ROOT}')
runpy.run_path('src/etl_spark.py', run_name='__main__')
