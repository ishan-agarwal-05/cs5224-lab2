"""Run separately with the PySpark environment: python tests/spark_edge_cases.py."""
import importlib.util
from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('job', ROOT / 'analysis/spark_job.py')
job = importlib.util.module_from_spec(spec); spec.loader.exec_module(job)
spark = SparkSession.builder.master('local[2]').appName('Lab2-edge-cases').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
raw = spark.createDataFrame([(s,) for s in ['1 2', '1 2', '2 1', '3 2', '5 6', '4 4', '',
                                          'bad row', '1 2 3', '0001\t002', '999999999999999999999999 6']], ['value'])
parsed, edges = job.clean_edges(raw)
actual = {r.user_id: (r.followers, r.followees) for r in job.count_users(edges).collect()}
expected = {'1': (1, 1), '2': (2, 1), '3': (0, 1), '5': (0, 1), '6': (2, 0),
            '999999999999999999999999': (0, 1)}
assert actual == expected, (actual, expected)
assert edges.count() == 5
print('PASS: Spark dirty-input, deduplication, reciprocal direction, zero-degree sides and large IDs.')
spark.stop()
