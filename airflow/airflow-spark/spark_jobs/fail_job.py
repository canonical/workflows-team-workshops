"""Spark job submitted by the spark_fail_demo DAG (cluster mode).

Runs inside a charmed-spark driver Pod. Starts a SparkSession and then raises an
exception, so the driver exits non-zero, spark-submit reports a failed
application, and the Airflow task (and DAG run) ends in a failed state.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("airflow-spark-failing-demo").getOrCreate()
print("row count:", spark.range(10).count())

raise RuntimeError("Intentional failure to demonstrate a failed Spark job")
