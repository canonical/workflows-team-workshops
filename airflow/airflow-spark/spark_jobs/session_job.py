"""Spark job submitted by the spark_session_demo DAG (cluster mode).

Runs inside a charmed-spark driver Pod. Builds a SparkSession (and therefore a
SparkContext), runs a small aggregation, and stops. Building the session makes
Spark write an event log to S3, which the Spark History Server then renders.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("airflow-spark-session-demo").getOrCreate()
sc = spark.sparkContext
print("spark version:", sc.version)
print("application id:", sc.applicationId)

df = spark.range(1, 1001)
print("row count:", df.count())
print("sum of ids:", df.groupBy().sum("id").collect()[0][0])

spark.stop()
