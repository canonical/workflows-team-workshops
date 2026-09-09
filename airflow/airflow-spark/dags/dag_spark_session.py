# spark_session_demo — runs a PySpark job that builds a full SparkSession.
#
# The job creates a SparkSession (and therefore a SparkContext), runs a small
# aggregation over an in-memory range, and stops. Building a SparkSession makes
# Spark emit an event log to S3, which is what the Spark History Server reads.
import os
from datetime import timedelta

from airflow.sdk import DAG
from airflow.operators.bash import BashOperator

# Injected into the worker Pod by the executor charm from the Spark Hub relation.
SA = os.environ.get("SPARK_USERNAME", "spark")
NS = os.environ.get("SPARK_NAMESPACE", "airflow-spark")

SPARK_JOB = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("airflow-spark-session-demo").getOrCreate()
sc = spark.sparkContext
print("spark version:", sc.version)
print("application id:", sc.applicationId)

df = spark.range(1, 1001)
print("row count:", df.count())
print("sum of ids:", df.groupBy().sum("id").collect()[0][0])

spark.stop()
"""

with DAG(
    "spark_session_demo",
    schedule=None,
    catchup=False,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=30)},
) as dag:
    spark_session_job = BashOperator(
        task_id="spark_session_job",
        bash_command=(
            "set -ex && export PATH=$JAVA_HOME/bin:$PATH && "
            "cat > /tmp/session_job.py << 'PYEOF'\n"
            f"{SPARK_JOB}\n"
            "PYEOF\n"
            f"python3 -m spark8t.cli.spark_submit --username {SA} --namespace {NS} "
            "--deploy-mode client /tmp/session_job.py"
        ),
    )
