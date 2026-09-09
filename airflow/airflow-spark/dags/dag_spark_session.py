# spark_session_demo — submits a PySpark job to Charmed Apache Spark.
#
# The worker Pod runs `spark8t` in **cluster mode**: Spark creates a driver Pod
# (charmed-spark image) in the Spark namespace, which spawns executor Pods and
# writes an event log to S3 (visible in the Spark History Server). The job file
# is read from S3 — it is staged there by the justfile (`configure-microceph`).
import os
from datetime import timedelta

from airflow.sdk import DAG
from airflow.operators.bash import BashOperator

# Injected into the worker Pod by the executor charm from the Spark Hub relation.
SA = os.environ.get("SPARK_USERNAME", "spark")
NS = os.environ.get("SPARK_NAMESPACE", "airflow-spark")

# PySpark job staged in S3 (see spark_jobs/session_job.py).
JOB_URI = "s3a://spark-history/jobs/session_job.py"

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
            f"python3 -m spark8t.cli.spark_submit --username {SA} --namespace {NS} "
            f"--deploy-mode cluster {JOB_URI}"
        ),
    )
