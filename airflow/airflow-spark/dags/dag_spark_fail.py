# spark_fail_demo — a DAG that is expected to FAIL.
#
# Same cluster-mode submission as spark_session_demo, but the staged job raises
# in the driver, so spark-submit reports a failed application and the Airflow
# task (and DAG run) ends in a failed state.
import os
from datetime import timedelta

from airflow.sdk import DAG
from airflow.operators.bash import BashOperator

# Injected into the worker Pod by the executor charm from the Spark Hub relation.
SA = os.environ.get("SPARK_USERNAME", "spark")
NS = os.environ.get("SPARK_NAMESPACE", "airflow-spark")

# PySpark job staged in S3 (see spark_jobs/fail_job.py).
JOB_URI = "s3a://spark-history/jobs/fail_job.py"

with DAG(
    "spark_fail_demo",
    schedule=None,
    catchup=False,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=30)},
) as dag:
    spark_failing_job = BashOperator(
        task_id="spark_failing_job",
        bash_command=(
            "set -ex && export PATH=$JAVA_HOME/bin:$PATH && "
            f"python3 -m spark8t.cli.spark_submit --username {SA} --namespace {NS} "
            f"--deploy-mode cluster {JOB_URI}"
        ),
    )
