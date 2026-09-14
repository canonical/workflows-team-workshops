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
        # Cluster-mode spark-submit exits 0 even when the app fails, so the task
        # succeeds/fails based on the driver's final phase in the output.
        bash_command=(
            "set -o pipefail && export PATH=$JAVA_HOME/bin:$PATH && "
            f"python3 -m spark8t.cli.spark_submit --username {SA} --namespace {NS} "
            f"--deploy-mode cluster {JOB_URI} 2>&1 | tee /tmp/spark.out; "
            "grep -q 'phase: Failed' /tmp/spark.out && { echo '>>> Spark application FAILED'; exit 1; }; "
            "grep -q 'phase: Succeeded' /tmp/spark.out || { echo '>>> Spark application did not succeed'; exit 1; }"
        ),
    )
