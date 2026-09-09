# spark_fail_demo — a DAG that is expected to FAIL.
#
# The PySpark job starts a SparkSession and then raises an exception in the
# driver. spark-submit exits non-zero, so the Airflow task (and the DAG run)
# ends in a "failed" state. Useful to demo how failures surface in the UI.
import os
from datetime import timedelta

from airflow.sdk import DAG
from airflow.operators.bash import BashOperator

# Injected into the worker Pod by the executor charm from the Spark Hub relation.
SA = os.environ.get("SPARK_USERNAME", "spark")
NS = os.environ.get("SPARK_NAMESPACE", "airflow-spark")

SPARK_JOB = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("airflow-spark-failing-demo").getOrCreate()
print("row count:", spark.range(10).count())

# Intentional failure: the driver raises, so spark-submit exits non-zero.
raise RuntimeError("Intentional failure to demonstrate a failed Spark job")
"""

with DAG(
    "spark_fail_demo",
    schedule=None,
    catchup=False,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=30)},
) as dag:
    spark_failing_job = BashOperator(
        task_id="spark_failing_job",
        # No trailing "echo EXIT=$?": the non-zero exit propagates and fails the task.
        bash_command=(
            "set -ex && export PATH=$JAVA_HOME/bin:$PATH && "
            "cat > /tmp/fail_job.py << 'PYEOF'\n"
            f"{SPARK_JOB}\n"
            "PYEOF\n"
            f"python3 -m spark8t.cli.spark_submit --username {SA} --namespace {NS} "
            "--deploy-mode client /tmp/fail_job.py"
        ),
    )
