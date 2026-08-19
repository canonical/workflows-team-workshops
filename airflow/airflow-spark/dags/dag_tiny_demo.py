import os
from datetime import timedelta
from airflow.sdk import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

SA = os.environ.get("SPARK_USERNAME", "spark")
NS = os.environ.get("SPARK_NAMESPACE", "airflow-spark")

with DAG("tiny_spark_demo", schedule=None, catchup=False,
         default_args={"retries": 0, "execution_timeout": timedelta(minutes=30)}) as dag:
    hello = PythonOperator(task_id="hello", python_callable=lambda: print(f"SA={SA} NS={NS}"))
    spark = BashOperator(
        task_id="spark_job",
        bash_command=(
            "set -ex && export PATH=$JAVA_HOME/bin:$PATH && "
            "java -version 2>&1 && "
            "python3 -c 'import pyspark; print(\"pyspark:\", pyspark.__version__)' && "
            "echo 'print(42)' > /tmp/j.py && "
            f"timeout 120 python3 -m spark8t.cli.spark_submit --log-level DEBUG --username {SA} --namespace {NS}"
            " --deploy-mode client /tmp/j.py 2>&1; echo EXIT=$?"
        ),
    )
    hello >> spark
