import os
from datetime import timedelta
from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Injected natively by the Kubernetes Executor charm via the
# airflow-coordinator ↔ spark-integration-hub relation.
SA, NS = os.environ.get("SPARK_USERNAME", "spark"), os.environ.get("SPARK_NAMESPACE", "airflow-spark")
IMAGE  = "ghcr.io/canonical/charmed-spark:3.5-22.04_edge"
CMD    = (f"echo 'from pyspark.sql import SparkSession;"
          f"spark=SparkSession.builder.appName(\"Demo\").getOrCreate();"
          f"print(\"rows:\", spark.range(3).count());spark.stop()' > /tmp/j.py"
          f" && python3 -m spark8t.cli.spark_submit"
          f" --username {SA} --namespace {NS} --deploy-mode client /tmp/j.py")

with DAG("spark_demo", schedule=None, catchup=False,
         default_args={"retries": 0, "execution_timeout": timedelta(minutes=10)}) as dag:
    hello = PythonOperator(task_id="hello", python_callable=lambda: print(f"SA={SA} NS={NS}"))
    spark = KubernetesPodOperator(
        task_id="spark_job", name="spark-job", namespace=NS, image=IMAGE,
        cmds=["/bin/bash", "-c"], arguments=[CMD],
        service_account_name=SA, get_logs=True, is_delete_operator_pod=False,
    )
    hello >> spark
