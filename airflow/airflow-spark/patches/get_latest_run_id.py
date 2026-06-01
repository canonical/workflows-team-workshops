from airflow.models import DagRun
from airflow.utils.session import create_session
with create_session() as s:
    dr = s.query(DagRun).filter(DagRun.dag_id=="parallel_spark_analytics").order_by(DagRun.start_date.desc()).first()
    if dr:
        print(dr.run_id)
