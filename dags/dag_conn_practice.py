from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

with DAG(
        dag_id="dag_conn_practice",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=["dag_conn_practice"],
) as dag:
    t1 = EmptyOperator(task_id="t1", dag=dag)
    t2 = EmptyOperator(task_id="t2", dag=dag)
    t3 = EmptyOperator(task_id="t3", dag=dag)
    t4 = EmptyOperator(task_id="t4", dag=dag)
    t5 = EmptyOperator(task_id="t5", dag=dag)
    t6 = EmptyOperator(task_id="t6", dag=dag)
    t7 = EmptyOperator(task_id="t7", dag=dag)
    t8 = EmptyOperator(task_id="t8", dag=dag)

    t1 >> [t2, t3] >> t4
    t5 >> t4
    [t4, t7] >> t6 >> t8

