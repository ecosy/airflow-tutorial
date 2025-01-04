from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime

from common.common_func import regist_with_kwargs

with DAG(
    dag_id="dag_python_with_op_kwargs",
    schedule="*/10 * * * *",
    start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    regist_t1 = PythonOperator(
        task_id="regist_t1",
        python_callable=regist_with_kwargs,
        op_args = ['my name', 'my sex', 'args1', 'args2'],
        op_kwargs={'email': 'test.com', 'phone': '010'}
    )