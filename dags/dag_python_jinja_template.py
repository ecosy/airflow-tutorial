from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from pendulum import datetime

with DAG(
        dag_id="dag_python_jinja_template",
        schedule="30 9 * * * ",
        start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    def print_start_end_date(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    t1 = PythonOperator(
        task_id="t1",
        python_callable=print_start_end_date,
        op_kwargs={'start_date': '{{data_interval_start | ds}}',
                   'end_date': '{{data_interval_end | ds}}', }
    )

    @task(task_id="t2")
    def print_kwargs(**kwargs):
        print(kwargs)
        print(f'ds: {kwargs["ds"]}')
        print(f'ts: {kwargs["ts"]}')
        print(f'data_interval_start: {str(kwargs["data_interval_start"])}')
        print(f'data_interval_end: {str(kwargs["data_interval_end"])}')
        print(f'task_instance: {str(kwargs["task_instance"])}')

    t1 >> print_kwargs()
