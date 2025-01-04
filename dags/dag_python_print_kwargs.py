from airflow import DAG
from airflow.decorators import task
from pendulum import datetime

with DAG(
    dag_id='dag_python_print_kwargs',
    schedule = "30 9 * * *",
    start_date = datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=True
) as dag:

    @task(task_id='python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    show_templates()