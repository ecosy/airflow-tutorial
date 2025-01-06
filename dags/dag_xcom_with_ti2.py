from airflow import DAG
from airflow.operators.python import task
from pendulum import datetime

with DAG(
        dag_id='dag_xcom_with_ti2',
        schedule="10 0 * * *",
        start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    @task(task_id='xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'

    @task(task_id='xcom_pull')
    def xcom_pull_by_task_id(**kwargs):
        ti = kwargs['ti']
        value = ti.xcom_pull(task_ids='xcom_push_by_return')
        print(f'get return value from xcom : {value}')

    @task(task_id='print_params')
    def print_params(status):
        print(f'input : {status}')

    result = xcom_push_result()
    print_params(result)

    result >> xcom_pull_by_task_id()