from airflow import DAG
from airflow.decorators import task
from pendulum import datetime

with DAG(
    dag_id='dag_xcom_with_ti',
    schedule="10 0 * * *",
    start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    @task(task_id='xcom_push_t1')
    def xcom_push1(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key="key1", value="value1")
        ti.xcom_push(key="key2", value=[1,2,3])

    @task(task_id='xcom_push_t2')
    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key="key1", value="value2")
        ti.xcom_push(key="key2", value=[1,2,3,4])

    @task(task_id='xcom_pull_t3')
    def xcom_pull(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(key="key1")
        value2 = ti.xcom_pull(key="key2", task_ids='xcom_push_t1')
        print(value1)
        print(value2)

    xcom_push2() >> xcom_push1() >> xcom_pull()