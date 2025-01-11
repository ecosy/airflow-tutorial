from airflow import DAG
from airflow.decorators import task_group, task
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from pendulum import datetime

with DAG(
        dag_id='dag_python_with_task_group',
        schedule="10 0 * * *",
        start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
        catchup=False
) as dag:
    def print_msg(**kwargs):
        print(f"message: {kwargs['msg']}")

    @task_group(group_id='1st_group')
    def group1():
        """ 1st task group decorator description """

        @task(task_id='inner_task1')
        def inner_task1(**kwargs):
            print("1st_group >> inner task1")

        inner_task2 = PythonOperator(
            task_id='inner_task2',
            python_callable=print_msg,
            op_kwargs={'msg': '1st group >> inner task2'}
        )

        inner_task1() >> inner_task2

    with TaskGroup(group_id='2nd_group', tooltip='2nd task group description') as group2:
        """ 표시되지 않은 설명 입니다."""
        @task(task_id='inner_task1')
        def inner_task1(**kwargs):
            print("1st_group >> inner task1")

        inner_task2 = PythonOperator(
            task_id='inner_task2',
            python_callable=print_msg,
            op_kwargs={'msg': '1st group >> inner task2'}
        )

        inner_task1() >> inner_task2

    group1() >> group2