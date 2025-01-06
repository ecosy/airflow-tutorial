from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator
from pendulum import datetime

with DAG(
        dag_id='dag_task_branch',
        schedule="10 0 * * *",
        start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
        catchup=False
) as dag:
    @task.branch(task_id='python_branch_task')
    def select_random():
        import random

        items = ['A', 'B', 'C']
        selected = random.choice(items)
        if selected == 'A':
            return 'task_a'
        else:
            return ['task_b', 'task_c']

    def common_func(**kwargs):
        print(kwargs.get('selected'))

    task_a = BranchPythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected': 'A'},
    )

    task_b = BranchPythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected': 'B'},
    )

    task_c = BranchPythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected': 'C'},
    )

    select_random() >> [task_a, task_b, task_c]
