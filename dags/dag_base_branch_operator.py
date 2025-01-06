from airflow import DAG
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import BranchPythonOperator
from pendulum import datetime

with DAG(
        dag_id='dag_base_branch_operator',
        schedule="10 0 * * *",
        start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
        catchup=False
) as dag:
    class CustomBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):
            import random

            items = ['A', 'B', 'C']
            selected = random.choice(items)
            if selected == 'A':
                return 'task_a'
            else:
                return ['task_b', 'task_c']

    custom_branch_operator = CustomBranchOperator(task_id='custom_branch_task')

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

    custom_branch_operator >> [task_a, task_b, task_c]
