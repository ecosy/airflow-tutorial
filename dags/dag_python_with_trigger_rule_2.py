from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from pendulum import datetime

with DAG(
        dag_id='dag_python_with_trigger_rule_2',
        schedule="10 0 * * *",
        start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
        catchup=False
) as dag:
    @task.branch(task_id='branch_t1')
    def random_branch():
        import random
        items = ['A', 'B', 'C']
        selected = random.choice(items)
        return 'task_{}'.format(selected.lower())

    task_a = BashOperator(
        task_id='task_a',
        bash_command='echo "Task A"',
    )

    @task(task_id='task_b')
    def task_b():
        print('Task B 완료')

    @task(task_id='task_c')
    def task_c():
        print('Task C 완료')

    @task(task_id='task_d', trigger_rule='none_skipped')
    def task_d():
        print('Task D done..')

    random_branch() >> [task_a, task_b(), task_c()] >> task_d()