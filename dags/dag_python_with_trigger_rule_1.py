from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from pendulum import datetime

with DAG(
        dag_id='dag_python_with_trigger_rule_1',
        schedule="10 0 * * *",
        start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
        catchup=False
) as dag:
    bash_upstream_t1 = BashOperator(
        task_id='bash_upstream_t1',
        bash_command='echo upstream1!!',
    )

    @task(task_id='python_upstream_t2')
    def python_upstream_t2():
        raise AirflowException('python_upstream_t2 raise an exception!!')

    @task(task_id='python_upstream_t3')
    def python_upstream_t3():
        print('python_upstream_t3 완료!')

    @task(task_id='python_downstream_t4', trigger_rule='all_done')
    def python_downstream_t4():
        print('python_downstream_t4 완료!')

    [bash_upstream_t1, python_upstream_t2(), python_upstream_t3()] >> python_downstream_t4()