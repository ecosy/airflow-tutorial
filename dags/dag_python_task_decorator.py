import pendulum
from airflow.decorators import dag, task


@dag(
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
)
def dag_python_task_decorator():
    @task(task_id="python_task_1")
    def print_input(ss):
        print(ss)

    python_task_1 = print_input('decorator 실행!')


dag_python_task_decorator()
