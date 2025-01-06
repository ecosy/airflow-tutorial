from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from pendulum import datetime

with DAG(
        dag_id='dag_bash_with_variable',
        schedule="10 0 * * *",
        start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    variable_value = Variable.get("test")

    # 1안
    bash_var_1 = BashOperator(
        task_id="bash_var_1",
        bash_command=f"echo variable 1 : {variable_value}"
    )

    # 2안, 권고 방식! (스케줄러 부하 감소)
    bash_var_2 = BashOperator(
        task_id="bash_var_2",
        bash_command="echo variable 2 : {{ var.value.test }}"
    )

    bash_var_1 >> bash_var_2