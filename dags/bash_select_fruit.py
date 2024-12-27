from airflow import DAG
from airflow.operators.bash import BashOperator
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

with DAG(
        dag_id="bash_select_fruit",
        schedule="10 0 * * 6#1",
        start_date=pendulum.datetime(2019, 10, 10, tz="Asia/Seoul"),
        catchup=False
) as dag:
    t1_orange = BashOperator(
        task_id="t1_orange",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE",
    )

    t2_avocado = BashOperator(
        task_id="t2_avocado",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh AVOCADO",
    )

    t1_orange >> t2_avocado