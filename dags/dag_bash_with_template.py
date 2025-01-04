import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id='dag_bash_with_template',
        schedule='10 * * * *',
        start_date=pendulum.datetime(2021, 12, 1, tz='Asia/Seoul'),
        catchup=False,
) as dag:
    bash_t1 = BashOperator(
        task_id='bash_t1',
        bash_command='echo "data_interval_end: {{ data_interval_end }}"'
    )

    bash_t2 = BashOperator(
        task_id='bash_t2',
        env={
            'START_DATE': '{{data_interval_start | ds}}',  # | ds : YYYY_MM_DD 형식으로 출력
            'END_DATE': '{{data_interval_end | ds}}'
        },
        bash_command='echo $START_DATE && echo $END_DATE'
    )

    bash_t1 >> bash_t2