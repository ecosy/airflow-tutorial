from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime

with DAG(
        dag_id='dag_trigger_dag_run_operator',
        start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
        schedule="30 9 * * *",
        catchup=False,
) as dag:
    start_task = BashOperator(
        task_id="start_task",
        bash_command='echo "start task!!"'
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id="trigger_dag_task",
        trigger_dag_id="python_operator",
        trigger_run_id=None,
        logical_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None
    )

    start_task >> trigger_dag_task