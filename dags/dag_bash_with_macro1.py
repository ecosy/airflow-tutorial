from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime

with DAG(
        dag_id="dag_bash_with_macro1",
        schedule="10 0 L * *", # 전월 말일
        start_date=datetime(2024, 12, 1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    # START_DATE: 전월 말일, END_DATE: 1일 전
    bash_t1 = BashOperator(
        task_id="bash_t1",
        env={
            'START_DATE': '{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}',
            'END_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}',
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )