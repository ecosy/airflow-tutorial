import datetime

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id="example_bash_operator",  # dat name (File 이름과 동일하게 맞추는게 좋음)
        schedule="0 0 * * *",
        start_date=pendulum.datetime(2021, 1, 1, tz="KST"),
        catchup=False,  # start date 로부터 누락된 구간을 한번에 모두 돌릴 것인지 여부
        dagrun_timeout=datetime.timedelta(minutes=60),  # 60분 이상 실행시 예외처리
        tags=["example", "example2"],  # 대시보드에서 필터링 할 수 있는 태그
        params={"example_key": "example_value"},  # tasks 공통 파라미터
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1", # Graph에 나타나는 task 이름
        bash_command="echo bash operator!!",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2