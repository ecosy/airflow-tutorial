from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from pendulum import datetime

with DAG(
        dag_id='dag_empty_with_edge_label',
        schedule="10 0 * * *",
        start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
        catchup=False
) as dag:
    empty_1 = EmptyOperator(task_id='empty_1')
    empty_2 = EmptyOperator(task_id='empty_2')
    empty_3 = EmptyOperator(task_id='empty_3')
    empty_4 = EmptyOperator(task_id='empty_4')
    empty_5 = EmptyOperator(task_id='empty_5')

    # 종속성 설정 및 라벨링
    empty_1 >> Label("start branch") >> [empty_2, empty_3, empty_4] >> Label("end branch") >> empty_5