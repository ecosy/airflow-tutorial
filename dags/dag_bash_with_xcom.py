from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime

with DAG(
        dag_id='dag_bash_with_xcom',
        schedule="10 0 * * *",
        start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    bash_push = BashOperator(
        task_id='bash_push',
        bash_command="""
                     echo START && 
                     echo XCOM_PUSHED {{ ti.xcom_push(key='bash_pushed', value='first message!') }} &&  
                     echo COMPLETE!
                     """  # 마지막으로 출력된 문장이 리턴 값!
    )

    bash_pull = BashOperator(
        task_id='bash_pull',
        env={
            'PUSHED_VALUE': "{{ ti.xcom_pull(key='bash_pushed') }}",  # 위 key에 대한 value 의 값을 가져온다.
            'RETURNED_VALUE': "{{ ti.xcom_pull(task_ids='bash_pushed') }}"  # task id 의 리턴 값을 가져온다.
        },
        bash_command="echo $PUSHED_VALUE && echo $RETURNED_VALUE",
        do_xcom_push=False  # 마지막 출력 문장을 xcom 에 저장하지 않는다.
    )

    bash_push >> bash_pull
