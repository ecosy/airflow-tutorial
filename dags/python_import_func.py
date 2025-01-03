import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from plugins.common.common_func import get_sftp

with DAG(
        dag_id='python_import_func',
        schedule="30 6 * * *",
        start_date=pendulum.datetime(2020, 1, 1, tz="Asia/Seoul"),
        catchup=False
) as dag:
    task_get_sftp = PythonOperator(
        task_id='task_get_sftp',
        python_callable=get_sftp
    )