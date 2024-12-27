import random

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
        dag_id='python_operator',
        schedule="30 6 * * *",
        start_date=pendulum.datetime(2020, 1, 1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    def select_fruit():
        fruit = ['APPLE', 'ORANGE', 'BANANA']
        rand_int = random.randint(0, len(fruit) - 1)
        print(fruit[rand_int])


    py_t1 = PythonOperator(
        task_id='py_t1',
        python_callable=select_fruit
    )