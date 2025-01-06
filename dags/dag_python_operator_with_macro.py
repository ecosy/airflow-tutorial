from airflow import DAG
from airflow.operators.python import task
from pendulum import datetime

with DAG(
        dag_id="dag_python_operator_with_macro",
        schedule="10 0 * * *",
        start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
        catchup=False,
) as dag:
    @task(task_id="task_with_macro",
          templates_dict={
              'start_date': '{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds}}', # 전월 첫째일
              'end_date': '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds}}' #전월 마지막 일
          })
    def get_datetime_with_macro(**kwargs):
        templates_dict = kwargs.get('templates_dict', {})
        if templates_dict:
            start_date = templates_dict.get('start_date', "no data")
            end_date = templates_dict.get('end_date', "no data")
            print(start_date, end_date)

    @task(task_id="task_without_macro")
    def get_datetime_without_macro(**kwargs):
        from dateutil.relativedelta import relativedelta # DAG 최적화를 위해서 함수 내부에서 import 하는 것이 좋다.

        data_interval_end = kwargs.get('data_interval_end')
        prev_month_day_first = data_interval_end.in_timezone("Asia/Seoul") + relativedelta(months=-1, day=1)        # 전월 첫째일
        prev_month_day_last = data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + relativedelta(days=-1)   #전월 마지막 일
        print(prev_month_day_first.strftime("%Y-%m-%d"))
        print(prev_month_day_last.strftime("%Y-%m-%d"))

    get_datetime_with_macro() >> get_datetime_without_macro()

