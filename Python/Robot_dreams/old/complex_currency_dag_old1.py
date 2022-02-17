from airflow import DAG
from datetime import datetime

from complex_http import CurrencyAPISaveHttpOperator

currency_http_dag = DAG(
    dag_id='currency_complex',
    description='Our 1st Http Currency DAG',
    start_date=datetime(2021, 12, 26, 14, 30),
    end_date=datetime(2022, 12, 26, 14, 30),
    schedule_interval='@daily'
)

t1 = CurrencyAPISaveHttpOperator(
    task_id='get_currency',
    dag=currency_http_dag,
    method="GET",
    http_conn_id="currency_connection",
    endpoint="2021-12-01",
    data={'access_key': "a0ec6e79d368336768d78dd4bfc06e1f", 'symbols': "USD"},
    xcom_push=True,
    save=True,
    save_path="data"
)
