from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from complex_http import ComplexHttpOperator
from datetime import datetime

currencies = ['UAH', 'GBP', 'USD', 'PLN', 'RUB']


def group_currency(value):
    return ComplexHttpOperator(
        task_id=f'get_currency_{value}',
        dag=currency_http_dag,
        method="GET",
        http_conn_id="new_currency_connection",
        endpoint="2021-07-07",
        data={'access_key': "a0ec6e79d368336768d78dd4bfc06e1f", 'symbols': value},
        xcom_push=True,
        save=True,
        task_concurrency=5
    )


currency_http_dag = DAG(
     dag_id='complex_currency',
     description='Our Http Currencies DAG',
     start_date=datetime(2021, 7, 7, 14, 30),
     end_date=datetime(2021, 10, 7, 14, 30),
     schedule_interval='@daily'
 )

dummy1 = DummyOperator(task_id="dummy1", dag=currency_http_dag)

dummy2 = DummyOperator(task_id="dummy2", dag=currency_http_dag)

for currency in currencies:
    dummy1 >> group_currency(currency) >> dummy2
