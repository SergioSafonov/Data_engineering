from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# from Airflow.plugins.complex_http import ComplexHttpOperator
from complex_http import ComplexHttpOperator

from datetime import datetime

currencies = ['UAH', 'GBP', 'USD', 'PLN', 'RUB']
currency_tasks = []

currency_http_dag = DAG(
    dag_id='currency_complex_3',
    description='Our Http Currencies DAG 3',
    start_date=datetime(2021, 12, 26, 14, 30),
    end_date=datetime(2022, 12, 26, 14, 30),
    schedule_interval='@daily',
    concurrency=2
 )

dummy1 = DummyOperator(task_id="dummy1", dag=currency_http_dag)

dummy2 = DummyOperator(task_id="dummy2", dag=currency_http_dag)

for currency in currencies:
    currency_tasks.append(
        ComplexHttpOperator(
            task_id=f"get_currency_{currency}",
            dag=currency_http_dag,
            method="GET",
            http_conn_id="currency_connection",
            endpoint="2021-12-03",
            data={'access_key': "a0ec6e79d368336768d78dd4bfc06e1f", 'symbols': currency},
            xcom_push=True,
            save=True,
            task_concurrency=5
        )
    )
    dummy1 >> currency_tasks >> dummy2
