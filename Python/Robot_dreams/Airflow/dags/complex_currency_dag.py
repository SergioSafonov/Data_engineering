import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from config import Config
from complex_http import ComplexHttpOperator

from datetime import datetime

config = Config(os.path.join('/', 'home', 'user', 'airflow', 'plugins', 'config.yaml'))
config_set = config.get_config('currency_app')


def group_currency(currency):
    return ComplexHttpOperator(
        task_id=f'get_currency_{currency}',
        dag=currency_http_dag,
        method="GET",
        http_conn_id="currency_connection",
        endpoint=config_set['last_url'],
        data={'access_key': config_set['access_key'], 'base': config_set['base'], 'symbols': currency},
        # data={'access_key': "a0ec6e79d368336768d78dd4bfc06e1f", 'symbols': value},
        xcom_push=True,
        save=True,
        save_path=config_set['directory'],
        task_concurrency=5
    )


currency_http_dag = DAG(
    dag_id='currency_complex_4',
    description='Our Http Currencies DAG 4',
    start_date=datetime(2021, 12, 26, 14, 30),
    end_date=datetime(2022, 12, 26, 14, 30),
    schedule_interval='@daily'
)

dummy1 = DummyOperator(task_id="dummy1", dag=currency_http_dag)

dummy2 = DummyOperator(task_id="dummy2", dag=currency_http_dag)

currencies = config_set['symbols']
for cur in currencies:
    dummy1 >> group_currency(cur) >> dummy2
