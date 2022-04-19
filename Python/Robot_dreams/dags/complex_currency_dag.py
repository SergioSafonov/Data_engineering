import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# from config import Config
from get_config import get_config
from complex_http import CurrencyAPISaveHttpOperator
from datetime import datetime

# config = Config(os.path.join('/', 'home', 'user', 'airflow', 'plugins', 'config.yaml'))
# config_set = config.get_config('currency_app')
config = get_config()
config_set = config.get('currency_app')
currencies = config_set['symbols']


def group_currency(currency):
    return CurrencyAPISaveHttpOperator(
        task_id=f'get_currency_{currency}',
        dag=currency_http_dag,
        method="GET",
        http_conn_id="currency_connection",
        endpoint="latest",
        data={'access_key': config_set['access_key'], 'base': config_set['base'], 'symbols': currency},
        xcom_push=True,
        save=True,
        save_path=config_set['directory'],
        task_concurrency=1
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

for cur in currencies:
    dummy1 >> group_currency(cur) >> dummy2
