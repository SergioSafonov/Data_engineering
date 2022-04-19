from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from get_config import get_config
from etl_bronze import CurrencyAPISaveHttpOperator
from etl_silver import currency_silver_load

config = get_config()
config_set = config.get('currency_app')


def currencies_bronze(currency):
    return CurrencyAPISaveHttpOperator(
        task_id=f'currency_{currency}_current_bronze',
        dag=currency_current_dag,
        method="GET",
        http_conn_id="currency_connection",
        endpoint="latest",
        data={'access_key': config_set['access_key'], 'base': config_set['base'], 'symbols': currency},
        xcom_push=False,
        save_hdfs=True,
        save_path=config_set['directory'],
        context_date=config_set['context_date'],
        task_concurrency=1
            )


def currencies_silver(currency):
    return PythonOperator(
        task_id="currency_current_" + currency + "_silver",
        dag=currency_current_dag,
        python_callable=currency_silver_load,
        op_kwargs={
            "currency": currency,
            "base": config_set['base'],
            "save_path": config_set['directory'],
            "context_date": config_set['context_date']
                   },
        provide_context=True
            )


currency_current_dag = DAG(
    dag_id='currency_current_dag',
    description='DAG for getting currencies current data from Currency API to Data Lake',
    start_date=datetime(2022, 2, 24, 14, 30),
    end_date=datetime(2023, 2, 24, 14, 30),
    schedule_interval='@daily'
)

dummy1 = DummyOperator(
    task_id="start_bronze_load",
    dag=currency_current_dag
)
dummy2 = DummyOperator(
    task_id="start_silver_load",
    dag=currency_current_dag
)
dummy3 = DummyOperator(
    task_id="end_load",
    dag=currency_current_dag
)

currencies = config_set['symbols']
for cur in currencies:
    dummy1 >> currencies_bronze(cur) >> dummy2 >> currencies_silver(cur) >> dummy3
