from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from get_config import get_config, get_hdfs, get_spark, get_postgres_conn

from etl_bronze import CurrencyAPISaveHttpOperator
from etl_silver import currency_silver_load
from etl_gold import currency_gold_load

config = get_config()
config_set = config.get('currency_app')


def currencies_bronze(client, currency):
    return CurrencyAPISaveHttpOperator(
        task_id=f'currency_{currency}_current_bronze',
        dag=currency_dag,
        method="GET",
        http_conn_id=config.get('connections').get('currency_api'),
        endpoint=config_set['endpoint'],
        data={'access_key': config_set['access_key'], 'currency_base': config_set['base'], 'symbols': currency},
        xcom_push=False,
        save_hdfs=True,
        client_hdfs=client,
        currency_path=config_set['directory'],
        context_date=config_set['context_date'],
        task_concurrency=1
    )


def currencies_silver(sparkcontext, currency):
    return PythonOperator(
        task_id="currency_" + currency + "_silver",
        dag=currency_dag,
        python_callable=currency_silver_load,
        op_kwargs={
            "currency": currency,
            "currency_base": config_set['base'],
            "currency_path": config_set['directory'],
            "context_date": config_set['context_date'],
            "spark": sparkcontext
        },
        provide_context=True
    )


def currencies_gold(conn, sparkcontext):
    return PythonOperator(
        task_id="currency_gold",
        dag=currency_dag,
        python_callable=currency_gold_load,
        op_kwargs={
            "currency_base": config_set['base'],
            "currency_path": config_set['directory'],
            "context_date": config_set['context_date'],
            "spark": sparkcontext,
            "gp_conn": conn
        },
        provide_context=True
    )


currency_dag = DAG(
    dag_id='currency_current_dag',
    description='DAG for getting currencies current data from Currency API to Data Lake',
    start_date=datetime(2022, 4, 20, 14, 00),
    end_date=datetime(2022, 6, 20, 14, 00),
    schedule_interval='@daily'
)

dummy1 = DummyOperator(
    task_id="start_bronze_load",
    dag=currency_dag
)
dummy2 = DummyOperator(
    task_id="start_silver_load",
    dag=currency_dag
)
dummy3 = DummyOperator(
    task_id="start_gold_load",
    dag=currency_dag
)
dummy4 = DummyOperator(
    task_id="end_load",
    dag=currency_dag
)

# Currency API -> hdfs Bronze -> hdfs Silver -> Greenplum
hdfs = get_hdfs()
spark = get_spark()
gp_conn = get_postgres_conn('target')

currencies = config_set['symbols']
for cur in currencies:
    dummy1 >> currencies_bronze(hdfs, cur) >> dummy2 >> currencies_silver(spark, cur) >> dummy3

dummy3 >> currencies_gold(gp_conn, spark) >> dummy4
