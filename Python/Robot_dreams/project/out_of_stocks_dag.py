from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from get_config import get_payload_dates, get_hdfs, get_spark, get_postgres_conn

from etl_bronze import out_of_stocks_config_load
from etl_silver import out_of_stocks_config_silver_load
from etl_gold import out_of_stocks_config_gold_load

default_args = {
    "owner": "airflow",
    "email": ["airflow@airflow.com"],
    "email_on_failure": False
}


# Dag definition of loading process from Out_of_stocks API to Data Lake

def out_of_stocks_bronze(hdfs, used_date):
    return PythonOperator(
        task_id=f'out_of_stocks_config_{used_date}_bronze',
        dag=out_of_stocks_dag,
        python_callable=out_of_stocks_config_load,
        op_kwargs={'process_date': used_date, 'client_hdfs': hdfs},
        task_concurrency=1
    )


def out_of_stocks_silver(sparkcontext, used_date):
    return PythonOperator(
        task_id=f'out_of_stocks_config_{used_date}_silver',
        dag=out_of_stocks_dag,
        python_callable=out_of_stocks_config_silver_load,
        op_kwargs={'process_date': used_date, 'spark': sparkcontext},
        task_concurrency=1
    )


def out_of_stocks_gold(sparkcontext, conn, used_date):
    return PythonOperator(
        task_id=f'out_of_stocks_config_{used_date}_gold',
        dag=out_of_stocks_dag,
        python_callable=out_of_stocks_config_gold_load,
        op_kwargs={'process_date': used_date, 'spark': sparkcontext, 'gp_conn': conn},
        task_concurrency=1
    )


out_of_stocks_dag = DAG(
    dag_id='out_of_stocks_config_dag',
    description='DAG for getting payload data from out_of_stocks API to Data Lake',
    schedule_interval='@daily',
    start_date=datetime(2022, 4, 20, 15, 30),
    end_date=datetime(2022, 6, 20, 15, 30),
    default_args=default_args
)

dummy1 = DummyOperator(
    task_id="start_bronze_load",
    dag=out_of_stocks_dag
)
dummy2 = DummyOperator(
    task_id="start_silver_load",
    dag=out_of_stocks_dag
)
dummy3 = DummyOperator(
    task_id="start_gold_load",
    dag=out_of_stocks_dag
)
dummy4 = DummyOperator(
    task_id="end_load",
    dag=out_of_stocks_dag
)

client_hdfs = get_hdfs()
spark = get_spark()
gp_conn = get_postgres_conn('target')

for payload_date in get_payload_dates():
    dummy1 >> out_of_stocks_bronze(client_hdfs, payload_date) \
        >> dummy2 >> out_of_stocks_silver(spark, payload_date) \
        >> dummy3 >> out_of_stocks_gold(spark, gp_conn, payload_date) \
        >> dummy4
