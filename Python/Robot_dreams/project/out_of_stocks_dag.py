from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from get_config import get_payload_dates
from etl_bronze import out_of_stocks_config_load
from etl_silver import out_of_stocks_config_silver_load

default_args = {
    "owner": "airflow",
    "email": ["airflow@airflow.com"],
    "email_on_failure": False
}


# Dag definition of loading process from Out_of_stocks API to Data Lake

def out_of_stocks_bronze(used_date):
    return PythonOperator(
        task_id=f"out_of_stocks_{used_date}_bronze",
        dag=out_of_stocks_dag,
        python_callable=out_of_stocks_config_load,
        op_kwargs={'process_date': used_date},
        task_concurrency=1
    )


def out_of_stocks_silver(used_date):
    return PythonOperator(
        task_id=f"out_of_stocks_{used_date}_silver",
        dag=out_of_stocks_dag,
        python_callable=out_of_stocks_config_silver_load,
        op_kwargs={'process_date': used_date},
        task_concurrency=1
    )


out_of_stocks_dag = DAG(
    dag_id='out_of_stocks_dag',
    description='DAG for getting payload data from out_of_stocks API to Data Lake',
    schedule_interval='@daily',
    start_date=datetime(2022, 2, 24),
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
    task_id="end_load",
    dag=out_of_stocks_dag
)

for payload_date in get_payload_dates():
    dummy1 >> out_of_stocks_bronze(payload_date) >> dummy2 >> out_of_stocks_silver(payload_date) >> dummy3
