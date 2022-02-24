from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from get_config import get_payload_dates
from load_from_sources import load_out_of_stocks

default_args = {
    "owner": "airflow",
    "email": ["airflow@airflow.com"],
    "email_on_failure": False
}


# Dag definition of loading process from Out_of_stocks API to Data Lake bronze

def out_of_stocks(used_date):
    return PythonOperator(
        task_id=f"out_of_stocks_{used_date}",
        dag=out_of_stocks_dag,
        python_callable=load_out_of_stocks,
        op_kwargs={'process_date': used_date},
        task_concurrency=1,
        provide_context=True
    )


out_of_stocks_dag = DAG(
    dag_id='out_of_stocks_dag',
    description='DAG for getting payload data from out_of_stocks API to Data Lake',
    schedule_interval='@daily',
    start_date=datetime(2022, 2, 24),
    default_args=default_args
)

dummy1 = DummyOperator(
    task_id="start_load",
    dag=out_of_stocks_dag
)
dummy2 = DummyOperator(
    task_id="end_load",
    dag=out_of_stocks_dag
)

for payload_date in get_payload_dates():
    dummy1 >> out_of_stocks(payload_date) >> dummy2
