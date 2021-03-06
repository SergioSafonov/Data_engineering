from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from load_from_sources import out_of_stocks_current_load
from load_to_silver import out_of_stocks_current_silver_load

default_args = {
    "owner": "airflow",
    "email": ["airflow@airflow.com"],
    "email_on_failure": False
}


# Dag definition of loading process from Out_of_stocks API to Data Lake bronze

oos_current_dag = DAG(
    dag_id='oos_current_dag',
    description='DAG for getting payload current data from out_of_stocks API to Data Lake',
    schedule_interval='@daily',
    start_date=datetime(2022, 2, 24),
    default_args=default_args
)

out_of_stocks_bronze = PythonOperator(
    task_id="out_of_stocks_current_bronze",
    dag=oos_current_dag,
    python_callable=out_of_stocks_current_load,
    task_concurrency=1,
    provide_context=True
)

out_of_stocks_silver = PythonOperator(
    task_id="out_of_stocks_current_silver",
    dag=oos_current_dag,
    python_callable=out_of_stocks_current_silver_load,
    task_concurrency=1,
    provide_context=True
)

dummy1 = DummyOperator(
    task_id="start_bronze_load",
    dag=oos_current_dag
)
dummy2 = DummyOperator(
    task_id="start_silver_load",
    dag=oos_current_dag
)
dummy3 = DummyOperator(
    task_id="end_load",
    dag=oos_current_dag
)

dummy1 >> out_of_stocks_bronze >> dummy2 >> out_of_stocks_silver >> dummy3

