from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from get_config import get_dshop_tables
from load_from_sources import load_to_bronze_dshop

default_args = {
    "owner": "airflow",
    "email": ["airflow@airflow.com"],
    "email_on_failure": False
}


# Dag definition of loading process from PostgreSQL dshop db to Data Lake bronze

def load_to_bronze_dshop(value):
    return PythonOperator(
        task_id="dshop_" + value + "_bronze",
        dag=dshop_datalake_dag,
        python_callable=load_to_bronze_dshop,
        op_kwargs={"table": value},
        provide_context=True
    )


dshop_datalake_dag = DAG(
    dag_id="dshop_datalake_dag",
    description="Load data from PostgerSQL database dshop to Data Lake",
    schedule_interval="@daily",
    start_date=datetime(2022, 2, 24),
    default_args=default_args
)

dummy1 = DummyOperator(
    task_id="start_load",
    dag=dshop_datalake_dag
)
dummy2 = DummyOperator(
    task_id="end_load",
    dag=dshop_datalake_dag
)

for table in get_dshop_tables():
    dummy1 >> load_to_bronze_dshop(table) >> dummy2

