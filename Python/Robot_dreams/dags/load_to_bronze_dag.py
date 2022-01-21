from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

from get_config import get_pagila_tables
from load_to_bronze import load_to_bronze, load_to_bronze_spark

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}


def load_to_bronze_group(value):
    return PythonOperator(
        task_id="load_" + value + "_to_bronze",
        dag=bronze_dag,
        python_callable=load_to_bronze_spark,
        op_kwargs={"table": value}
    )


bronze_dag = DAG(
    dag_id="load_to_bronze",
    description="Load data from PostgerSQL database to Data Lake bronze",
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 13),
    default_args=default_args
)

dummy1 = DummyOperator(
    task_id="start_load_to_bronze",
    dag=bronze_dag
)
dummy2 = DummyOperator(
    task_id="end_load_to_bronze",
    dag=bronze_dag
)

for table in get_pagila_tables():
    dummy1 >> load_to_bronze_group(table) >> dummy2
