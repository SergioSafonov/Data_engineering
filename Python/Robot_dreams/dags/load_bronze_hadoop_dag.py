from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime

from get_config import get_dshop_tables
from load_to_bronze import load_to_bronze_hadoop

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}


def load_to_bronze_dshop(value):
    return PythonOperator(
        task_id="load_dshop_" + value + "_bronze",
        dag=bronze_dag,
        python_callable=load_to_bronze_hadoop,
        op_kwargs={"table": value}
    )


bronze_dag = DAG(
    dag_id="load_bronze_dshop_dag",
    description="Load data from PostgerSQL database to Data Lake bronze",
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 13),
    default_args=default_args
)

dummy1 = DummyOperator(
    task_id="start_load_bronze",
    dag=bronze_dag
)
dummy2 = DummyOperator(
    task_id="end_load_bronze",
    dag=bronze_dag
)

for table in get_dshop_tables():
    dummy1 >> load_to_bronze_dshop(table) >> dummy2

