from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from get_config import get_dshopbu_tables, get_silver_tables
from etl_bronze import dshopbu_bronze_load
from etl_silver import dshopbu_silver_load

default_args = {
    "owner": "airflow",
    "email": ["airflow@airflow.com"],
    "email_on_failure": False
}


def load_bronze_dshopbu(value):
    return PythonOperator(
        task_id="dshopbu_" + value + "_bronze",
        dag=dshopbu_datalake_dag,
        python_callable=dshopbu_bronze_load,
        op_kwargs={"table": value},
        provide_context=True
    )


def load_silver_dshopbu(value):
    return PythonOperator(
        task_id="dshopbu_" + value + "_silver",
        dag=dshopbu_datalake_dag,
        python_callable=dshopbu_silver_load,
        op_kwargs={"table": value},
        provide_context=True
    )


dshopbu_datalake_dag = DAG(
    dag_id="dshopbu_datalake_dag",
    description="Load data from PostgreSQL database dshop_bu to Data Lake bronze",
    schedule_interval="@daily",
    start_date=datetime(2022, 2, 24),
    default_args=default_args
)

dummy1 = DummyOperator(
    task_id="start_bronze_load",
    dag=dshopbu_datalake_dag
)
dummy2 = DummyOperator(
    task_id="start_silver_load",
    dag=dshopbu_datalake_dag
)
dummy3 = DummyOperator(
    task_id="end_load",
    dag=dshopbu_datalake_dag
)

for bronze_table in get_dshopbu_tables():
    dummy1 >> load_bronze_dshopbu(bronze_table) >> dummy2

for silver_table in get_silver_tables():
    dummy2 >> load_silver_dshopbu(silver_table) >> dummy3

