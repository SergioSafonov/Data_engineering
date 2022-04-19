from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from get_config import get_dshop_tables
from load_from_sources import dshop_bronze_load
from load_to_silver import aisles_silver_load, clients_silver_load, departments_silver_load
from load_to_silver import products_silver_load, orders_silver_load

default_args = {
    "owner": "airflow",
    "email": ["airflow@airflow.com"],
    "email_on_failure": False
}


def load_bronze_dshop(value):
    return PythonOperator(
        task_id="dshop_" + value + "_bronze",
        dag=dshop_datalake_dag,
        python_callable=dshop_bronze_load,
        op_kwargs={"table": value},
        provide_context=True
    )


dshop_datalake_dag = DAG(
    dag_id="dshop_datalake_dag",
    description="Load data from PostgreSQL database dshop to Data Lake bronze",
    schedule_interval="@daily",
    start_date=datetime(2022, 2, 24),
    default_args=default_args
)

dummy1 = DummyOperator(
    task_id="start_bronze_load",
    dag=dshop_datalake_dag
)
dummy2 = DummyOperator(
    task_id="start_silver_load",
    dag=dshop_datalake_dag
)

load_aisles_silver = PythonOperator(
    task_id="dshop_aisles_silver",
    dag=dshop_datalake_dag,
    python_callable=aisles_silver_load,
    provide_context=True
)
load_clients_silver = PythonOperator(
    task_id="dshop_clients_silver",
    dag=dshop_datalake_dag,
    python_callable=clients_silver_load,
    provide_context=True
)
load_departments_silver = PythonOperator(
    task_id="dshop_departments_silver",
    dag=dshop_datalake_dag,
    python_callable=departments_silver_load,
    provide_context=True
)
load_products_silver = PythonOperator(
    task_id="dshop_products_silver",
    dag=dshop_datalake_dag,
    python_callable=products_silver_load,
    provide_context=True
)
load_orders_silver = PythonOperator(
    task_id="dshop_orders_silver",
    dag=dshop_datalake_dag,
    python_callable=orders_silver_load,
    provide_context=True
)

dummy3 = DummyOperator(
    task_id="end_load",
    dag=dshop_datalake_dag
)

for table in get_dshop_tables():
    dummy1 >> load_bronze_dshop(table) >> dummy2 >> \
    (
        load_aisles_silver,
        load_clients_silver,
        load_departments_silver,
        load_products_silver,
        load_orders_silver
    ) >> dummy3
