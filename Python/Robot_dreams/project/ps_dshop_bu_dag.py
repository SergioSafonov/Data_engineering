from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from get_config import get_postgres_conn, get_hdfs, get_dshopbu_tables
from get_config import get_silver_tables, get_spark, get_gold_tables

from etl_bronze import dshopbu_bronze_load
from etl_silver import dshopbu_silver_load
from etl_gold import dshopbu_gold_load

default_args = {
    "owner": "airflow",
    "email": ["airflow@airflow.com"],
    "email_on_failure": False
}


def load_bronze_dshopbu(conn, hdfs, value):
    return PythonOperator(
        task_id="dshopbu_" + value + "_bronze",
        dag=dshopbu_datalake_dag,
        python_callable=dshopbu_bronze_load,
        op_kwargs={"table": value, "client_hdfs": hdfs, "pg_conn": conn},
        provide_context=True,
        task_concurrency=1
    )


def load_silver_dshopbu(sparkcontext, value):
    return PythonOperator(
        task_id="dshopbu_" + value + "_silver",
        dag=dshopbu_datalake_dag,
        python_callable=dshopbu_silver_load,
        op_kwargs={"table": value, "spark": sparkcontext},
        provide_context=True,
        task_concurrency=1
    )


def load_gold_dshopbu(conn, sparkcontext, value):
    return PythonOperator(
        task_id="dshopbu_" + value + "_gold",
        dag=dshopbu_datalake_dag,
        python_callable=dshopbu_gold_load,
        op_kwargs={"table": value, "spark": sparkcontext, "gp_conn": conn},
        provide_context=True,
        task_concurrency=1
    )


dshopbu_datalake_dag = DAG(
    dag_id="dshopbu_datalake_dag",
    description="Load data from PostgreSQL database dshop_bu to Data Lake and Greenplum",
    schedule_interval="@daily",
    start_date=datetime(2022, 4, 20, 16, 00),
    end_date=datetime(2022, 6, 20, 16, 00),
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
    task_id="start_gold_load",
    dag=dshopbu_datalake_dag
)
dummy4 = DummyOperator(
    task_id="end_load",
    dag=dshopbu_datalake_dag
)

# postgres -> hdfs bronze
client_hdfs = get_hdfs()
pg_conn = get_postgres_conn('source')

for bronze_table in get_dshopbu_tables():
    dummy1 >> load_bronze_dshopbu(pg_conn, client_hdfs, bronze_table) >> dummy2

# hdfs bronze -> hdfs silver
spark = get_spark()

for silver_table in get_silver_tables():
    dummy2 >> load_silver_dshopbu(spark, silver_table) >> dummy3

# hdfs silver -> greenplum
gp_conn = get_postgres_conn('target')

for gold_table in get_gold_tables():
    dummy3 >> load_gold_dshopbu(gp_conn, spark, gold_table) >> dummy4

