from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from get_config import get_hdfs, get_spark, get_postgres_conn

from etl_bronze import out_of_stocks_current_load
from etl_silver import out_of_stocks_current_silver_load
from etl_gold import out_of_stocks_current_gold_load

default_args = {
    "owner": "airflow",
    "email": ["airflow@airflow.com"],
    "email_on_failure": False
}


# Dag definition of loading process from Out_of_stocks API to Data Lake bronze

def out_of_stocks_current_bronze(hdfs):
    return PythonOperator(
        task_id="out_of_stocks_current_bronze",
        dag=out_of_stocks_current_dag,
        python_callable=out_of_stocks_current_load,
        op_kwargs={'client_hdfs': hdfs},
        task_concurrency=1,
        provide_context=True
    )


def out_of_stocks_current_silver(sparkcontext):
    return PythonOperator(
        task_id="out_of_stocks_current_silver",
        dag=out_of_stocks_current_dag,
        python_callable=out_of_stocks_current_silver_load,
        op_kwargs={'spark': sparkcontext},
        task_concurrency=1,
        provide_context=True
    )


def out_of_stocks_current_gold(sparkcontext, conn):
    return PythonOperator(
        task_id="out_of_stocks_current_gold",
        dag=out_of_stocks_current_dag,
        python_callable=out_of_stocks_current_gold_load,
        op_kwargs={'spark': sparkcontext, 'gp_conn': conn},
        task_concurrency=1,
        provide_context=True
    )


out_of_stocks_current_dag = DAG(
    dag_id='out_of_stocks_current_dag',
    description='DAG for getting payload current data from out_of_stocks API to Data Lake',
    schedule_interval='@daily',
    start_date=datetime(2022, 4, 20, 15, 00),
    end_date=datetime(2022, 6, 20, 15, 00),
    default_args=default_args
)

dummy1 = DummyOperator(
    task_id="start_bronze_load",
    dag=out_of_stocks_current_dag
)
dummy2 = DummyOperator(
    task_id="start_silver_load",
    dag=out_of_stocks_current_dag
)
dummy3 = DummyOperator(
    task_id="start_gold_load",
    dag=out_of_stocks_current_dag
)
dummy4 = DummyOperator(
    task_id="end_load",
    dag=out_of_stocks_current_dag
)

client_hdfs = get_hdfs()
spark = get_spark()
gp_conn = get_postgres_conn('target')

dummy1 >> out_of_stocks_current_bronze(client_hdfs) \
    >> dummy2 >> out_of_stocks_current_silver(spark) \
    >> dummy3 >> out_of_stocks_current_gold(spark, gp_conn) \
    >> dummy4
