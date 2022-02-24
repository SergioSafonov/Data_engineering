from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.bash_operator import BashOperator

from get_config import get_pagila_tables
from load_to_bronze import load_to_bronze_spark

default_args = {
    "owner": "airflow",
    "email": ["airflow@airflow.com"],
    "email_on_failure": False
}


def load_to_bronze_pagila(value):
    return PythonOperator(
        task_id="load_pagila_" + value + "_bronze",
        dag=bronze_pagila_dag,
        python_callable=load_to_bronze_spark,
        op_kwargs={"table": value},
        provide_context=True
    )


bronze_pagila_dag = DAG(
    dag_id="load_bronze_pagila_dag",
    description="Load data from PostgerSQL database pagila to Data Lake bronze",
    schedule_interval="@daily",
    start_date=datetime(2022, 2, 24),
    default_args=default_args
)
'''
One more way to run spark job

test = BashOperator(
    task_id="test_task_spark",
    dag=bronze_pagila_dag,
    bash_command="spark-submit --name some_job  --jars /home/user/shared_folder/Distrib/postgresql-42.2.23.jar "
                 "--driver-class-path /home/user/shared_folder/Distrib/postgresql-42.2.23.jar "
                 "/home/user/shared_folder/Distrib/spark.py "
)
'''
dummy1 = DummyOperator(
    task_id="start_load_bronze",
    dag=bronze_pagila_dag
)
dummy2 = DummyOperator(
    task_id="end_load_bronze",
    dag=bronze_pagila_dag
)

for table in get_pagila_tables():
    dummy1 >> load_to_bronze_pagila(table) >> dummy2
