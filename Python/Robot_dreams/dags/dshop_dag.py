import os

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresHook


def postgres_copy(table_name):

    file_name = table_name + '.tsv'
    result_dir = os.path.join('/', 'home', 'user', 'data', 'postgres_data', 'dshop')
    os.makedirs(os.path.join(result_dir), exist_ok=True)

    postgres = PostgresHook(postgres_conn_id='postgres_dshop')
    postgres.bulk_dump(table_name, os.path.join(result_dir, file_name))


def postgres_dump(table):
    return PythonOperator(
        task_id=f"Copy_dshop_{table}_task",
        dag=Dshop_dag,
        python_callable=postgres_copy,
        op_kwargs={'table_name': table},
        task_concurrency=5
    )


Dshop_dag = DAG(
    dag_id='Dshop_dag',
    description='DAG for getting Postgres Dshop data to local tsv files',
    start_date=datetime(2021, 7, 23, 1, 30),
    end_date=datetime(2021, 9, 23, 1, 30),
    schedule_interval='@daily'
)

dummy1 = DummyOperator(task_id="dummy1", dag=Dshop_dag)
dummy2 = DummyOperator(task_id="dummy2", dag=Dshop_dag)

dummy1 >> (
        postgres_dump('aisles'),
        postgres_dump('clients'),
        postgres_dump('departments'),
        postgres_dump('orders'),
        postgres_dump('products')
 ) >> dummy2

