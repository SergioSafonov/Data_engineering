import os
import psycopg2

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from hdfs import InsecureClient

postgres_creds = {
    'host': '192.168.56.101',
    'port': 5432,
    'database': 'dshop',
    'user': 'pguser',
    'password': 'secret'
}


def postgres_dshop_dl(table_name, **kwargs):
    # exec_date = date.isoformat(kwargs["execution_date"])

    file_name = table_name + '.csv'
    # result_dir = os.path.join('/', 'bronze', 'postgres', 'dshop', exec_date)
    result_dir = os.path.join('/', 'datalake', 'bronze', 'dshop')
    file_path = os.path.join(result_dir, file_name)

    # open HDFS Data Lake client
    client_hdfs = InsecureClient('http://127.0.0.1:50070/', user='user')

    # create result DL folder
    client_hdfs.makedirs(result_dir)

    # copy dump dshop DB from Postgres to HDFS Bronze csv files
    with psycopg2.connect(**postgres_creds) as postgres_connection:
        cursor = postgres_connection.cursor()

        with client_hdfs.write(file_path) as csv_file:  # write to HDFS csv file ...
            client_hdfs.delete(file_path, recursive=False)  # delete previous csv file if exist
            cursor.copy_expert(f"COPY {table_name} TO STDOUT WITH HEADER CSV", csv_file)  # ... Postgres table


default_args = {
    'owner': 'airflow',
    'params': {
        'table1': 'aisles',
        'table2': 'clients',
        'table3': 'departments',
        'table4': 'orders',
        'table5': 'products'
    }
}

Dshop_DL_dag = DAG(
    dag_id='Dshop_DL_dag',
    description='DAG for getting Postgres Dshop data to Bronze Data Lake',
    default_args=default_args,
    start_date=datetime(2021, 7, 23, 1, 30),
    end_date=datetime(2021, 9, 23, 1, 30),
    schedule_interval='@daily'
)


def postgres_dump_dshop(table):
    return PythonOperator(
        task_id=f"Copy_dshop_{table}_DL_task",
        dag=Dshop_DL_dag,
        python_callable=postgres_dshop_dl,
        op_kwargs={'table_name': table},
        task_concurrency=5,
        provide_context=True
    )


dummy1 = DummyOperator(task_id="dummy1", dag=Dshop_DL_dag)
dummy2 = DummyOperator(task_id="dummy2", dag=Dshop_DL_dag)

for postgres_table in Dshop_DL_dag.params.values():
    dummy1 >> postgres_dump_dshop(postgres_table) >> dummy2
