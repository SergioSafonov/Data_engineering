import os
import psycopg2
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from hdfs import InsecureClient

postgres_creds = {
    'host': '192.168.88.98',
    'port': 5432,
    'database': 'dshop_bu',
    'user': 'pguser',
    'password': 'secret'
 }


def postgres_dshop_bu_dl(table_name):
    file_name = table_name + '.csv'
    result_dir = os.path.join('/', 'bronze', 'postgres', 'dshop_bu')
    file_path = os.path.join(result_dir, file_name)

    # open HDFS Data Lake client
    client_hdfs = InsecureClient('http://127.0.0.1:50070/', user='user')

    # create result DL folder
    client_hdfs.makedirs(result_dir)

    # define Postgres dshop_bu connection
    with psycopg2.connect(**postgres_creds) as postgres_connection:
        cursor = postgres_connection.cursor()

        # copy dump dshop_bu DB from Postgres to HDFS Bronze csv files
        with client_hdfs.write(file_path) as csv_file:  # write to HDFS csv file
            client_hdfs.delete(file_path, recursive=False)
            cursor.copy_expert(f"COPY {table_name} TO STDOUT WITH HEADER CSV", csv_file)  # Postgres table


default_args = {
    'owner': 'airflow',
    'params': {
        'table1': 'aisles',
        'table2': 'clients',
        'table3': 'departments',
        'table4': 'location_areas',
        'table5': 'orders',
        'table6': 'products',
        'table7': 'store_types',
        'table8': 'stores'
    }
}

Dshop_BU_DL_dag = DAG(
    dag_id='Dshop_BU_DL_dag',
    description='DAG for getting Postgres Dshop_bu data to Bronze Data Lake',
    default_args=default_args,
    start_date=datetime(2021, 7, 23, 2, 00),
    end_date=datetime(2021, 9, 23, 2, 00),
    schedule_interval='@daily'
)


def postgres_dump_dshop_bu(table):
    return PythonOperator(
        task_id=f"Copy_dshop_bu_{table}_DL_task",
        dag=Dshop_BU_DL_dag,
        python_callable=postgres_dshop_bu_dl,
        op_kwargs={'table_name': table},
        task_concurrency=1  # decrease VM resource using without parallel runs
    )


dummy1 = DummyOperator(task_id="dummy1", dag=Dshop_BU_DL_dag)
dummy2 = DummyOperator(task_id="dummy2", dag=Dshop_BU_DL_dag)

for postgres_table in Dshop_BU_DL_dag.params.values():
    dummy1 >> postgres_dump_dshop_bu(postgres_table) >> dummy2
