import os

from airflow.operators.postgres_operator import PostgresHook


def postgres_copy(table_name):

    file_name = table_name + '.tsv'
    result_dir = os.path.join('/', 'home', 'user', 'data', 'postgres_data', 'dshop')
    os.makedirs(result_dir, exist_ok=True)

    postgres = PostgresHook(postgres_conn_id='postgres_dshop')
    postgres.bulk_dump(table_name, os.path.join(result_dir, file_name))
