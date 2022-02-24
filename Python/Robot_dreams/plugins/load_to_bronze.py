import os
import logging
import psycopg2

from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook

from pyspark.sql import SparkSession

hdfs_path = os.path.join('/', 'datalake', 'bronze')


def load_to_bronze_hadoop(table, **kwargs):
    current_date = kwargs["execution_date"].strftime('%Y-%m-%d')
    file_name = table + '.csv'

    hdfs_conn = BaseHook.get_connection('datalake_hdfs')
    client = InsecureClient(f"{hdfs_conn.host}:{hdfs_conn.port}/", user=hdfs_conn.login)

    pg_conn = BaseHook.get_connection('oltp_postgres')
    db_name = 'dshop'  # pg_conn.schema

    pg_creds = {
        'host': pg_conn.host,
        'port': pg_conn.port,
        'database': db_name,
        'user': pg_conn.login,
        'password': pg_conn.password
            }

    logging.info(f"Writing table {table} from {pg_conn.host} to Bronze")

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()

        with client.write(os.path.join(hdfs_path, db_name, table, current_date, file_name)
                          , overwrite=True) as csv_file:
            cursor.copy_expert(f"COPY {table} TO STDOUT WITH HEADER CSV", csv_file)

    logging.info("Successfully loaded")
    # no exceptions - run from Airflow!


def load_to_bronze_spark(table, **kwargs):
    current_date = kwargs["execution_date"].strftime('%Y-%m-%d')

    pg_conn = BaseHook.get_connection('oltp_postgres')
    db_name = 'pagila'  # pg_conn.schema

    pg_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{db_name}"
    pg_properties = {"user": pg_conn.login, "password": pg_conn.password}

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/Distrib/postgresql-42.2.23.jar') \
        .master('local') \
        .appName('lesson_14') \
        .getOrCreate()

    logging.info(f"Writing table {table} from {pg_conn.host} to Bronze")

    table_df = spark.read.jdbc(pg_url, table=table, properties=pg_properties)
    table_df.write.parquet(
        os.path.join(hdfs_path, db_name, table, current_date, table), mode="overwrite")

    logging.info("Successfully loaded")
