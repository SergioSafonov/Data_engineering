import os
import psycopg2
import logging

from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook
from pyspark.sql import SparkSession


def load_to_bronze_hadoop(table):
    # not resolved hadoop connection at Virtual machine!
    file_name = table + '.csv'

    hdfs_conn = BaseHook.get_connection('datalake_hdfs')
    client = InsecureClient("http://" + hdfs_conn.host, user=hdfs_conn.login)

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

        with client.write(os.path.join('/', 'datalake', 'bronze', db_name, file_name)) as csv_file:
            cursor.copy_expert(f"COPY {table} TO STDOUT WITH HEADER CSV", csv_file)

    logging.info("Successfully loaded")
    # no exceptions - run from etc!


def load_to_bronze_spark(table):
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
    # , '/home/user/shared_folder/postgres/postgresql-42.2.23.jar') \

    logging.info(f"Writing table {table} from {pg_conn.host} to Bronze")

    table_df = spark.read.jdbc(pg_url, table=table, properties=pg_properties)
    table_df.write.parquet(
        os.path.join('/', 'datalake', 'bronze', db_name, table),
        mode="overwrite")

    logging.info("Successfully loaded")
