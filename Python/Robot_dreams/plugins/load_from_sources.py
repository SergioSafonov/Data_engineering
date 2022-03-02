import os
import logging
import json
import psycopg2
import requests

from requests.exceptions import HTTPError

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

from hdfs import InsecureClient

from pyspark.sql import SparkSession
import pyspark.sql.functions as func

from get_config import get_config


def init_bronze_load():
    bronze_path = os.path.join('/', 'datalake', 'bronze')

    # dump API data to HDFS Bronze json file
    hdfs_conn = BaseHook.get_connection('datalake_hdfs')
    client_hdfs = InsecureClient(f"{hdfs_conn.host}:{hdfs_conn.port}/", user=hdfs_conn.login)

    return bronze_path, client_hdfs


def init_silver_load():
    bronze_path = os.path.join('/', 'datalake', 'bronze')
    silver_path = os.path.join('/', 'datalake', 'silver')

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/Distrib/postgresql-42.2.23.jar') \
        .master('local') \
        .appName('homework_7') \
        .getOrCreate()

    return bronze_path, silver_path, spark


def load_to_bronze_dshop(table, db_name, **kwargs):
    process_date = kwargs["execution_date"].strftime('%Y-%m-%d')
    file_name = table + '.csv'

    bronze_path, client_hdfs = init_bronze_load()

    pg_conn = BaseHook.get_connection('oltp_postgres')
    # db_name = pg_conn.schema
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

        with client_hdfs.write(os.path.join(bronze_path, db_name, table, process_date, file_name)
                , overwrite=True) as csv_file:
            cursor.copy_expert(f"COPY {table} TO STDOUT WITH HEADER CSV", csv_file)

    logging.info(f"Successfully loaded {table} from {pg_conn.host} to Bronze")


def load_to_silver_dshop(table, db_name, **kwargs):
    process_date = kwargs["execution_date"].strftime('%Y-%m-%d')
    file_name = table + '.csv'

    bronze_path, silver_path, spark = init_silver_load()

    if table == 'aisles':
        bronze_aisles_df = spark.read \
            .load(os.path.join(bronze_path, db_name, table, process_date, file_name),
                  header="true",
                  inferSchema="true",
                  format="csv"
                  )
        bronze_aisles_df = bronze_aisles_df.where(func.col('aisle').isNotNull())
        bronze_aisles_df = bronze_aisles_df.dropDuplicates()

        bronze_aisles_df.write \
            .parquet(os.path.join(silver_path, db_name, table)
                     , mode='overwrite')


def load_out_of_stocks_config(process_date):
    load_out_of_stocks(process_date)


def load_out_of_stocks_current(**kwargs):
    process_date = kwargs["execution_date"].strftime('%Y-%m-%d')

    load_out_of_stocks(process_date)


def load_out_of_stocks(process_date):
    config = get_config()
    config_data = config.get('rd_dreams_app')

    try:
        # read authentication data from config
        auth_url = config_data['url'] + config_data['auth_endpoint']
        headers = {"content-type": f"{config_data['content-type']}"}
        data = {"username": f"{config_data['username']}", "password": f"{config_data['password']}"}

        # get auth token
        token_request = requests.post(auth_url, headers=headers, data=json.dumps(data), timeout=10)
        token_request.raise_for_status()
        authentication_token = token_request.json()['access_token']

        logging.info(f"Successfully get auth token!")

    except HTTPError:
        raise AirflowException("Error get auth token!")

    try:
        bronze_path, client_hdfs = init_bronze_load()

        result_dir = os.path.join(bronze_path, 'rd_payload', process_date)
        file_name = config_data['file_name']

        # read API data from config_data
        api_url = config_data['url'] + config_data['endpoint']
        api_headers = {"content-type": f"{config_data['content-type']}",
                       "Authorization": f"{config_data['auth_prefix']}" + authentication_token}
        processed_data = {"date": f"{process_date}"}

        # request API data
        logging.info(f"Getting data from Out_of_stocks API data for {process_date}")
        api_data = requests.get(api_url, headers=api_headers, data=json.dumps(processed_data), timeout=10)
        api_data.raise_for_status()

        api_data_json = api_data.json()
        logging.info(f"Successfully get Out_of_stocks API data for {process_date}!")

        with client_hdfs.write(os.path.join(result_dir, file_name),
                               encoding='utf-8', overwrite=True, blocksize=1048576, replication=1) as api_data_file:
            json.dump(api_data_json, api_data_file)

        logging.info(f"Successfully load to Bronze {file_name} for {process_date}!")

    except HTTPError:
        raise AirflowException(f"Not Out_of_stocks API data for {process_date}!")
