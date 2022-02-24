import os
import logging
import json
import psycopg2
import requests

from datetime import date
from requests.exceptions import HTTPError

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

from hdfs import InsecureClient
# from pyspark.sql import SparkSession

from get_config import get_config


def load_to_bronze_dshop(table):  # , **kwargs):
    # process_date = kwargs["execution_date"].strftime('%Y-%m-%d')
    process_date = str(date.today())

    # start_path, spark = init_load()
    start_path = os.path.join('/', 'datalake', 'bronze')
    file_name = table + '.csv'

    hdfs_conn = BaseHook.get_connection('datalake_hdfs')
    client = InsecureClient(f"{hdfs_conn.host}:{hdfs_conn.port}/", user=hdfs_conn.login)

    pg_conn = BaseHook.get_connection('postgres_dshop')
    db_name = pg_conn.schema
    pg_creds = {
        'host': pg_conn.host,
        'port': pg_conn.port,
        'database': db_name,
        'user': pg_conn.login,
        'password': pg_conn.password
    }
    ''' 
    pg_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{db_name}"
    pg_properties = {"user": pg_conn.login, "password": pg_conn.password}
 
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/Distrib/postgresql-42.2.23.jar') \
        .master('local') \
        .appName('homework_7') \
        .getOrCreate()
    '''
    logging.info(f"Writing table {table} from {pg_conn.host} to Bronze")

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()

        with client.write(os.path.join(start_path, db_name, table, process_date, file_name)
                , overwrite=True) as csv_file:
            cursor.copy_expert(f"COPY {table} TO STDOUT WITH HEADER CSV", csv_file)
    '''
    table_df = spark.read.jdbc(pg_url, table=table, properties=pg_properties)
    table_df.write.csv(
        os.path.join(start_path, db_name, table, process_date, file_name), mode="overwrite")
    '''
    logging.info(f"Successfully loaded {table} from {pg_conn.host} to Bronze")


def load_out_of_stocks(process_date, **kwargs):
    # process_date = kwargs["execution_date"].strftime('%Y-%m-%d')

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
        # start_path, spark = init_load()
        start_path = os.path.join('/', 'datalake', 'bronze')
        result_dir = os.path.join(start_path, 'rd_payload', process_date)
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

        # dump API data to HDFS Bronze json file
        hdfs_conn = BaseHook.get_connection('datalake_hdfs')
        client_hdfs = InsecureClient(f"{hdfs_conn.host}:{hdfs_conn.port}/", user=hdfs_conn.login)

        with client_hdfs.write(os.path.join(result_dir, file_name),
                               encoding='utf-8', overwrite=True, blocksize=1048576, replication=1) as api_data_file:
            json.dump(api_data_json, api_data_file)
        '''
        api_values_df = spark.read.json(result_data)
        api_values_df.write.json(
            os.path.join(start_path, result_dir, file_name), mode="overwrite")
        '''
        logging.info(f"Successfully load to Bronze {file_name} for {process_date}!")

    except HTTPError:
        raise AirflowException(f"Error get API data for {process_date}!")
