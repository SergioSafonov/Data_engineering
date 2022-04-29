import os
import logging
import json
import psycopg2
import requests

from requests.exceptions import HTTPError

from airflow.operators.http_operator import SimpleHttpOperator
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException

from get_config import get_config, get_hadoop_path, get_table_types


def dshopbu_bronze_load(table, client_hdfs, pg_conn, **kwargs):
    process_date = kwargs["ds"]         # today
    order_date = kwargs["prev_ds"]      # yesterday

    db_name = pg_conn.schema
    file_name = table + '.csv'

    try:
        bronze_path = get_hadoop_path('bronze')

        pg_creds = {
            'host': pg_conn.host,
            'port': pg_conn.port,
            'database': db_name,
            'user': pg_conn.login,
            'password': pg_conn.password
        }

        logging.info(f"Writing table {table} from {pg_conn.host}:{db_name} to Bronze")

        source_column_list, target_column_list, rename_str, csv_schema_str = get_table_types(table)

        with psycopg2.connect(**pg_creds) as pg_connection:
            cursor = pg_connection.cursor()

            with client_hdfs.write(os.path.join(bronze_path, db_name, table, process_date, file_name)
                    , overwrite=True) as csv_file:
                if table != 'orders':
                    cursor.copy_expert(f"COPY {table} TO STDOUT WITH HEADER CSV", csv_file)
                else:       # for orders fact table copy only yesterday data (by order_date)
                    cursor.copy_expert(
                        f"COPY (SELECT {source_column_list} FROM public.{table}"
                        f" WHERE order_date BETWEEN '{order_date} 00:00:00' AND '{order_date} 23:59:59')"
                        " TO STDOUT WITH HEADER CSV", csv_file)

        logging.info(f"Successfully loaded {table} from {pg_conn.host}:{db_name} to Bronze")

    except HTTPError:
        raise AirflowException(f"Error load {table} from {pg_conn.host}:{db_name} to Bronze")


def out_of_stocks_config_load(process_date, client_hdfs):
    out_of_stocks_load(process_date, client_hdfs)


def out_of_stocks_current_load(client_hdfs, **kwargs):
    process_date = kwargs["ds"]

    out_of_stocks_load(process_date, client_hdfs)


def out_of_stocks_load(process_date, client_hdfs):
    config = get_config()
    config_data = config.get('rd_dreams_app')

    bronze_path = get_hadoop_path('bronze')

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
        dir_name = config_data['directory']
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

        with client_hdfs.write(os.path.join(bronze_path, dir_name, process_date, file_name),
                               encoding='utf-8', overwrite=True, blocksize=1048576, replication=1) as api_data_file:
            json.dump(api_data_json, api_data_file)

        logging.info(f"Successfully load to Bronze {file_name} for {process_date}!")

    except HTTPError:
        raise AirflowException(f"Not Out_of_stocks API data for {process_date}!")


class CurrencyAPISaveHttpOperator(SimpleHttpOperator):  # define child extended class for SimpleHttpOperator

    def __init__(self, save_hdfs, client_hdfs, currency_path, context_date, *args, **kwargs):
        super(CurrencyAPISaveHttpOperator, self).__init__(*args, **kwargs)  # init as a parent - SimpleHttpOperator

        self.save_flag = save_hdfs  # added save_flag as input parameter
        self.client_hdfs = client_hdfs
        self.currency_path = currency_path
        self.context_date = context_date

    def execute(self, context):
        # initially copied from SimpleHttpOperator execute method

        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling Currency API HTTP method")

        # process_date = str(date.today())  # - timedelta(days=1))
        if self.context_date and self.context_date in context:
            process_date = context[self.context_date]
        else:
            process_date = context['ds']

        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)

        if self.log_response:
            self.log.info(response.text)

        if self.response_check:

            if not self.response_check(response):
                raise AirflowException("Currency API response check returned False.")

        api_data = response.json()

        if self.save_flag:  # added check for save_flag
            bronze_path = get_hadoop_path('bronze')

            directory = os.path.join(bronze_path, self.currency_path, process_date)
            # os.makedirs(directory, exist_ok=True)
            file_name = self.data['symbols'] + '_' + self.data['currency_base'] + '.json'

            with self.client_hdfs.write(os.path.join(directory, file_name),
                                        encoding='utf-8', overwrite=True, blocksize=1048576,
                                        replication=1) as json_file:
                json.dump(api_data, json_file)

            self.log.info(f"Writing to file {os.path.join(directory, file_name)}")

        if self.xcom_push_flag:  # if you need to store result into etc. Scoms
            return response.text
