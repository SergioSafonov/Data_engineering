import requests
import os
import json

from config import Config
from requests.exceptions import HTTPError
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresHook

payload_dates = ['2021-01-02', '2021-01-03']


def rd_dreams_run(process_date):

    global authentication_token
    start_dir = os.path.join('/', 'home', 'user')
    conf = Config(os.path.join(start_dir, 'airflow', 'plugins', 'config.yaml'))
    config_data = conf.get_config('rd_dreams_app')

    try:
        # read authentication data from config
        auth_url = config_data['url'] + config_data['auth_endpoint']
        headers = {"content-type": f"{config_data['content-type']}"}
        data = {"username": f"{config_data['username']}", "password": f"{config_data['password']}"}

        # get auth token
        token_request = requests.post(auth_url, headers=headers, data=json.dumps(data), timeout=10)
        token_request.raise_for_status()
        authentication_token = token_request.json()['access_token']

    except HTTPError:
        print('Error get auth token!')

    try:
        # check date folder
        result_dir = os.path.join(start_dir, 'data', 'rd_payload', process_date)
        os.makedirs(os.path.join(result_dir), exist_ok=True)

        # read API data from config_data
        api_url = config_data['url'] + config_data['endpoint']
        api_headers = {"content-type": f"{config_data['content-type']}",
                       "Authorization": f"{config_data['auth_prefix']}" + authentication_token}
        used_data = {"date": f"{process_date}"}

        # request API data
        result = requests.get(api_url, headers=api_headers, data=json.dumps(used_data), timeout=10)
        result.raise_for_status()

        # dump API data to json file
        file_name = 'api_values.json'
        with open(os.path.join(result_dir, file_name), 'w') as json_file:
            result_data = result.json()
            json.dump(result_data, json_file)

    except HTTPError:
        print('Error data API request!')


def postgres_copy(table_name):

    file_name = table_name + '.tsv'
    result_dir = os.path.join('/', 'home', 'user', 'data', 'postgres_data', 'dshop')
    os.makedirs(os.path.join(result_dir), exist_ok=True)

    postgres = PostgresHook(postgres_conn_id='postgres_dshop')
    postgres.bulk_dump(table_name, os.path.join(result_dir, file_name))


def group_payload(used_date):
    return PythonOperator(
        task_id=f"get_RD_payload_task_{used_date}",
        dag=RD_payload_dag,
        python_callable=rd_dreams_run,
        op_kwargs={'process_date': used_date},
        # op_args=used_date,
        task_concurrency=5
    )


def postgres_dump(table):
    return PythonOperator(
        task_id=f"Copy_dshop_{table}_task",
        dag=RD_payload_dag,
        python_callable=postgres_copy,
        op_kwargs={'table_name': table},
        task_concurrency=5
    )


RD_payload_dag = DAG(
    dag_id='RD_payload_dag',
    description='DAG for getting payload data from RD API',
    start_date=datetime(2021, 7, 19, 1, 00),
    end_date=datetime(2021, 9, 19, 1, 00),
    schedule_interval='@daily'
)

dummy1 = DummyOperator(task_id="dummy1", dag=RD_payload_dag)
dummy2 = DummyOperator(task_id="dummy2", dag=RD_payload_dag)

for payload_date in payload_dates:
    dummy1 >> group_payload(payload_date) \
>> (
    postgres_dump('aisles'),
    postgres_dump('clients'),
    postgres_dump('departments'),
    postgres_dump('orders'),
    postgres_dump('products')
)>> dummy2
