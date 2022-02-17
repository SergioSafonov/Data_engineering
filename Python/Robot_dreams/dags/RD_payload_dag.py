import requests
import os
import json
import logging

from requests.exceptions import HTTPError
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from get_config import get_config, get_payload_dates

config = get_config()
config_data = config.get('rd_dreams_app')
payload_dates = get_payload_dates()


def rd_dreams_run(process_date):
    global authentication_token

    start_dir = os.path.join('/', 'home', 'user')
    data_path = config_data['directory']

    try:
        # read authentication data from config
        auth_url = config_data['url'] + config_data['auth_endpoint']
        headers = {"content-type": f"{config_data['content-type']}"}
        data = {"username": f"{config_data['username']}", "password": f"{config_data['password']}"}

        # get auth token
        token_request = requests.post(auth_url, headers=headers, data=json.dumps(data), timeout=10)
        token_request.raise_for_status()
        authentication_token = token_request.json()['access_token']

        logging.info("Successfully got authentication token")

    except HTTPError:
        print('Error get auth token!')
        raise Exception

    try:
        # check date folder
        result_dir = os.path.join(start_dir, data_path, process_date)
        os.makedirs(os.path.join(result_dir), exist_ok=True)

        # read API data from config_data
        api_url = config_data['url'] + config_data['endpoint']
        api_headers = {"content-type": f"{config_data['content-type']}",
                       "Authorization": f"{config_data['auth_prefix']}" + authentication_token}
        processed_data = {"date": f"{process_date}"}

        # request API data
        logging.info(f"Start request api data for{process_date}")
        result = requests.get(api_url, headers=api_headers, data=json.dumps(processed_data), timeout=10)
        result.raise_for_status()

        # dump API data to json file
        file_name = 'api_values.json'
        logging.info(f"Start dump api file {file_name} for {process_date}")

        with open(os.path.join(result_dir, file_name), 'w') as json_file:
            result_data = result.json()
            json.dump(result_data, json_file)

        logging.info("Successfully loaded")

    except HTTPError:
        print('Error data API request!')
        raise Exception


def group_payload(used_date):
    return PythonOperator(
        task_id=f"RD_payload_task_{used_date}",
        dag=RD_payload_dag,
        python_callable=rd_dreams_run,
        op_kwargs={'process_date': used_date},
        # op_args=used_date,
        task_concurrency=1
    )


RD_payload_dag = DAG(
    dag_id='RD_payload_dag',
    description='DAG for getting payload data from RD API to local json file',
    start_date=datetime(2021, 7, 23, 1, 00),
    end_date=datetime(2021, 9, 23, 1, 00),
    schedule_interval='@daily'
)

dummy1 = DummyOperator(task_id="dummy1", dag=RD_payload_dag)
dummy2 = DummyOperator(task_id="dummy2", dag=RD_payload_dag)

for payload_date in payload_dates:
    dummy1 >> group_payload(payload_date) >> dummy2
