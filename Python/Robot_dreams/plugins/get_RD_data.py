import os
import requests
import json

from get_config import get_config
from requests.exceptions import HTTPError
from airflow.exceptions import AirflowException
# from airflow.hooks.base_hook import BaseHook


def rd_dreams_run(process_date):
    config = get_config()
    config_data = config.get('rd_dreams_app')

    try:
        # read authentication data from config
        # http_conn = BaseHook.get_connection('RD_connection')

        auth_url = config_data['url'] + config_data['auth_endpoint']
        # auth_url = http_conn.host + config_data['auth_endpoint']

        headers = {"content-type": f"{config_data['content-type']}"}
        data = {"username": f"{config_data['username']}", "password": f"{config_data['password']}"}
        # data = {"username": f"{http_conn.login}", "password": f"{http_conn.password}"}

        # get auth token
        token_request = requests.post(auth_url, headers=headers, data=json.dumps(data), timeout=10)
        token_request.raise_for_status()
        authentication_token = token_request.json()['access_token']

    except HTTPError:
        print('Error get auth token!')
        raise AirflowException("Error get auth token!")
        # raise Exception

    try:
        # check date folder
        data_path = config_data['directory']
        result_dir = os.path.join('/', 'home', 'user', data_path, process_date)
        os.makedirs(os.path.join(result_dir), exist_ok=True)

        # read API data from config_data
        api_url = config_data['url'] + config_data['endpoint']
        # api_url = http_conn.host + config_data['endpoint']
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
        raise AirflowException("Error data API request!")
        # raise Exception
