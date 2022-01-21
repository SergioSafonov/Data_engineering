import requests
import os
import json

from requests.exceptions import HTTPError
from airflow.exceptions import AirflowException
from hdfs import InsecureClient

from get_config import get_config


# def rd_dreams_run(process_date, **kwargs):
def rd_dreams_run(**kwargs):

    global authentication_token

    # exec_date = date.isoformat(kwargs["execution_date"])
    exec_date = '2021-07-06'

    file_name = 'api_values.json'
    result_dir = os.path.join('/', 'datalake', 'bronze', 'rd_payload', exec_date)

    # config_path = Config(os.path.join('/', 'home', 'user', 'airflow', 'plugins', '../Homework_5/config.yaml'))
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

    except HTTPError:
        print('Error get auth token!')
        raise AirflowException("Error get auth token!")

    try:
        # read API data from config_data
        api_url = config_data['url'] + config_data['endpoint']
        api_headers = {"content-type": f"{config_data['content-type']}",
                       "Authorization": f"{config_data['auth_prefix']}" + authentication_token}
        processed_data = {"date": f"{exec_date}"}

        # request API data
        result = requests.get(api_url, headers=api_headers, data=json.dumps(processed_data), timeout=10)
        result.raise_for_status()

        result_data = result.json()

        # open HDFS Data Lake client
        client_hdfs = InsecureClient('http://127.0.0.1:50070/', user='user')

        # create result DL folder
        client_hdfs.makedirs(result_dir)

        # dump API data to HDFS Bronze json file
        with client_hdfs.write(os.path.join(result_dir, file_name),
                               encoding='utf-8', overwrite=True, blocksize=1048576, replication=1) as result_DL_file:
            json.dump(result_data, result_DL_file)

    except HTTPError:
        print('Error data API request!')
        raise AirflowException("Error data API request!")
