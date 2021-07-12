import requests
import os
import json

from config import Config
from requests.exceptions import HTTPError
from datetime import date


def get_auth_token(config_data):
    try:
        # read authentication data from config
        auth_url = config_data['url'] + config_data['auth_endpoint']
        headers = {"content-type": f"{config_data['content-type']}"}
        data = {"username": f"{config_data['username']}", "password": f"{config_data['password']}"}

        # get auth token
        token_request = requests.post(auth_url, headers=headers, data=json.dumps(data), timeout=10)
        token_request.raise_for_status()
        auth_token = token_request.json()['access_token']

        return auth_token

    except HTTPError:
        print('Error get auth token!')


def rd_dreams_run(config_data, token, process_date=None):
    if process_date:
        pass
    else:
        process_date = str(date.today())

    try:
        # check date folder
        # config_date = config_data['payload']
        # for process_date in config_date:

        os.makedirs(os.path.join(process_date), exist_ok=True)

        # read API data from config_data
        api_url = config_data['url'] + config_data['endpoint']
        api_headers = {"content-type": f"{config_data['content-type']}", "Authorization": f"{config_data['auth_prefix']}" + token}
        used_data = {"date": f"{process_date}"}

        # request API data
        result = requests.get(api_url, headers=api_headers, data=json.dumps(used_data), timeout=10)
        result.raise_for_status()

        # dump API data to json file
        dir_name = process_date
        file_name = 'api_values.json'
        with open(os.path.join('.', dir_name, file_name), 'w') as json_file:
            result_data = result.json()
            json.dump(result_data, json_file)

    except HTTPError:
        print('Error data API request!')


if __name__ == '__main__':
    conf = Config(os.path.join('.', 'config.yaml'))

    auth_token = get_auth_token(
        config_data=conf.get_config('rd_dreams_app')
        )

    payload = ["2021-01-02"]
    for dt in payload:

        rd_dreams_run(
            config_data=conf.get_config('rd_dreams_app'),
            process_date=dt,
            token=auth_token
            )