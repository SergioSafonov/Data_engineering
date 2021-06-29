import requests
import os
import json

from config import Config
from requests.exceptions import HTTPError


def rd_dreams_run(config):

    try:
        # check date folder
        process_date = config['payload']
        os.makedirs(os.path.join(process_date), exist_ok=True)

        # read authentication data from config
        auth_url = config['url'] + config['auth_endpoint']
        headers = {"content-type": f"{config['content-type']}"}
        data = {"username": f"{config['username']}", "password": f"{config['password']}"}

        # get auth token
        token_request = requests.post(auth_url, headers=headers, data=json.dumps(data))
        token_request.raise_for_status()
        token = token_request.json()['access_token']

        # read API data from config
        api_url = config['url'] + config['endpoint']
        api_headers = {"content-type": f"{config['content-type']}", "Authorization": f"{config['auth_prefix']}" + token}
        used_data = {"date": f"{process_date}"}

        # request API data
        result = requests.get(api_url, headers=api_headers, data=json.dumps(used_data))
        result.raise_for_status()

        # dump API data to json file
        dir_name = process_date
        file_name = 'first_2_values.json'
        with open(os.path.join('.', dir_name, file_name), 'w') as json_file:
            result_data = result.json()
            json.dump(result_data, json_file)

    except HTTPError:
        print('Code error!')


if __name__ == '__main__':
    config = Config(os.path.join('.', 'config.yaml'))

    rd_dreams_run(
        config=config.get_config('rd_dreams_app')
    )
