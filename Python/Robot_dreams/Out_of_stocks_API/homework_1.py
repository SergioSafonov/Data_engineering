import json
import os
import sys
import requests

from datetime import date
from datetime import timedelta

from requests.exceptions import HTTPError

from Config.config import Config


def get_dates(payload_date=None):
    if not payload_date:
        argv_len = len(sys.argv) - 1    # get command line argument length.

        if argv_len < 1:                # if no data list parameters -> use date - month()
            params = str(date.today() - timedelta(days=30))
        else:
            params = ''
            for i in range(argv_len):  # loop in all arguments.
                params = params + sys.argv[i + 1]

                if i + 1 < argv_len:
                    params = f"{params} "

        payload_date = list(params.split(" "))      # list of date parameters

    return payload_date


def get_auth_token(config_data):
    try:
        # read authentication data from config
        auth_url = config_data['url'] + config_data['auth_endpoint']
        headers = {"content-type": f"{config_data['content-type']}"}
        data = {"username": f"{config_data['username']}", "password": f"{config_data['password']}"}

        # get auth token
        token_request = requests.post(auth_url, headers=headers, data=json.dumps(data), timeout=10)
        token_request.raise_for_status()
        authentication_token = token_request.json()['access_token']

        return authentication_token

    except HTTPError:
        print('Error get auth token!')


def rd_dreams_run(config_data, process_date, token):
    try:
        # check date folder
        data_path = os.path.join('..', config_data['directory'], process_date)
        os.makedirs(data_path, exist_ok=True)

        # read API data from config_data
        api_url = config_data['url'] + config_data['endpoint']
        api_headers = {"content-type": f"{config_data['content-type']}",
                       "Authorization": f"{config_data['auth_prefix']}" + token}
        used_data = {"date": f"{process_date}"}

        # request API data
        result = requests.get(api_url, headers=api_headers, data=json.dumps(used_data), timeout=10)
        # print(result)
        result.raise_for_status()

        # dump API data to json file
        file_name = 'api_values.json'
        with open(os.path.join(data_path, file_name), 'w') as json_file:
            result_data = result.json()
            json.dump(result_data, json_file)

    except HTTPError:
        print('Error data API request!')


# run with parameters: '2021-07-02 2021-07-03'
if __name__ == '__main__':
    # pay_date = ['2021-07-02', '2021-07-03']
    # payload_dates = get_dates(pay_date)
    payload_dates = get_dates()

    conf = Config(os.path.join('..', 'Config', 'config.yaml'))
    auth_token = get_auth_token(
        config_data=conf.get_config('rd_dreams_app')
    )

    for dt in payload_dates:
        rd_dreams_run(
            config_data=conf.get_config('rd_dreams_app'),
            process_date=dt,
            token=auth_token
        )
