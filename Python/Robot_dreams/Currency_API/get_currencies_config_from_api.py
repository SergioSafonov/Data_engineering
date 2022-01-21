import json
import os
import requests  # previously run in Terminal: >pip3.8 install requests

from Config.config import Config
from requests.exceptions import HTTPError
from datetime import date


def app(config_set, process_date=None):
    if not process_date:
        process_date = str(date.today())

    data_path = os.path.join('..', config_set['directory'], process_date)
    os.makedirs(data_path, exist_ok=True)

    try:
        for currency in config_set['symbols']:
            url = config_set['url'] + '/' + process_date
            params = {'access_key': config_set['access_key'], 'symbols': currency}

            response = requests.get(url, params=params)
            # print(response)  if 400 - client error
            # response.status_code
            response.raise_for_status()

            file_name = f'{currency}_to_EUR.json'
            with open(os.path.join(data_path, file_name), 'w') as json_file:
                data = response.json()
                rates = data['rates']
                json.dump(rates, json_file)

    except HTTPError:
        print('Error!')


# for direct call this function app()
if __name__ == '__main__':
    config = Config(os.path.join('..', 'Config', 'config.yaml'))
    print(config.get_config('currency_app'))

    date_list = ['2021-06-20', '2021-06-21']
    for dt in date_list:
        app(config.get_config('currency_app'), dt)
