import os
import json
import requests  # previously run in Terminal: >pip3.8 install requests

from config import Config
from requests.exceptions import HTTPError

from datetime import date
from datetime import timedelta


def get_currencies(config_set, process_date=None):
    if not process_date:
        process_date = config_set['last_url']       # latest

    data_path = os.path.join('..', config_set['directory'], process_date)
    os.makedirs(data_path, exist_ok=True)

    url = config_set['url'] + process_date

    base_currency = config_set['base']
    currencies = config_set['symbols']

    try:
        for currency in currencies:
            params = {'access_key': config_set['access_key'], 'base': base_currency, 'symbols': currency}

            response = requests.get(url, params=params, timeout=10)
            # print(response.status_code)  # if 400 - client error
            response.raise_for_status()

            file_name = f'{currency}_{base_currency}.json'
            with open(os.path.join(data_path, file_name), 'w') as json_file:
                data = response.json()
                rates = data['rates']
                json.dump(rates, json_file)

    except HTTPError:
        print('Error!')


if __name__ == '__main__':
#   config = Config(os.path.join('/', 'home', 'user', 'airflow', 'plugins', 'config.yaml'))
    config = Config(os.path.join('..', 'Config', 'config.yaml'))
    config_data = config.get_config('currency_app')

#   get_currencies(config_data)                    # get last currencies rates

#   date_list = ['2021-12-18', '2021-12-19']       # get for date currency rates
    daysago = 30
    for i in range(1, daysago):
        dt = str(date.today() - timedelta(days=i))
        print(dt)
        get_currencies(config_data, dt)

