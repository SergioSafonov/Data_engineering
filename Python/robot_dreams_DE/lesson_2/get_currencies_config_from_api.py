import json
import os
import requests  # previously run in Terminal: >pip3.8 install requests

from config import Config
from requests.exceptions import HTTPError
from datetime import date


def app(config, process_date=None):
    if process_date:
        pass
    else:
        process_date = str(date.today())

    os.makedirs(os.path.join(config['directory'], process_date), exist_ok=True)

    try:
        for currency in config['symbols']:
            url = config['url'] + '/' + process_date
            params = {'access_key': config['access_key'], 'symbols': currency}

            response = requests.get(url, params=params)
#            print(response)         # if 400 - client error
#            response.status_code
            response.raise_for_status()

            file_name = f'{currency}_to_EUR.json'
            with open(os.path.join(config['directory'], process_date, file_name), 'w') as json_file:
                data = response.json()
                rates = data['rates']
                json.dump(rates, json_file)

    except HTTPError:
        print('Error!')


# for direct call this function app()
if __name__ == '__main__':
    config = Config(os.path.join('.', 'config.yaml'))
#   print(config.get_config('currency_app'))

    date = ['2021-06-20', '2021-06-21']
    for dt in date:
         app(
             config=config.get_config('currency_app')
             , process_date=dt
         )
