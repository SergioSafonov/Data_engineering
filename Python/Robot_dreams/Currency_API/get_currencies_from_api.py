import json
import os
import requests     # previously run in Terminal: >pip3.8 install requests

def app():

    currencies = ['USD', 'GBP', 'RUB', 'UAH', 'PLN']
    for currency in currencies:
        url = f"http://api.exchangeratesapi.io/v1/latest?access_key=a0ec6e79d368336768d78dd4bfc06e1f&symbols={currency}"
        response = requests.get(url)
        print(response.status_code)

        dir_name = 'data/'
        file_name = f'{currency}_to_EUR.json'
        with open(os.path.join('.', dir_name, file_name), 'w') as f:
            data = response.json()
            rates = data['rates']
            json.dump(rates, f)


# for direct call this function app()
if __name__ == '__main__':
    app()

