import json
import os
import requests  # previously run in Terminal: >pip3.8 install requests


def app():
    currencies = ['AED', 'AUD', 'CAD', 'CHF', 'CZK', 'GBP', 'HRK', 'IDR', 'ILS', 'INR', 'JPY', 'NOK', 'SEK', 'USD',
                  'ZAR']

    for currency in currencies:
        url = f"http://api.exchangeratesapi.io/v1/latest?access_key=a0ec6e79d368336768d78dd4bfc06e1f&symbols={currency}"
        response = requests.get(url)
        print(response.status_code)

        dir_name = os.path.join('../data/latest/')
        os.makedirs(dir_name, exist_ok=True)

        file_name = f'{currency}_to_EUR.json'
        with open(os.path.join(dir_name, file_name), 'w') as f:
            data = response.json()
            rates = data['rates']
            json.dump(rates, f)


# for direct call this function app()
if __name__ == '__main__':
    app()
