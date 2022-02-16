import json
import os
import requests     # previously run in Terminal: >pip3.8 install requests


def app():

        url = "http://api.exchangeratesapi.io/v1/symbols?access_key=a0ec6e79d368336768d78dd4bfc06e1f"
        response = requests.get(url)

        dir_name = os.path.join('../data/currencies')
        os.makedirs(dir_name, exist_ok=True)

        file_name = 'symbols.json'
        with open(os.path.join(dir_name, file_name), 'w') as f:
            data = response.json()
            rates = data['symbols']
            json.dump(rates, f)


# for direct call this function app()
if __name__ == '__main__':
    app()
