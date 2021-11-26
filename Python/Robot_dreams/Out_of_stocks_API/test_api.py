import requests
import json


def run():
    url = "https://robot-dreams-de-api.herokuapp.com/auth"
    headers = {"content-type": "application/json"}
    data = {"username": "rd_dreams", "password": "djT6LasE"}
    r = requests.post(url, headers=headers, data=json.dumps(data))
    token = r.json()['access_token']
    # print(token)  # get api token key, will be obsolete

    url = "https://robot-dreams-de-api.herokuapp.com/out_of_stock"
    headers = {"content-type": "application/json", "Authorization": "JWT " + token}
    data = {"date": "2021-03-06"}
    r = requests.get(url, headers=headers, data=json.dumps(data))
    print(r.json()[0])
    print(r.json()[1])
    print(len(r.json()))


# if direct run test_api.py
if __name__ == '__main__':
    run()