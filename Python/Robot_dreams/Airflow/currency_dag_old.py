import json
import logging
import os
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from requests import HTTPError
# from Airflow.python_dag import py_func (get_currency)
# / - absolute path, . - current path


def get_currency():
    process_date = "2021-07-08"
    directory = os.path.join('data', process_date)      # /home/user/data/
    currencies = ['UAH', 'GBP', 'USD', 'PLN', 'RUB']
    url = "http://api.exchangeratesapi.io/v1/"
    url = url + '/' + process_date
    access_key = "a0ec6e79d368336768d78dd4bfc06e1f"

    os.makedirs(directory, exist_ok=True)

    try:
        for currency in currencies:
            logging.info(f"Processing {currency} for {process_date}")

            params = {'access_key': access_key, 'symbols': currency}

            response = requests.get(url, params=params)
            response.raise_for_status()

            file_name = f'{currency}_to_EUR.json'
            with open(os.path.join(directory, file_name), 'w') as json_file:
                data = response.json()
                rates = data['rates']
                json.dump(rates, json_file)

    except HTTPError as e:
        logging.error(e)


currency_dag = DAG(
    dag_id='currency_dag',
    description='Our Python currency DAG',
    start_date=datetime(2021, 12, 15, 14, 30),
    end_date=datetime(2022, 12, 15, 14, 30),
    schedule_interval='@daily'
)

t1 = PythonOperator(
    task_id='currency_task',
    dag=currency_dag,
    python_callable=get_currency
)
