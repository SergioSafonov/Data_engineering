import json
import os

from airflow.operators.http_operator import SimpleHttpOperator
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook


class ComplexHttpOperator(SimpleHttpOperator):  # define child extended class for SimpleHttpOperator

    def __init__(self, save, save_path, *args, **kwargs):
        super(ComplexHttpOperator, self).__init__(*args, **kwargs)          # init as a parent - SimpleHttpOperator
        self.save_flag = save                                               # added save_flag as input parameter
        self.save_path = save_path                                          # added save_path as input parameter

    def execute(self, context):
        # initially copied from SimpleHttpOperator execute method

        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTP method")

        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)

        if self.log_response:
            self.log.info(response.text)

        if self.response_check:

            if not self.response_check(response):
                raise AirflowException("Response check returned False.")

        if self.save_flag:              # added check for save_flag

            # '/home/user/data/USD/[yyyy-mm-dd]/'
            directory = os.path.join('/', 'home', 'user', self.save_path, self.data['symbols'], self.endpoint)
            file_name = self.data['symbols'] + '_' + self.data['base'] + '.json'  # added var file_name
            os.makedirs(directory, exist_ok=True)

            with open(os.path.join(directory, file_name), 'w') as json_file:
                self.log.info(f"Writing to file {os.path.join(directory, file_name)}")
                data = response.json()
                json.dump(data, json_file)

        if self.xcom_push_flag:         # if need to store result into Airflow Scoms
            return response.text
