import json
import os

from airflow.operators.http_operator import SimpleHttpOperator
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook


class ComplexHttpOperator(SimpleHttpOperator):
    def __init__(self, save, *args, **kwargs):
        super(ComplexHttpOperator, self).__init__(*args, **kwargs)
        self.save_flag=save
        # self.save_path

    def execute(self, context):
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

        if self.save_flag:
            os.makedirs('/home/user/api/', exist_ok=True)
            with open('/home/user/api/data.json', 'w') as json_file:
                data = response.json()
                json.dump(data, json_file)

        if self.xcom_push_flag:
            return response.text


