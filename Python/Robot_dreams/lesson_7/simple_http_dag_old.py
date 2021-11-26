from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime

currency_http_dag = DAG(
     dag_id='currency_http',
     description='Our 1st Http Currency DAG',
     start_date=datetime(2021, 7, 7, 14, 30),
     end_date=datetime(2021, 10, 7, 14, 30),
     schedule_interval='@daily'
 )

t1 = SimpleHttpOperator(
    task_id='get_currency',
    dag=currency_http_dag,
    method="GET",
    http_conn_id="new_currency_connection",
    endpoint="2021-07-07",
    data={'access_key': "a0ec6e79d368336768d78dd4bfc06e1f", 'symbols': "USD"},
    xcom_push=True,          # put (store) data to Airflow Xcoms - meta data
    save=True
 )
