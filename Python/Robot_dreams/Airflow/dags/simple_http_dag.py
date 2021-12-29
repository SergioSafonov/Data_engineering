from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime

currency_http_dag = DAG(
    dag_id='currency_http',
    description='Our 1st Http Currency DAG',
    start_date=datetime(2021, 12, 26, 14, 30),
    end_date=datetime(2022, 12, 26, 14, 30),
    schedule_interval='@daily'
)

t1 = SimpleHttpOperator(
    task_id='get_currency',
    dag=currency_http_dag,
    method="GET",
    http_conn_id="new_currency_connection",
    endpoint="2021-12-01",
    data={'access_key': "a0ec6e79d368336768d78dd4bfc06e1f", 'symbols': "USD"},
    xcom_push=True,  # put (store) data to Airflow Xcoms - meta data
    save=True
)

# how to get Xcoms data from Airflow meta data
# t3 = SSHOperator(
#     ssh_conn_id='ssh_default',
#     task_id='ssh_xcom_check',
#     command='echo "{{ ti.xcom_pull(task_ids="get_currency") }}"',
#     dag=currency_http_dag
#     )
