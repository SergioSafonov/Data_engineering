from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
from get_currency import def_currency       # run inside Airflow. get_currency_old.py at ~/airflow/plugins/


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
    python_callable=def_currency
)
