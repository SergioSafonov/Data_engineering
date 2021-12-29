from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
from python_func import py_func       # run inside Airflow. python_func.py at ~/airflow/plugins/

python_dag = DAG(
    dag_id='python_dag',
    description='Our Python DAG',
    start_date=datetime(2021, 12, 15, 14, 30),
    end_date=datetime(2022, 12, 15, 14, 30),
    schedule_interval='@daily'
)

t1 = PythonOperator(
    task_id='python_task',
    dag=python_dag,
    python_callable=py_func,
    provide_context=True        # put Airflow params into function py_func
)