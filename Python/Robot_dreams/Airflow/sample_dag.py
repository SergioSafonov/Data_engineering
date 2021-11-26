from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime


sample_dag = DAG(
    dag_id='sample_dag',
    description='Our first DAG',
    start_date=datetime(2021, 7, 7, 14, 30),
    end_date=datetime(2021, 10, 7, 14, 30),
    schedule_interval='@daily'
)

t1 = BashOperator(
    task_id='1st_bash',
    bash_command='date',
    dag=sample_dag
)

t2 = BashOperator(
    task_id='2st_bash',
    bash_command='sleep 5',
    dag=sample_dag
)

t3 = BashOperator(
    task_id='3st_bash',
    bash_command='date',
    dag=sample_dag
)

t1 >> t2 >> t3
