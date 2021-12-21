from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

sample_dag = DAG(
    dag_id='sample_dag',
    description='Our first DAG',
    start_date=datetime(2021, 12, 15, 14, 30),
    end_date=datetime(2022, 12, 15, 14, 30),
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
    dag=sample_dag,
    retries=2,
    retry_delay=timedelta(seconds=120)
)

t4 = BashOperator(
    task_id='4st_bash',
    bash_command='date',
    dag=sample_dag
)

t1 >> (t2, t3) >> t4        # t2, t3 in parallel, t1 -> t4 - sequentially
