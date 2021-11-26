from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
# from some_file import py_func


def py_func(**kwargs):
    # **kwargs - get any function params
    print('Hello from inside Python function')
    print(f'Param: {kwargs["execution_date"]}')
    a = 10
    a = a * a * a
    print(a)


python_dag = DAG(
    dag_id='python_dag',
    description='Our Python DAG',
    start_date=datetime(2021, 7, 7, 14, 30),
    end_date=datetime(2021, 10, 7, 14, 30),
    schedule_interval='@daily'
)

t1 = PythonOperator(
    task_id='python_task',
    dag=python_dag,
    python_callable=py_func,
    provide_context=True        # put Airflow params into function py_func
)