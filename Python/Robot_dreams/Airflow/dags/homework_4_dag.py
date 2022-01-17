from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from get_RD_data import rd_dreams_run
from get_postgres_dshop import postgres_copy


payload_dates = ['2021-12-02', '2021-12-03']


def group_payload(used_date):
    return PythonOperator(
        task_id=f"get_RD_task_{used_date}",
        dag=RD_payload_dag,
        python_callable=rd_dreams_run,
        op_kwargs={'process_date': used_date},
        # op_args=used_date,
        task_concurrency=5
    )


def postgres_dump(table):
    return PythonOperator(
        task_id=f"Copy_{table}_task",
        dag=RD_payload_dag,
        python_callable=postgres_copy,
        op_kwargs={'table_name': table},
        task_concurrency=5
    )


RD_payload_dag = DAG(
    dag_id='homework_4_dag',
    description='DAG for getting payload data from RD API',
    start_date=datetime(2021, 7, 19, 1, 00),
    end_date=datetime(2021, 9, 19, 1, 00),
    schedule_interval='@daily'
)

dummy1 = DummyOperator(task_id="dummy1", dag=RD_payload_dag)
dummy2 = DummyOperator(task_id="dummy2", dag=RD_payload_dag)

for payload_date in payload_dates:
    dummy1 >> group_payload(payload_date) \
>> (
    postgres_dump('aisles'),
    postgres_dump('clients'),
    postgres_dump('departments'),
    postgres_dump('orders'),
    postgres_dump('products')
)>> dummy2
