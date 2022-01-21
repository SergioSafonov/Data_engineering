from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from get_RD_DL_data import rd_dreams_run

# default_args = {
#     'owner': 'airflow',
#     'params': {
#         'payload_date1': '2021-07-05'
#         'payload_date2': '2021-07-06'
#     }
# }

RD_payload_DL_dag = DAG(
    dag_id='HW_5_dag',
    description='DAG for getting payload data from RD API to Bronze Data Lake',
    # default_args=default_args,
    start_date=datetime(2022, 1, 18, 1, 00),
    end_date=datetime(2022, 12, 18, 1, 00),
    schedule_interval='@daily'
)


PythonTask = PythonOperator(
        task_id=f"RD_payload_DL_task",
        dag=RD_payload_DL_dag,
        python_callable=rd_dreams_run,
        # op_kwargs={'process_date': used_date},
        task_concurrency=1,
        provide_context=True
    )


dummy1 = DummyOperator(task_id="dummy1", dag=RD_payload_DL_dag)
dummy2 = DummyOperator(task_id="dummy2", dag=RD_payload_DL_dag)

# for payload_date in RD_payload_DL_dag.params.values():
dummy1 >> PythonTask >> dummy2
