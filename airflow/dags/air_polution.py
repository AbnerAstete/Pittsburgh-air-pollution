import datetime

import airflow
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from helpers import (
    extract_esdr
)

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = airflow.DAG(
    dag_id='air_polution',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)


# Nodes

start = DummyOperator(
    task_id='start', 
    dag=dag,
    trigger_rule='all_success',
)

extract_esdr_data = PythonOperator(
    task_id='extract_esdr_data',
    python_callable=extract_esdr,
    op_kwargs={
        "url": "https://esdr.cmucreatelab.org/"
    },
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

end = DummyOperator(
    task_id='end', 
    dag=dag,
    trigger_rule='all_done',
)


start  >> extract_esdr_data >> end