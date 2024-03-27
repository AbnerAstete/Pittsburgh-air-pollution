import datetime

import airflow
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from helpers import (
    extract_esdr,
    extract_smell,
    clean_esdr_1,
    clean_esdr_3,
    clean_esdr_23,
    clean_esdr_24,
    clean_esdr_26,
    clean_esdr_27,
    clean_esdr_28,
    clean_esdr_29,
    clean_esdr_43,
    clean_esdr_3506,
    clean_esdr_3508,
    clean_esdr_5975
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

extract_smell_data = PythonOperator(
    task_id='extract_smell_data',
    python_callable=extract_smell,
    op_kwargs={
        "url": "http://api.smellpittsburgh.org/"
    },
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

clean_esdr_1_data = PythonOperator(
    task_id='clean_esdr_1_data',
    python_callable=clean_esdr_1,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

clean_esdr_3_data = PythonOperator(
    task_id='clean_esdr_3_data',
    python_callable=clean_esdr_3,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

clean_esdr_23_data = PythonOperator(
    task_id='clean_esdr_23_data',
    python_callable=clean_esdr_23,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

clean_esdr_24_data = PythonOperator(
    task_id='clean_esdr_24_data',
    python_callable=clean_esdr_24,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

clean_esdr_26_data = PythonOperator(
    task_id='clean_esdr_26_data',
    python_callable=clean_esdr_26,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

clean_esdr_27_data = PythonOperator(
    task_id='clean_esdr_27_data',
    python_callable=clean_esdr_27,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

clean_esdr_28_data = PythonOperator(
    task_id='clean_esdr_28_data',
    python_callable=clean_esdr_28,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

clean_esdr_29_data = PythonOperator(
    task_id='clean_esdr_29_data',
    python_callable=clean_esdr_29,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

clean_esdr_43_data = PythonOperator(
    task_id='clean_esdr_43_data',
    python_callable=clean_esdr_43,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

clean_esdr_3506_data = PythonOperator(
    task_id='clean_esdr_3506_data',
    python_callable=clean_esdr_3506,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

clean_esdr_3508_data = PythonOperator(
    task_id='clean_esdr_3508_data',
    python_callable=clean_esdr_3508,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

clean_esdr_5975_data = PythonOperator(
    task_id='clean_esdr_5975_data',
    python_callable=clean_esdr_5975,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

end = DummyOperator(
    task_id='end', 
    dag=dag,
    trigger_rule='all_done',
)


start  >> extract_esdr_data >> extract_smell_data >> [clean_esdr_1_data,clean_esdr_3_data,clean_esdr_23_data,clean_esdr_24_data,clean_esdr_26_data,clean_esdr_27_data,clean_esdr_28_data,clean_esdr_29_data,clean_esdr_43_data,clean_esdr_3506_data,clean_esdr_3508_data,clean_esdr_5975_data] >> end