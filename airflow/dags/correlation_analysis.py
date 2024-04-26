import datetime
import pandas as pd

import airflow
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator


from helpers.correlation.getData_correlation import (
    extract_esdr,
    extract_smell
)

from helpers.correlation.correlation import (
    correlation_analysis_esdr,
    correlation_analysis_smell,
    mergeDataFrames
)

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = airflow.DAG(
    dag_id='correlation_analysis',
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

esdr = PythonOperator(
    task_id='all_esdr',
    python_callable=correlation_analysis_esdr,
    op_kwargs={
        "path": "data/esdr/"
    },
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

smell = PythonOperator(
    task_id='smell',
    python_callable=correlation_analysis_smell,
    op_kwargs={
        "path": "data/correlation_analisys/smell_correlation/smell_report_correlation.csv"
    },
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

merge = PythonOperator(
    task_id='merge',
    python_callable=mergeDataFrames,
    op_kwargs={
        "esdr_path": "data/correlation_analisys/correlation_esdr.csv",
        "smell_path": "data/correlation_analisys/correlation_smell.csv"
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

start  >> extract_esdr_data >> extract_smell_data >> esdr >> smell >> merge >> end

