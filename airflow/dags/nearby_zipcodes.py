import datetime
import pandas as pd


df_metrics = pd.DataFrame(columns=['source', 'nodes', 'edges'])

import airflow
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from helpers import (
    insert_smell_data_in_neo4j,
    inster_nearby_zipcodes,
    delete_all
)

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = airflow.DAG(
    dag_id='nearby_zipcodes',
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

delete_all_neo4j = PythonOperator(
    task_id='delete_all_neo4j',
    python_callable = delete_all,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

insert_smell_data_neo4j = PythonOperator(
    task_id='insert_smell_data_neo4j',
    python_callable = insert_smell_data_in_neo4j,
    op_args=[df_metrics],
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

inster_nearby_zipcodes_neo4j = PythonOperator(
    task_id='inster_nearby_zipcodes_neo4j',
    python_callable = inster_nearby_zipcodes,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)


end = DummyOperator(
    task_id='end', 
    dag=dag,
    trigger_rule='all_done',
)


start >> delete_all_neo4j >>  insert_smell_data_neo4j >>  inster_nearby_zipcodes_neo4j >> end