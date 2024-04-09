import datetime
import pandas as pd

import airflow
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from helpers import (
    delete_all,
    insert_smell_data_in_neo4j,
    insert_esdr_1_neo4j,
    insert_esdr_3_neo4j,
    insert_esdr_23_neo4j,
    insert_esdr_24_neo4j,
    insert_esdr_26_neo4j,
    insert_esdr_27_neo4j,
    insert_esdr_28_neo4j,
    insert_esdr_29_neo4j,
    insert_esdr_43_neo4j,
    insert_esdr_3506_neo4j,
    insert_esdr_3508_neo4j,
    insert_esdr_5975_neo4j,
)

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = airflow.DAG(
    dag_id='neo4j',
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
    task_id='delete_all',
    python_callable = delete_all,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

insert_smell_data_neo4j = PythonOperator(
    task_id='insert_smell_data_neo4j',
    python_callable = insert_smell_data_in_neo4j,
    op_args=[df_metrics_neo4j],
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

insert_esdr_1_data_neo4j = PythonOperator(
    task_id='insert_esdr_1_data_neo4j',
    python_callable = insert_esdr_1_neo4j,
    op_args=[df_metrics_neo4j],
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

insert_esdr_3_data_neo4j = PythonOperator(
    task_id='insert_esdr_3_data_neo4j',
    python_callable = insert_esdr_3_neo4j,
    op_args=[df_metrics_neo4j],
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)


insert_esdr_23_data_neo4j = PythonOperator(
    task_id='insert_esdr_23_data_neo4j',
    python_callable = insert_esdr_23_neo4j,
    op_args=[df_metrics_neo4j],
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

insert_esdr_24_data_neo4j = PythonOperator(
    task_id='insert_esdr_24_data_neo4j',
    python_callable = insert_esdr_24_neo4j,
    op_args=[df_metrics_neo4j],
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

insert_esdr_26_data_neo4j = PythonOperator(
    task_id='insert_esdr_26_data_neo4j',
    python_callable = insert_esdr_26_neo4j,
    op_args=[df_metrics_neo4j],
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

insert_esdr_27_data_neo4j = PythonOperator(
    task_id='insert_esdr_27_data_neo4j',
    python_callable = insert_esdr_27_neo4j,
    op_args=[df_metrics_neo4j],
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)


insert_esdr_28_data_neo4j = PythonOperator(
    task_id='insert_esdr_28_data_neo4j',
    python_callable = insert_esdr_28_neo4j,
    op_args=[df_metrics_neo4j],
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

insert_esdr_29_data_neo4j = PythonOperator(
    task_id='insert_esdr_29_data_neo4j',
    python_callable = insert_esdr_29_neo4j,
    op_args=[df_metrics_neo4j],
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

insert_esdr_43_data_neo4j = PythonOperator(
    task_id='insert_esdr_43_data_neo4j',
    python_callable = insert_esdr_43_neo4j,
    op_args=[df_metrics_neo4j],
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

insert_esdr_3506_data_neo4j = PythonOperator(
    task_id='insert_esdr_3506_data_neo4j',
    python_callable = insert_esdr_3506_neo4j,
    op_args=[df_metrics_neo4j],
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

insert_esdr_3508_data_neo4j = PythonOperator(
    task_id='insert_esdr_3508_data_neo4j',
    python_callable = insert_esdr_3508_neo4j,
    op_args=[df_metrics_neo4j],
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

insert_esdr_5975_data_neo4j = PythonOperator(
    task_id='insert_esdr_5975_data_neo4j',
    python_callable = insert_esdr_5975_neo4j,
    op_args=[df_metrics_neo4j],
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

end = DummyOperator(
    task_id='end', 
    dag=dag,
    trigger_rule='all_done',
)


start >> delete_all_neo4j >> insert_smell_data_neo4j >> [insert_esdr_1_data_neo4j,insert_esdr_3_data_neo4j,insert_esdr_23_data_neo4j,insert_esdr_24_data_neo4j,insert_esdr_26_data_neo4j,insert_esdr_27_data_neo4j,insert_esdr_28_data_neo4j,insert_esdr_29_data_neo4j,insert_esdr_43_data_neo4j,insert_esdr_3506_data_neo4j,insert_esdr_3508_data_neo4j,insert_esdr_5975_data_neo4j] >> end