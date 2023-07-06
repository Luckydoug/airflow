import os
import sys

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from datetime import datetime

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

from sub_tasks.Ajua.ajua_info import fetch_ajua_info

# from tmp.python_test
DAG_ID = 'Ajua_ETL_Pipeline_Wairimu'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'start_date': datetime(2021, 12, 13)
    
}

with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='30 16 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    """
    TEST
    """
    with TaskGroup('test') as test:

        fetch_ajua_info = PythonOperator(
            task_id = 'fetch_ajua_info',
            python_callable=fetch_ajua_info,
            provide_context=True
        )

    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> test >> finish
