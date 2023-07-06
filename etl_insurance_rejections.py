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
from airflow.models import Variable

from kenya_automation.insurance_rejections.chris_insurance_rejections import rejections

# from tmp.python_test
DAG_ID = 'Daily_Insurance_Rejections'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'start_date': datetime(2021, 12, 13)
    
}

with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='50 2 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    """
    UPDATE
    """
    with TaskGroup('daily_update') as daily_update:
        
        rejections = PythonOperator(
            task_id = 'rejections',
            python_callable=rejections,
            provide_context=True
        )
        rejections 

    finish = DummyOperator(
    task_id = "finish"
        )      
    
    start >>  daily_update >> finish