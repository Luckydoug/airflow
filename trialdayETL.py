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

from sub_tasks.daily_salereport.trialsmtp import send_email


# from sub_tasks.ticketing_data.refresh_cache import (cda_cache1)

# from tmp.python_test
DAG_ID = 'SMTPtrial_Pipeline'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'start_date': datetime(2021, 12, 13)
    
}

with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='00 6 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    """
    UPDATE
    """

    with TaskGroup('smtp') as smtp:

        send_email = PythonOperator(
            task_id = 'send_email',
            python_callable=send_email,
            provide_context=True
        )

        send_email

    # with TaskGroup('cache') as cache:

    #     cda_cache1 = PythonOperator(
    #         task_id = 'cda_cache1',
    #         python_callable=cda_cache1,
    #         provide_context=True
    #     )

    #     cda_cache1

    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> smtp >> finish