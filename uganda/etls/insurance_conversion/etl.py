from airflow.models import variable
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

DAG_ID = 'UG_Insurance_Conversion'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
    'start_date': datetime(2021, 12, 13),
    'email': ['ian.gathumbi@optica.africa','wairimu@optica.africa','douglas.kathurima@optica.africa'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    DAG_ID,
    default_args=default_args,
    tags=['Live'],
    schedule_interval='00 01 * * 3',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    with TaskGroup('report') as report:
        with TaskGroup('build') as build:
            from uganda.automations.insurance_conversion.report import (
            build_uganda_insurance_conversion
        )


        build_uganda_insurance_conversion = PythonOperator(
            task_id = 'build_uganda_insurance_conversion',
            python_callable= build_uganda_insurance_conversion,
            provide_context=True
        )
        
        build_uganda_insurance_conversion
    


    with TaskGroup('smtp') as smtp:
        with TaskGroup('send') as sends:
            from uganda.automations.insurance_conversion.report import (
            send_to_uganda_management,
            send_to_uganda_branches,
            clean_uganda_folder
        )

        send_to_uganda_management= PythonOperator(
            task_id = 'send_to_uganda_management',
            python_callable= send_to_uganda_management,
            provide_context=True
        )

        send_to_uganda_branches= PythonOperator(
            task_id = 'send_to_uganda_branches',
            python_callable= send_to_uganda_branches,
            provide_context=True
        )

        clean_uganda_folder = PythonOperator(
            task_id = 'clean_uganda_folder',
            python_callable=  clean_uganda_folder,
            provide_context=True
        )

      
        send_to_uganda_management >> send_to_uganda_branches >> clean_uganda_folder 


        build >> sends

    finish = DummyOperator(
        task_id="finish"
    )

    start >> report >> smtp >> finish