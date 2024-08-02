import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

DAG_ID = 'MPESA_STATEMENTS_ETL'

default_args = {
    'owner': 'Data Team',
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
    'start_date': datetime(2021, 12, 13),
    'email': [
        'ian.gathumbi@optica.africa',
        'wairimu@optica.africa',
        'douglas.kathurima@optica.africa'
    ],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='00 03 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    with TaskGroup('zoho_updates') as transactions:
        from sub_tasks.mpesa_statements.statements import (
            download_transactions,
            upsert_to_source_mpesa_transactions,
            cleann_folder,
            truncate_email_message
        )

        download_transactions = PythonOperator(
            task_id = 'download_transactions',
            python_callable= download_transactions,
            provide_context=True
        )

        upsert_to_source_mpesa_transactions = PythonOperator(
            task_id = 'upsert_to_source_mpesa_transactions',
            python_callable= upsert_to_source_mpesa_transactions,
            provide_context=True
        )

        cleann_folder = PythonOperator(
            task_id = 'cleann_folder',
            python_callable= cleann_folder,
            provide_context=True
        )

        truncate_email_message = PythonOperator(
            task_id = 'truncate_email_message',
            python_callable= truncate_email_message,
            provide_context=True
        )

        download_transactions >> upsert_to_source_mpesa_transactions >> cleann_folder >> truncate_email_message

       

    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> transactions >> finish