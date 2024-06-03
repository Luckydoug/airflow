import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

DAG_ID = 'Zoho_Tickets_Updates'

default_args = {
    'owner': 'Data Team',
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
    schedule_interval='00 03 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    with TaskGroup('zoho_updates') as zoho_updates:
        from sub_tasks.zoho.update_tickets import (
            get_token,
            get_tickets_to_update,
            update_zoho_tickets,
            truncate_zoho_updates
        )

        get_token = PythonOperator(
            task_id = 'get_token',
            python_callable= get_token,
            provide_context=True
        )

        get_tickets_to_update = PythonOperator(
            task_id = 'get_tickets_to_update',
            python_callable= get_tickets_to_update,
            provide_context=True
        )

        update_zoho_tickets = PythonOperator(
            task_id = 'update_zoho_tickets',
            python_callable= update_zoho_tickets,
            provide_context=True
        )

        truncate_zoho_updates = PythonOperator(
            task_id = 'truncate_zoho_updates',
            python_callable= truncate_zoho_updates,
            provide_context=True
        )

        get_token >> get_tickets_to_update >> update_zoho_tickets >> truncate_zoho_updates

       

    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> zoho_updates >> finish