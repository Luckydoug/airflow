import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

DAG_ID = 'Zoho_ETL_Pipeline'

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

    with TaskGroup('fetch_closed_tickets') as fetch_closed_tickets:
        from sub_tasks.zoho.closed_tickets import (fetch_closed_tickets)

        fetch_closed_tickets = PythonOperator(
            task_id = 'fetch_closed_tickets',
            python_callable= fetch_closed_tickets,
            provide_context=True
        )

    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> fetch_closed_tickets >> finish