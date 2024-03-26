from airflow.models import variable
import os
import sys
from datetime import datetime,timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from uganda.automations.stb_rab.report import uganda_daily_stb_delays,send_to_uganda_branches 

DAG_ID = 'UG_STB_RAB_Delays'

default_args = {
    'owner': 'Data Team',
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
    schedule_interval='00 02 * * 2,3,4,5,6,7',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    with TaskGroup('data') as data:
        uganda_daily_stb_delays = PythonOperator(
            task_id='uganda_daily_stb_delays',
            python_callable=uganda_daily_stb_delays,
            provide_context=True
        )

        send_to_uganda_branches = PythonOperator(
            task_id='send_to_uganda_branches',
            python_callable=send_to_uganda_branches,
            provide_context=True
        )    

        uganda_daily_stb_delays >> send_to_uganda_branches
    
    finish = DummyOperator(
            task_id = "finish"
            ) 
            
    start >> data >> finish      