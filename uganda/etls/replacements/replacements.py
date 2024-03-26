from airflow.models import variable
import os
import sys
from datetime import datetime,timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from uganda.automations.replacements.replacement_report import daily_replacements,send__replacements_to_uganda_branches


DAG_ID = 'Replacements_Not_Received_at_Branch'

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
    schedule_interval='30 02 * * 2,3,4,5,6,7',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    with TaskGroup('data') as data:
        daily_replacements = PythonOperator(
            task_id='daily_replacements',
            python_callable=daily_replacements,
            provide_context=True
        )

        send__replacements_to_uganda_branches = PythonOperator(
            task_id='send__replacements_to_uganda_branches',
            python_callable=send__replacements_to_uganda_branches,
            provide_context=True
        )      
        daily_replacements >> send__replacements_to_uganda_branches
    
    finish = DummyOperator(
            task_id = "finish"
            ) 
            
    start >> data >> finish      