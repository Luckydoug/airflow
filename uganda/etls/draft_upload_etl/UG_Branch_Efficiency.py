from airflow.models import variable
import os
import sys
from datetime import datetime,timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

DAG_ID = 'UG_Branch_Efficiency'

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
    schedule_interval='50 02 * * *',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    with TaskGroup('report') as report:
        with TaskGroup('build') as build:
            from uganda.automations.draft_to_upload.branch_report import (
               push_uganda_efficiency_data,
               build_branches_efficiency
            )

            push_uganda_efficiency_data = PythonOperator(
                task_id='push_uganda_efficiency_data',
                python_callable=push_uganda_efficiency_data,
                provide_context=True
            )

            build_branches_efficiency = PythonOperator(
                task_id='build_branches_efficiency',
                python_callable=build_branches_efficiency,
                provide_context=True
            )

            push_uganda_efficiency_data >> build_branches_efficiency


    with TaskGroup('smtp') as smtp:
        with TaskGroup('send') as sends:
            from uganda.automations.draft_to_upload.branch_report import (
                trigger_efficiency_smtp,
                clean_uganda_folder
            )

            trigger_efficiency_smtp = PythonOperator(
                task_id='trigger_efficiency_smtp',
                python_callable=trigger_efficiency_smtp,
                provide_context=True
            )

            clean_uganda_folder = PythonOperator(
                task_id='clean_uganda_folder',
                python_callable=clean_uganda_folder,
                provide_context=True
            )

            trigger_efficiency_smtp >> clean_uganda_folder 

        build >> sends

    finish = DummyOperator(
        task_id="finish"
    )

    start >> report >> smtp >> finish

    """
    From Optica Data Team
    Unleash the Power of Automation
    
    """
