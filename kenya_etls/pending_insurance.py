from airflow.models import variable
import os
import sys
from datetime import datetime,timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

DAG_ID = 'PENDING_INSURANCE_ETL'

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
            from kenya_automation.pending_insurance.insurance import (
               build_pending_insurances
            )

            build_pending_insurances = PythonOperator(
                task_id='build_pending_insurances',
                python_callable=build_pending_insurances,
                provide_context=True
            )

            build_pending_insurances


    with TaskGroup('smtp') as smtp:
        with TaskGroup('send') as sends:
            from kenya_automation.pending_insurance.insurance import (
                send_to_branches
              
            )

            send_to_branches = PythonOperator(
                task_id='end_to_branches',
                python_callable=send_to_branches,
                provide_context=True
            )

            send_to_branches

        build >> sends

    finish = DummyOperator(
        task_id="finish"
    )

    start >> report >> smtp >> finish

    """
    From Optica Data Team
    Unleash the Power of Automation
    
    """