from airflow.models import variable
import os
import sys
from datetime import datetime,timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

DAG_ID = 'UG_Queue_Time'

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
    # schedule_interval='50 02 * * *',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    with TaskGroup('report') as report:
        with TaskGroup('build') as build:
            from uganda.automations.queue_time.report import (
               build_uganda_queue_time
            )

            build_uganda_queue_time = PythonOperator(
                task_id='build_uganda_queue_time',
                python_callable=build_uganda_queue_time,
                provide_context=True
            )

            build_uganda_queue_time


    with TaskGroup('smtp') as smtp:
        with TaskGroup('send') as sends:
            from uganda.automations.queue_time.report import (
                trigger_branches_queue_smtp,
                trigger_management_smtp
            )

            trigger_branches_queue_smtp = PythonOperator(
                task_id='trigger_branches_queue_smtp',
                python_callable=trigger_branches_queue_smtp,
                provide_context=True
            )

            trigger_management_smtp = PythonOperator(
                task_id='trigger_management_smtp',
                python_callable= trigger_management_smtp,
                provide_context=True
            )


            trigger_management_smtp >> trigger_branches_queue_smtp

        build >> sends

    finish = DummyOperator(
        task_id="finish"
    )

    start >> report >> smtp >> finish

    """
    From Optica Data Team
    Unleash the Power of Automation
    
    """
