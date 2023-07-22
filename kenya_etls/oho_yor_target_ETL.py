import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))


DAG_ID = 'Optica_York_House_TargetsETL'

default_args = {
    'owner': 'Data Team',
    'start_date': datetime(2021, 12, 13),
    'retries': 3,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='30 04 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    with TaskGroup('incentives') as incentives:
        with TaskGroup('fetch') as fetches:
            from sub_tasks.oho_yor_targets.daily_report import (
                build_incentives
            )

            build_incentives = PythonOperator(
                task_id = 'build_incentives',
                python_callable= build_incentives,
                provide_context=True
            )

            build_incentives

    with TaskGroup('smtp') as smtp:
        with TaskGroup('send') as sends:
            from sub_tasks.oho_yor_targets.targets_smtp import (
                send_to_salespersons,
                send_branch_version,
                clean_folder
            )

            send_to_salespersons = PythonOperator(
                task_id = 'send_to_salespersons',
                python_callable= send_to_salespersons,
                provide_context=True
            )

            send_branch_version = PythonOperator(
                task_id = 'send_branch_version',
                python_callable= send_branch_version,
                provide_context=True
            )

            clean_folder = PythonOperator(
                task_id = 'clean_folder',
                python_callable= clean_folder,
                provide_context=True
            )

            send_to_salespersons >> send_branch_version >> clean_folder

        incentives >> smtp

    finish = DummyOperator(
        task_id = "finish"
    )

    start >> incentives >> smtp >> finish