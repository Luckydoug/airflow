from airflow.models import variable
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

DAG_ID = 'RW_Conversion_ETL'

default_args = {
    'owner': 'Data Team',
    'start_date': datetime(2021, 12, 13),
    'retries': 2,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    DAG_ID,
    default_args=default_args,
    tags=['Live'],
    schedule_interval='00 01 * * 2',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    with TaskGroup('report') as report:
        with TaskGroup('build') as build:
            from rwanda.automations.conversion.report import (
            build_rwanda_et_conversion,
            build_rwanda_reg_conversion,
            build_rwanda_viewrx_conversion
        )


        build_rwanda_et_conversion = PythonOperator(
            task_id = 'build_rwanda_et_conversion',
            python_callable= build_rwanda_et_conversion,
            provide_context=True
        )

        build_rwanda_reg_conversion = PythonOperator(
            task_id = 'build_rwanda_reg_conversion',
            python_callable= build_rwanda_reg_conversion,
            provide_context=True
        )

        build_rwanda_viewrx_conversion = PythonOperator(
            task_id = 'build_rwanda_viewrx_conversion',
            python_callable = build_rwanda_viewrx_conversion,
            provide_context=True
        )

        
        build_rwanda_et_conversion >> build_rwanda_reg_conversion >> build_rwanda_viewrx_conversion
    


    with TaskGroup('smtp') as smtp:
        with TaskGroup('send') as sends:
            from rwanda.automations.conversion.report import (
            trigger_rwanda_management_smtp,
            trigger_rwanda_branches_smtp,
            clean_rwanda_registrations,
            clean_rwanda_eyetests,
            clean_rwanda_views
        )

        trigger_rwanda_management_smtp= PythonOperator(
            task_id = 'trigger_rwanda_management_smtp',
            python_callable= trigger_rwanda_management_smtp,
            provide_context=True
        )

        trigger_rwanda_branches_smtp = PythonOperator(
            task_id = 'trigger_rwanda_branches_smtp',
            python_callable= trigger_rwanda_branches_smtp,
            provide_context=True
        )

        clean_rwanda_registrations = PythonOperator(
            task_id = 'clean_rwanda_registrations',
            python_callable= clean_rwanda_registrations,
            provide_context=True
        )

        clean_rwanda_eyetests = PythonOperator(
            task_id = 'clean_rwanda_eyetests',
            python_callable= clean_rwanda_eyetests,
            provide_context=True
        )

        clean_rwanda_views = PythonOperator(
            task_id = 'clean_rwanda_views',
            python_callable=   clean_rwanda_views,
            provide_context=True
        )

        
        trigger_rwanda_management_smtp >> trigger_rwanda_branches_smtp >> clean_rwanda_registrations >> clean_rwanda_eyetests >> clean_rwanda_views


        build >> sends

    finish = DummyOperator(
        task_id="finish"
    )

    start >> report >> smtp >> finish

    """
    From Optica Data Team
    Unleash the Power of Automation
    
    """
