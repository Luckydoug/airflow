from airflow.models import variable
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

DAG_ID = 'Uganda_Conversion_ETL'

default_args = {
    'owner': 'Iconia ETLs',
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
    schedule_interval='00 01 * * 1',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    with TaskGroup('report') as report:
        with TaskGroup('build') as build:
            from uganda.automations.conversion.report import (
            build_uganda_et_conversion,
            build_uganda_reg_conversion,
            build_uganda_viewrx_conversion
        )


        build_uganda_et_conversion = PythonOperator(
            task_id = 'build_uganda_et_conversion',
            python_callable= build_uganda_et_conversion,
            provide_context=True
        )

        build_uganda_reg_conversion = PythonOperator(
            task_id = 'build_uganda_reg_conversion',
            python_callable= build_uganda_reg_conversion,
            provide_context=True
        )

        build_uganda_viewrx_conversion = PythonOperator(
            task_id = 'build_uganda_viewrx_conversion',
            python_callable = build_uganda_viewrx_conversion,
            provide_context=True
        )

        
        build_uganda_et_conversion >> build_uganda_reg_conversion >> build_uganda_viewrx_conversion
    


    with TaskGroup('smtp') as smtp:
        with TaskGroup('send') as sends:
            from uganda.automations.conversion.report import (
            trigger_uganda_management_smtp,
            trigger_uganda_branches_smtp,
            clean_uganda_registrations,
            clean_uganda_eyetests,
            clean_uganda_views
        )

        trigger_uganda_management_smtp= PythonOperator(
            task_id = 'trigger_uganda_management_smtp',
            python_callable= trigger_uganda_management_smtp,
            provide_context=True
        )

        trigger_uganda_branches_smtp = PythonOperator(
            task_id = 'trigger_uganda_branches_smtp',
            python_callable= trigger_uganda_branches_smtp,
            provide_context=True
        )

        clean_uganda_registrations = PythonOperator(
            task_id = 'clean_uganda_registrations',
            python_callable= clean_uganda_registrations,
            provide_context=True
        )

        clean_uganda_eyetests = PythonOperator(
            task_id = 'clean_uganda_eyetests',
            python_callable= clean_uganda_eyetests,
            provide_context=True
        )

        clean_uganda_views = PythonOperator(
            task_id = 'clean_uganda_views',
            python_callable=   clean_uganda_views,
            provide_context=True
        )

        
        trigger_uganda_management_smtp >> trigger_uganda_branches_smtp >> clean_uganda_registrations >> clean_uganda_eyetests >> clean_uganda_views


        build >> sends

    finish = DummyOperator(
        task_id="finish"
    )

    start >> report >> smtp >> finish

    """
    From Optica Data Team
    Unleash the Power of Automation
    
    """
