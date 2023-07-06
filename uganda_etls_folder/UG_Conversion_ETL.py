from airflow.models import variable
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

DAG_ID = 'UG_CONVERSION_ETL'

default_args = {
    'owner': 'Data Team',
    'start_date': datetime(2021, 12, 13),
    'retries': 2,
    'retry_delay': timedelta(seconds=5)
}


with DAG(
    DAG_ID,
    default_args=default_args,
    tags=['Live'],
    schedule_interval='50 05 * * 1',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    with TaskGroup('report') as report:
        with TaskGroup('build') as build:
            from uganda_sub_tasks.conversion.weekly_report import (
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
            from uganda_sub_tasks.conversion.weekly_report_smtp import (
            send_management_report,
            send_branches_report,
            clean_registrations,
            clean_eyetests,
            clean_views
        )

        send_management_report = PythonOperator(
            task_id = 'send_management_report',
            python_callable= send_management_report,
            provide_context=True
        )

        send_branches_report = PythonOperator(
            task_id = 'send_branches_report',
            python_callable= send_branches_report,
            provide_context=True
        )

        clean_registrations = PythonOperator(
            task_id = 'clean_registrations',
            python_callable= clean_registrations,
            provide_context=True
        )

        clean_eyetests = PythonOperator(
            task_id = 'clean_eyetests',
            python_callable= clean_eyetests,
            provide_context=True
        )

        clean_views = PythonOperator(
            task_id = 'clean_views',
            python_callable= clean_views,
            provide_context=True
        )

        send_management_report >> send_branches_report >> clean_registrations >> clean_eyetests >> clean_views

        build >> sends

    finish = DummyOperator(
        task_id="finish"
    )

    start >> report >> smtp >> finish

    """
    From Optica Data Team
    Unleash the Power of Automation
    
    """
