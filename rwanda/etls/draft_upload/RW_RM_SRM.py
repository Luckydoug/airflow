from airflow.models import variable
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

DAG_ID = 'RW_RM_SRM_ETL'

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
    schedule_interval='40 05 * * *',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    with TaskGroup('report') as report:
        with TaskGroup('build') as build:
            from rwanda.automations.draft_upload.report import (
                push_rwanda_efficiency_data,
                build_rw_draft_upload,
                build_rw_rejections,
                build_rw_sops,
                build_plano_report,
                build_rwanda_opening_time,
                build_direct_conversion,
                build_eyetest_order,
                build_efficiency_before_feedback,
                build_rwanda_insurance_conversion
            )

            push_rwanda_efficiency_data = PythonOperator(
                task_id='push_rwanda_efficiency_data',
                python_callable=push_rwanda_efficiency_data,
                provide_context=True
            )

            build_rw_draft_upload = PythonOperator(
                task_id='build_rw_draft_upload',
                python_callable=build_rw_draft_upload,
                provide_context=True
            )

            build_rw_rejections = PythonOperator(
                task_id='build_rw_rejections',
                python_callable=build_rw_rejections,
                provide_context=True
            )

            build_rw_sops = PythonOperator(
                task_id='build_rw_sops',
                python_callable=build_rw_sops,
                provide_context=True
            )

            build_plano_report = PythonOperator(
                task_id='build_plano_report',
                python_callable=build_plano_report,
                provide_context=True
            )


            build_rwanda_opening_time = PythonOperator(
                task_id='build_rwanda_opening_time',
                python_callable=build_rwanda_opening_time,
                provide_context=True
            )


            build_rwanda_insurance_conversion = PythonOperator(
                task_id='build_rwanda_insurance_conversion',
                python_callable=build_rwanda_insurance_conversion,
                provide_context=True
            )


            build_direct_conversion = PythonOperator(
                task_id='build_direct_conversion',
                python_callable=build_direct_conversion,
                provide_context=True
            )

            build_eyetest_order = PythonOperator(
                task_id='build_eyetest_order',
                python_callable=build_eyetest_order,
                provide_context=True
            )

            build_efficiency_before_feedback = PythonOperator(
                task_id='build_efficiency_before_feedback',
                python_callable=build_efficiency_before_feedback,
                provide_context=True
            )

            
            push_rwanda_efficiency_data >> build_rw_draft_upload >> build_rw_sops >> build_rw_rejections >> build_plano_report >> build_rwanda_opening_time >> build_rwanda_insurance_conversion >> build_direct_conversion >> build_eyetest_order >> build_efficiency_before_feedback
            

    with TaskGroup('smtp') as smtp: 
        with TaskGroup('send') as sends:
            from rwanda.automations.draft_upload.report import (
                trigger_rwanda_smtp,
                trigger_rwanda_branches_smtp,
                clean_rwanda_folder
            )

            trigger_rwanda_smtp = PythonOperator(
                task_id='trigger_rwanda_smtp',
                python_callable=trigger_rwanda_smtp,
                provide_context=True
            )

            trigger_rwanda_branches_smtp = PythonOperator(
                task_id='trigger_rwanda_branches_smtp',
                python_callable=trigger_rwanda_branches_smtp,
                provide_context=True
            )

            clean_rwanda_folder = PythonOperator(
                task_id='clean_rwanda_folder',
                python_callable=clean_rwanda_folder,
                provide_context=True,
                trigger_rule = 'all_done'
            )

            trigger_rwanda_smtp >> trigger_rwanda_branches_smtp >> clean_rwanda_folder

        build >> sends

    finish = DummyOperator(
        task_id="finish"
    )

    start >> report >> smtp >> finish

    """
    From Optica Data Team
    Unleash the Power of Automation
    
    """
