from airflow.models import variable
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

DAG_ID = 'UG_SRM_RM_ETL'

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
    schedule_interval='30 05 * * *',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    with TaskGroup('report') as report:
        with TaskGroup('build') as build:
            from uganda.automations.draft_to_upload.report import (
                push_uganda_efficiency_data,
                build_uganda_ratings_report,
                build_ug_draft_upload,
                build_ug_rejections,
                build_ug_sops,
                build_plano_report,
                build_uganda_opening_time,
                build_uganda_insurance_conversion

            )

            push_uganda_efficiency_data = PythonOperator(
                task_id='push_uganda_efficiency_data',
                python_callable=push_uganda_efficiency_data,
                provide_context=True
            )

            build_ug_draft_upload = PythonOperator(
                task_id='build_ug_draft_upload',
                python_callable=build_ug_draft_upload,
                provide_context=True
            )

            build_ug_rejections = PythonOperator(
                task_id='build_ug_rejections',
                python_callable=build_ug_rejections,
                provide_context=True
            )

            build_ug_sops = PythonOperator(
                task_id='build_ug_sops',
                python_callable=build_ug_sops,
                provide_context=True
            )

            build_uganda_ratings_report = PythonOperator(
                task_id='build_uganda_ratings_report',
                python_callable=build_uganda_ratings_report,
                provide_context=True
            )

            build_plano_report = PythonOperator(
                task_id='build_plano_report',
                python_callable=build_plano_report,
                provide_context=True
            )


            build_uganda_opening_time = PythonOperator(
                task_id='build_uganda_opening_time',
                python_callable=build_uganda_opening_time,
                provide_context=True
            )

            build_uganda_insurance_conversion = PythonOperator(
                task_id='build_uganda_insurance_conversion',
                python_callable=build_uganda_insurance_conversion,
                provide_context=True
            )

            push_uganda_efficiency_data >> build_ug_draft_upload >> build_ug_sops >> build_uganda_ratings_report >> build_ug_rejections >> build_plano_report >>  build_uganda_opening_time >> build_uganda_insurance_conversion

    with TaskGroup('smtp') as smtp:
        with TaskGroup('send') as sends:
            from uganda.automations.draft_to_upload.report import (
                trigger_uganda_smtp,
                trigger_uganda_branches_smtp,
                clean_uganda_folder
            )

            trigger_uganda_smtp = PythonOperator(
                task_id='trigger_uganda_smtp',
                python_callable=trigger_uganda_smtp,
                provide_context=True
            )

            trigger_uganda_branches_smtp = PythonOperator(
                task_id='trigger_uganda_branches_smtp',
                python_callable=trigger_uganda_branches_smtp,
                provide_context=True
            )

            clean_uganda_folder = PythonOperator(
                task_id='clean_uganda_folder',
                python_callable=clean_uganda_folder,
                provide_context=True,
                trigger_rule = 'all_done'
            )

            trigger_uganda_smtp >> trigger_uganda_branches_smtp >> clean_uganda_folder

        build >> sends

    finish = DummyOperator(
        task_id="finish"
    )

    start >> report >> smtp >> finish

    """
    From Optica Data Team
    Unleash the Power of Automation
    
    """
