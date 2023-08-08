import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from airflow import DAG
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta


from uganda_sub_tasks.uganda_reports_automation.google_reviews.googlereviews import (google_reviews_automation)


# from tmp.python_test
DAG_ID = 'UG_Google_Reviews_Report_Automation'

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
    schedule_interval='15 5 * * 1',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    """
    GOOGLE REVIEWS AUTOMATION
    """
    
    with TaskGroup('google_reviews') as google_reviews:

        google_reviews_automation = PythonOperator(
            task_id = 'google_reviews_automation',
            python_callable=google_reviews_automation,
            provide_context=True
        )

        google_reviews_automation

    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> google_reviews >> finish
    