import os
import sys

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

from sub_tasks.Ajua.ajua_info import (fetch_ajua_info,update_log_nps)
from sub_tasks.gsheets.ajuatodrop import (fetch_npsreviews_with_issues,npsreviews_with_issues_live)

# from tmp.python_test
DAG_ID = 'Ajua_ETL_Pipeline'


default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
    'start_date': datetime(2021, 12, 13),
    # 'email': ['ian.gathumbi@optica.africa','wairimu@optica.africa'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
}


with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='30 21 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    # start1 = DummyOperator(
    #     task_id = "start1"
    # )

    """
    TEST
    """
    with TaskGroup('test') as test:

        fetch_ajua_info = PythonOperator(
            task_id = 'fetch_ajua_info',
            python_callable=fetch_ajua_info,
            provide_context=True)

        # fetch_ajua_sheet = PythonOperator(
        #     task_id = 'fetch_ajua_sheet',
        #     python_callable=fetch_ajua_sheet,
        #     provide_context=True)    

        fetch_npsreviews_with_issues = PythonOperator(
            task_id = 'fetch_npsreviews_with_issues',
            python_callable=fetch_npsreviews_with_issues,
            provide_context=True) 

        npsreviews_with_issues_live = PythonOperator(
            task_id = 'npsreviews_with_issues_live',
            python_callable=npsreviews_with_issues_live,
            provide_context=True)  

        
        update_log_nps = PythonOperator(
            task_id = 'update_log_nps',
            python_callable=update_log_nps,
            provide_context=True)  

    fetch_ajua_info >> fetch_npsreviews_with_issues >> npsreviews_with_issues_live >> update_log_nps

    finish = DummyOperator(
        task_id = "finish"
    ) 

    # finish1 = DummyOperator(
    #     task_id = "finish1"
    # ) 


    start >>  test >> finish
