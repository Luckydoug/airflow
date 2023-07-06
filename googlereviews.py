import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from airflow import DAG
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.task_group import TaskGroup
from datetime import datetime


from sub_tasks.googlereviews.locations import (fetch_locations, update_store_code)
from sub_tasks.googlereviews.reviews import (fetch_reviews,create_source_google_reviews)


# from tmp.python_test
DAG_ID = 'Google_Reviews_ETLs_Pipeline'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'start_date': datetime(2022, 2, 16)
    
}


with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='00 22 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    """
    LOCATIONS
    """
    
    with TaskGroup('locations') as locations:

        fetch_locations = PythonOperator(
            task_id = 'fetch_locations',
            python_callable=fetch_locations,
            provide_context=True
        )

        update_store_code = PythonOperator(
            task_id = 'update_store_code',
            python_callable=update_store_code,
            provide_context=True
        )
    
        fetch_locations >> update_store_code
    """
    REVIEWS
    """
    with TaskGroup('reviews') as reviews:

        fetch_reviews = PythonOperator(
            task_id = 'fetch_reviews',
            python_callable=fetch_reviews,
            provide_context=True
        )

        create_source_google_reviews = PythonOperator(
            task_id = 'create_source_google_reviews',
            python_callable=create_source_google_reviews,
            provide_context=True
        )

        fetch_reviews >> create_source_google_reviews
    
    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> locations >> reviews >> finish
    