from airflow.models import variable
import os
import sys
from datetime import datetime,timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from kenya_automation.non_conversion_remarks.et_non_conversion import (manipulate_et_non_conversions,smtp,clean_folder)
# from tmp.python_test
DAG_ID = 'ET_Non_converstions_KE_ETL'

default_args = {
    'owner': 'Data Team',
    'start_date': datetime.now().date().strftime('%Y-%m-%d'),
     'retries': 2,
    'retry_delay': timedelta(seconds=30)
}


with DAG(
    DAG_ID,
    default_args=default_args,
    tags=['Live'],
    schedule_interval='30 02 * * 1,2,3,4,5,6',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    """
    UPDATE
    """

    with TaskGroup('manipulate') as manipulate:
        
        manipulate_et_non_conversions = PythonOperator(
            task_id = 'manipulate_et_non_conversions',
            python_callable=manipulate_et_non_conversions,
            provide_context=True
        )
        manipulate_et_non_conversions

    with TaskGroup('send_smtp') as send_smtp:
        
        smtp = PythonOperator(
            task_id = 'smtp',
            python_callable=smtp,
            provide_context=True
        )
        smtp

    with TaskGroup('clean_excels') as clean_excels:
        
        clean_folder = PythonOperator(
            task_id = 'clean_folder',
            python_callable=clean_folder,
            provide_context=True
        )
        clean_folder

    finish = DummyOperator(
        task_id="finish"
    )

    start >> manipulate >> send_smtp >> clean_excels >> finish