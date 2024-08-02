from airflow.models import variable
import os
import sys
from datetime import datetime,timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from rwanda.automations.non_conversion_remarks.et_non_conversion import (smtp,clean_folder)

DAG_ID = 'ET_Non_converstions_RW_ETL'

default_args = {
    'owner': 'Data Team',
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
    schedule_interval='50 02 * * 1,2,3,4,5,6,7',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    """
    UPDATE
    """

    # with TaskGroup('manipulate') as manipulate:
        
    #     manipulate_et_non_conversions = PythonOperator(
    #         task_id = 'manipulate_et_non_conversions',
    #         python_callable=manipulate_et_non_conversions,
    #         provide_context=True
    #     )
    #     manipulate_et_non_conversions

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

    start >> send_smtp >> clean_excels >> finish