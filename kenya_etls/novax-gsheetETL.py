import os
import sys

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from datetime import datetime

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.example_dags.subdags.subdag import subdag 
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from sub_tasks.gsheets.novax import (fetch_novax_data,create_dim_novax_data,fetch_dhl_data,create_dim_dhl_data,create_dhl_with_orderscreen_data)

# from tmp.python_test
DAG_ID = 'NovaxGsheet_ETL_Pipeline'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'start_date': datetime(2021, 12, 13)
    
}

with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='00 23 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )


    """
    NOVAX & DHL
    """
    with TaskGroup('novax') as novax:

        fetch_novax_data = PythonOperator(
            task_id = 'fetch_novax_data',
            python_callable=fetch_novax_data,
            provide_context=True
            )

        fetch_dhl_data = PythonOperator(
            task_id = 'fetch_dhl_data',
            python_callable=fetch_dhl_data,
            provide_context=True
            )


        create_dim_novax_data = PythonOperator(
            task_id = 'create_dim_novax_data',
            python_callable=create_dim_novax_data,
            provide_context=True
            )
        
        create_dim_dhl_data = PythonOperator(
                task_id = 'create_dim_dhl_data',
                python_callable=create_dim_dhl_data,
                provide_context=True
            )

        create_dhl_with_orderscreen_data = PythonOperator(
            task_id = 'create_dhl_with_orderscreen_data',
            python_callable=create_dhl_with_orderscreen_data,
            provide_context=True
        )
        
        fetch_novax_data >> fetch_dhl_data >> create_dim_novax_data >> create_dim_dhl_data >> create_dhl_with_orderscreen_data

    finish = DummyOperator(
        task_id = "finish"
        ) 
        
    start >> novax >> finish        
            