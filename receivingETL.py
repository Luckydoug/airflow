import os
import sys

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from datetime import datetime

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

from sub_tasks.receiving.testreceiving import create_receivingdata
from sub_tasks.receiving.testreceiving import receiving  
from sub_tasks.receiving.testreceiving import create_time_difference
# from sub_tasks.receiving.testreceiving import update_source_receiving_data
# from tmp.python_test
DAG_ID = 'Receiving_ETL_Pipeline'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'start_date': datetime(2021, 12, 13)
    
}

with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='00 10 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    """
    TEST
    """
    with TaskGroup('test') as test:
        
        create_receivingdata = PythonOperator(
            task_id = 'create_receivingdata',
            python_callable=create_receivingdata,
            provide_context=True
        )


        receiving = PythonOperator(
            task_id = 'receiving',
            python_callable=receiving,
            provide_context=True
        )

        create_time_difference = PythonOperator(
            task_id = 'create_time_difference',
            python_callable=create_time_difference,
            provide_context=True
        )

        # update_source_receiving_data = PythonOperator(
        #     task_id = 'update_source_receiving_data',
        #     python_callable=update_source_receiving_data,
        #     provide_context=True
        # )
        create_receivingdata >> receiving >> create_time_difference 

    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> test >> finish