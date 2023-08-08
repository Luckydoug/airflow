import os
import sys

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from datetime import datetime,timedelta

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
from sub_tasks.gsheets.riders import (fetch_rider_times)
from sub_tasks.gsheets.routes import (fetch_routesdata)
# from sub_tasks.receiving.testreceiving import update_source_receiving_data
# from tmp.python_test
DAG_ID = 'Receiving_ETL_Pipeline'

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
    schedule_interval='00 10 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    """
    RIDERS TIMINGS
    """
    with TaskGroup('riders') as riders:

        fetch_rider_times = PythonOperator(
            task_id = 'fetch_rider_times',
            python_callable=fetch_rider_times,
            provide_context=True
        )
        fetch_rider_times
    """
    ROUTES
    """
    with TaskGroup('routes') as routes:

        fetch_routesdata = PythonOperator(
            task_id = 'fetch_routesdata',
            python_callable=fetch_routesdata,
            provide_context=True
        )
        fetch_routesdata
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

    start >> riders >> routes >> test >> finish