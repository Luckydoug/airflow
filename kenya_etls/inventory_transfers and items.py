import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from airflow import DAG
from datetime import datetime,timedelta
from airflow.utils.task_group import TaskGroup
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator

from sub_tasks.inventory_transfer.transfer_request import (fetch_sap_invt_transfer_request)
from sub_tasks.inventory_transfer.transfer_details import (fetch_sap_inventory_transfer)
from sub_tasks.inventory_transfer.itr_logs import (fetch_sap_itr_logs)
from sub_tasks.dimensionsETLs.items import fetch_sap_items

# from tmp.python_test
DAG_ID = 'Inventory_Transfer_Items_Pipeline'

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
    schedule_interval='00 18 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    """
    GET INVENTORY TRANSFER DETAILS
    """
    
    with TaskGroup('itr') as itr:

        fetch_sap_invt_transfer_request = PythonOperator(
            task_id = 'fetch_sap_invt_transfer_request',
            python_callable=fetch_sap_invt_transfer_request,
            provide_context=True
        )    

        fetch_sap_invt_transfer_request 

    with TaskGroup('it') as it:

        fetch_sap_inventory_transfer = PythonOperator(
            task_id = 'fetch_sap_inventory_transfer',
            python_callable=fetch_sap_inventory_transfer,
            provide_context=True
        )
    
        fetch_sap_inventory_transfer

    with TaskGroup('itr_logs') as itr_logs:

        fetch_sap_itr_logs = PythonOperator(
            task_id = 'fetch_sap_itr_logs',
            python_callable=fetch_sap_itr_logs,
            provide_context=True
        )
        
        fetch_sap_itr_logs 
    

    with TaskGroup('items') as items:

            fetch_sap_items = PythonOperator(
                task_id = 'fetch_sap_items',
                python_callable=fetch_sap_items,
                provide_context=True
            )  

    fetch_sap_items

    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> itr >> it >> itr_logs  >> items >> finish
