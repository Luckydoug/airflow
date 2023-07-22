from airflow.models import variable
import os
import sys
from datetime import datetime,timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from kenya_automation.delayed_and_pending_ITRs.delayed_and_pending_ITRs import (manipulate_delayed_and_pending_ITRs,smtp)
from sub_tasks.inventory_transfer.itr_logs import (fetch_sap_itr_logs)
from sub_tasks.inventory_transfer.transfer_request import (fetch_sap_invt_transfer_request)
from sub_tasks.inventory_transfer.transfer_details import (fetch_sap_inventory_transfer)
# from tmp.python_test
DAG_ID = 'Delayed_And_Pending_ITRs_ETL_V2'

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
    schedule_interval='30 10,14 * * 1,2,3,4,5,6',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    """
    UPDATE
    """

    with TaskGroup('fetch_itrs') as fetch_itrs:
        
        fetch_sap_invt_transfer_request = PythonOperator(
            task_id = 'fetch_sap_invt_transfer_request',
            python_callable=fetch_sap_invt_transfer_request,
            provide_context=True
        )
        fetch_sap_invt_transfer_request


    with TaskGroup('fetch_itr_details') as fetch_itr_details:
        
        fetch_sap_inventory_transfer = PythonOperator(
            task_id = 'fetch_sap_inventory_transfer',
            python_callable=fetch_sap_inventory_transfer,
            provide_context=True
        )
        fetch_sap_inventory_transfer


    with TaskGroup('fetch_itr_logs') as fetch_itr_logs:
        
        fetch_sap_itr_logs = PythonOperator(
            task_id = 'fetch_sap_itr_logs',
            python_callable=fetch_sap_itr_logs,
            provide_context=True
        )
        fetch_sap_itr_logs


    with TaskGroup('manipulate') as manipulate:
        
        manipulate_delayed_and_pending_ITRs = PythonOperator(
            task_id = 'manipulate_delayed_and_pending_ITRs',
            python_callable=manipulate_delayed_and_pending_ITRs,
            provide_context=True
        )
        manipulate_delayed_and_pending_ITRs


    with TaskGroup('send_smtp') as send_smtp:
        
        smtp = PythonOperator(
            task_id = 'smtp',
            python_callable=smtp,
            provide_context=True
        )
        smtp

    finish = DummyOperator(
        task_id="finish"
    )

    start >> fetch_itrs >> fetch_itr_details >> fetch_itr_logs >> manipulate >> send_smtp >> finish