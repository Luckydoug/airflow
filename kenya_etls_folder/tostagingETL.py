import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.example_dags.subdags.subdag import subdag
from datetime import datetime
"""
from sub_tasks.dimensionsETLs.technicians import (fetch_sap_technicians)
from sub_tasks.dimensionsETLs.users import (fetch_sap_users)
from sub_tasks.dimensionsETLs.customers import (fetch_sap_customers)
"""
"""
from sub_tasks.gsheets.orders_issues import (fetch_orders_with_issues, update_orders_with_issues, orders_with_issues_live)
from sub_tasks.gsheets.time_issues import (fetch_time_with_issues, update_time_with_issues, time_with_issues_live)
from sub_tasks.gsheets.cutoff import (fetch_cutoffs, update_cutoffs, create_cutoffs_live)
from sub_tasks.ordersETLs.ordersscreendetails import (fetch_sap_orderscreendetails, update_to_source_orderscreen,
create_source_orderscreen_staging, create_fact_orderscreen)
from sub_tasks.ordersETLs.orderscreendetailsc1 import (fetch_sap_orderscreendetailsc1, update_to_source_orderscreenc1,
create_source_orderscreenc1_staging, create_source_orderscreenc1_staging2, create_source_orderscreenc1_staging2_1,
create_source_orderscreenc1_staging2_1, update_orderscreenc1_staging2_1, create_source_orderscreenc1_staging3,
index_staging3, create_fact_orderscreenc1, index_fact_c1)
from sub_tasks.ordersETLs.dropped_orders import (get_source_dropped_orders, get_source_dropped_orders_staging,
update_source_dropped_orders_staging, create_fact_dropped_orders)
from sub_tasks.runtimes.time import (get_start_time)
"""

# from tmp.python_test
DAG_ID = 'To_Staging_ETL'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'start_date': datetime(2021, 12, 13)
    
}

with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='00 5-15/2 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    
    """
    FETCH ORDERSCREEN   
    """
    """
    with TaskGroup('orderscreen_etls') as orderscreen_etls:
        
        with TaskGroup('orderscreen_details_etls') as orderscreen_details_etls:
            
            fetch_sap_orderscreendetails = PythonOperator(
                task_id = 'fetch_sap_orderscreendetails',
                python_callable=fetch_sap_orderscreendetails,
                provide_context=True
            )


            update_to_source_orderscreen = PythonOperator(
                task_id = 'update_to_source_orderscreen',
                python_callable=update_to_source_orderscreen,
                provide_context=True
            )
            
            fetch_sap_orderscreendetails >> update_to_source_orderscreen 

        with TaskGroup('orderscreenc1_etls') as orderscreenc1_etls:
            
            
            fetch_sap_orderscreendetailsc1 = PythonOperator(
                task_id = 'fetch_sap_orderscreendetailsc1',
                python_callable=fetch_sap_orderscreendetailsc1,
                provide_context=True
            )

            update_to_source_orderscreenc1 = PythonOperator(
                task_id = 'update_to_source_orderscreenc1',
                python_callable=update_to_source_orderscreenc1,
                provide_context=True
            )
            
            fetch_sap_orderscreendetailsc1 >> update_to_source_orderscreenc1

        orderscreen_details_etls >> orderscreenc1_etls
    """
    



    finish = DummyOperator(
        task_id = "finish"
    ) 

    #start >> orderscreen_etls >> finish
    #start >> start_time >> finish
    