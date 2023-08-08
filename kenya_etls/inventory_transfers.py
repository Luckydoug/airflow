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

from sub_tasks.inventory_transfer.transfer_request import (fetch_sap_invt_transfer_request, 
create_fact_itr_details, create_first_mviews_salesorders_with_item_whse, update_fact_itr_details, create_itr_sales_orders, create_mviews_fact_itr, create_mviews_fact_itr_2, 
create_mviews_fact_itr_3, create_mviews_fact_itr_4, update_null_branches, update_cutoff_status, create_mview_fact_itr3, 
update_itrs_issues, update_itrs_time_issues, 
create_fact_itr, create_branch_picklist_cutoffs, update_odd_statuses)
from sub_tasks.inventory_transfer.transfer_details import (fetch_sap_inventory_transfer)
from sub_tasks.inventory_transfer.itr_logs import (fetch_sap_itr_logs, create_mviews_source_itr_log,
create_mviews_salesorders_with_item_whse, create_mviews_itr_whse_details, create_mviews_itr_with_details, 
create_mviews_branchstock_itrlogs, update_drop_status, 
create_mviews_fact_branchstock_rep_itrlogs, create_mviews_stores_branchstockrep_logs, update_stores_dropstatus,
transpose_mviews_stores_branchstockrep_logs, create_mviews_fact_stores_branchstockrep_logs,
create_fact_stores_branchstockrep_logs,
create_mviews_control_branchstockrep_logs, update_control_dropstatus, transpose_mviews_control_branchstockrep_logs,
create_mviews_fact_control_branchstockrep_logs, create_fact_control_branchstockrep_logs,
create_mviews_packaging_branchstockrep_logs, update_packaging_dropstatus, transpose_mviews_packaging_branchstockrep_logs,
create_mviews_fact_packaging_branchstockrep_logs, create_fact_packaging_branchstockrep_logs,
create_mviews_all_itr_logs, update_branch_codes, create_mviews_all_itrs, create_mviews_itr_sourcestore, 
create_mviews_all_itrs_createdate,
create_mviews_all_itr_logs_2, update_all_dropstatus, transpose_mviews_all_itr_logs, update_missing_dates, 
update_missing_dates_dept,
create_mviews_fact_all_itr_logs, create_fact_all_itr_logs,update_fact_all_itr_logs_dropped, update_fact_all_itr_logs_dept,
create_all_branch_itr_logs, update_branch_itr_dropstatus, transpose_mviews_all_branch_itr_logs)

# from tmp.python_test
DAG_ID = 'Inventory_Transfer_Pipeline'

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
    schedule_interval='00 20 * * *',
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

        create_fact_itr_details = PythonOperator(
            task_id = 'create_fact_itr_details',
            python_callable=create_fact_itr_details,
            provide_context=True
        )

        create_first_mviews_salesorders_with_item_whse = PythonOperator(
            task_id = 'create_first_mviews_salesorders_with_item_whse',
            python_callable=create_first_mviews_salesorders_with_item_whse,
            provide_context=True
        )

        update_fact_itr_details = PythonOperator(
            task_id = 'update_fact_itr_details',
            python_callable=update_fact_itr_details,
            provide_context=True
        )

        create_itr_sales_orders = PythonOperator(
            task_id = 'create_itr_sales_orders',
            python_callable=create_itr_sales_orders,
            provide_context=True
        )

        create_mviews_fact_itr = PythonOperator(
            task_id = 'create_mviews_fact_itr',
            python_callable=create_mviews_fact_itr,
            provide_context=True
        )

        update_null_branches = PythonOperator(
            task_id = 'update_null_branches',
            python_callable=update_null_branches,
            provide_context=True
        )

        create_mviews_fact_itr_2 = PythonOperator(
            task_id = 'create_mviews_fact_itr_2',
            python_callable=create_mviews_fact_itr_2,
            provide_context=True
        )

        create_mviews_fact_itr_3 = PythonOperator(
            task_id = 'create_mviews_fact_itr_3',
            python_callable=create_mviews_fact_itr_3,
            provide_context=True
        )

        create_mviews_fact_itr_4 = PythonOperator(
            task_id = 'create_mviews_fact_itr_4',
            python_callable=create_mviews_fact_itr_4,
            provide_context=True
        )

        create_branch_picklist_cutoffs = PythonOperator(
            task_id = 'create_branch_picklist_cutoffs',
            python_callable=create_branch_picklist_cutoffs,
            provide_context=True
        )

        update_cutoff_status = PythonOperator(
            task_id = 'update_cutoff_status',
            python_callable=update_cutoff_status,
            provide_context=True
        )

        create_mview_fact_itr3 = PythonOperator(
            task_id = 'create_mview_fact_itr3',
            python_callable=create_mview_fact_itr3,
            provide_context=True
        )

        update_itrs_issues = PythonOperator(
            task_id = 'update_itrs_issues',
            python_callable=update_itrs_issues,
            provide_context=True
        )

        update_itrs_time_issues = PythonOperator(
            task_id = 'update_itrs_time_issues',
            python_callable=update_itrs_time_issues,
            provide_context=True
        )

        create_fact_itr = PythonOperator(
            task_id = 'create_fact_itr',
            python_callable=create_fact_itr,
            provide_context=True
        )

        fetch_sap_invt_transfer_request >> create_fact_itr_details >> create_first_mviews_salesorders_with_item_whse >> update_fact_itr_details >> create_itr_sales_orders >> create_mviews_fact_itr >> update_null_branches >> create_mviews_fact_itr_2 >> create_mviews_fact_itr_3 >> create_mviews_fact_itr_4 >> create_branch_picklist_cutoffs >> update_cutoff_status >> create_mview_fact_itr3 >> update_itrs_issues >> update_itrs_time_issues >> create_fact_itr

    with TaskGroup('it') as it:

        fetch_sap_inventory_transfer = PythonOperator(
            task_id = 'fetch_sap_inventory_transfer',
            python_callable=fetch_sap_inventory_transfer,
            provide_context=True
        )
    

    with TaskGroup('itr_logs') as itr_logs:

        fetch_sap_itr_logs = PythonOperator(
            task_id = 'fetch_sap_itr_logs',
            python_callable=fetch_sap_itr_logs,
            provide_context=True
        )

        create_mviews_source_itr_log = PythonOperator(
            task_id = 'create_mviews_source_itr_log',
            python_callable=create_mviews_source_itr_log,
            provide_context=True
        )

        create_mviews_branchstock_itrlogs = PythonOperator(
            task_id = 'create_mviews_branchstock_itrlogs',
            python_callable=create_mviews_branchstock_itrlogs,
            provide_context=True
        )

        update_drop_status = PythonOperator(
            task_id = 'update_drop_status',
            python_callable=update_drop_status,
            provide_context=True
        )

        create_mviews_salesorders_with_item_whse = PythonOperator(
            task_id = 'create_mviews_salesorders_with_item_whse',
            python_callable=create_mviews_salesorders_with_item_whse,
            provide_context=True
        )

        create_mviews_itr_whse_details = PythonOperator(
            task_id = 'create_mviews_itr_whse_details',
            python_callable=create_mviews_itr_whse_details,
            provide_context=True
        )

        create_mviews_itr_with_details = PythonOperator(
            task_id = 'create_mviews_itr_with_details',
            python_callable=create_mviews_itr_with_details,
            provide_context=True
        )

        create_mviews_fact_branchstock_rep_itrlogs = PythonOperator(
            task_id = 'create_mviews_fact_branchstock_rep_itrlogs',
            python_callable=create_mviews_fact_branchstock_rep_itrlogs,
            provide_context=True
        )

        fetch_sap_itr_logs >> create_mviews_source_itr_log >> create_mviews_branchstock_itrlogs >> update_drop_status >> create_mviews_salesorders_with_item_whse >> create_mviews_itr_whse_details >> create_mviews_itr_with_details >> create_mviews_fact_branchstock_rep_itrlogs #>> transpose_branchstock_rep_itrlogs
    
    with TaskGroup('stores') as stores:  
        
        create_mviews_stores_branchstockrep_logs = PythonOperator(
            task_id = 'create_mviews_stores_branchstockrep_logs',
            python_callable=create_mviews_stores_branchstockrep_logs,
            provide_context=True
        ) 

        update_stores_dropstatus = PythonOperator(
            task_id = 'update_stores_dropstatus',
            python_callable=update_stores_dropstatus,
            provide_context=True
        ) 

        transpose_mviews_stores_branchstockrep_logs = PythonOperator(
            task_id = 'transpose_mviews_stores_branchstockrep_logs',
            python_callable=transpose_mviews_stores_branchstockrep_logs,
            provide_context=True
        ) 

        create_mviews_fact_stores_branchstockrep_logs = PythonOperator(
            task_id = 'create_mviews_fact_stores_branchstockrep_logs',
            python_callable=create_mviews_fact_stores_branchstockrep_logs,
            provide_context=True
        )

        create_fact_stores_branchstockrep_logs = PythonOperator(
            task_id = 'create_fact_stores_branchstockrep_logs',
            python_callable=create_fact_stores_branchstockrep_logs,
            provide_context=True
        ) 

        create_mviews_stores_branchstockrep_logs >> update_stores_dropstatus >> transpose_mviews_stores_branchstockrep_logs >> create_mviews_fact_stores_branchstockrep_logs >> create_fact_stores_branchstockrep_logs

    with TaskGroup('control') as control:  
        
        create_mviews_control_branchstockrep_logs = PythonOperator(
            task_id = 'create_mviews_control_branchstockrep_logs',
            python_callable=create_mviews_control_branchstockrep_logs,
            provide_context=True
        )
        
        update_control_dropstatus = PythonOperator(
            task_id = 'update_control_dropstatus',
            python_callable=update_control_dropstatus,
            provide_context=True
        )

        transpose_mviews_control_branchstockrep_logs = PythonOperator(
            task_id = 'transpose_mviews_control_branchstockrep_logs',
            python_callable=transpose_mviews_control_branchstockrep_logs,
            provide_context=True
        )

        create_mviews_fact_control_branchstockrep_logs = PythonOperator(
            task_id = 'create_mviews_fact_control_branchstockrep_logs',
            python_callable=create_mviews_fact_control_branchstockrep_logs,
            provide_context=True
        )

        create_fact_control_branchstockrep_logs = PythonOperator(
            task_id = 'create_fact_control_branchstockrep_logs',
            python_callable=create_fact_control_branchstockrep_logs,
            provide_context=True
        )

        create_mviews_control_branchstockrep_logs >> update_control_dropstatus >> transpose_mviews_control_branchstockrep_logs >> create_mviews_fact_control_branchstockrep_logs >> create_fact_control_branchstockrep_logs
 
    with TaskGroup('packaging') as packaging:  
        
        create_mviews_packaging_branchstockrep_logs = PythonOperator(
            task_id = 'create_mviews_packaging_branchstockrep_logs',
            python_callable=create_mviews_packaging_branchstockrep_logs,
            provide_context=True
        )

        update_packaging_dropstatus = PythonOperator(
            task_id = 'update_packaging_dropstatus',
            python_callable=update_packaging_dropstatus,
            provide_context=True
        )

        transpose_mviews_packaging_branchstockrep_logs = PythonOperator(
            task_id = 'transpose_mviews_packaging_branchstockrep_logs',
            python_callable=transpose_mviews_packaging_branchstockrep_logs,
            provide_context=True
        )

        create_mviews_fact_packaging_branchstockrep_logs = PythonOperator(
            task_id = 'create_mviews_fact_packaging_branchstockrep_logs',
            python_callable=create_mviews_fact_packaging_branchstockrep_logs,
            provide_context=True
        )

        create_fact_packaging_branchstockrep_logs = PythonOperator(
            task_id = 'create_fact_packaging_branchstockrep_logs',
            python_callable=create_fact_packaging_branchstockrep_logs,
            provide_context=True
        )

        create_mviews_packaging_branchstockrep_logs >> update_packaging_dropstatus >> transpose_mviews_packaging_branchstockrep_logs >> create_mviews_fact_packaging_branchstockrep_logs >> create_fact_packaging_branchstockrep_logs

    with TaskGroup('all_dept') as all_dept:  
        
        create_mviews_all_itr_logs = PythonOperator(
            task_id = 'create_mviews_all_itr_logs',
            python_callable=create_mviews_all_itr_logs,
            provide_context=True
        ) 

        update_branch_codes = PythonOperator(
            task_id = 'update_branch_codes',
            python_callable=update_branch_codes,
            provide_context=True
        )  

        create_mviews_all_itrs = PythonOperator(
            task_id = 'create_mviews_all_itrs',
            python_callable=create_mviews_all_itrs,
            provide_context=True
        ) 

        create_mviews_all_itrs_createdate = PythonOperator(
            task_id = 'create_mviews_all_itrs_createdate',
            python_callable=create_mviews_all_itrs_createdate,
            provide_context=True
        )

        create_mviews_all_itr_logs_2 = PythonOperator(
            task_id = 'create_mviews_all_itr_logs_2',
            python_callable=create_mviews_all_itr_logs_2,
            provide_context=True
        )

        create_mviews_itr_sourcestore = PythonOperator(
            task_id = 'create_mviews_itr_sourcestore',
            python_callable=create_mviews_itr_sourcestore,
            provide_context=True
        ) 

        update_all_dropstatus = PythonOperator(
            task_id = 'update_all_dropstatus',
            python_callable=update_all_dropstatus,
            provide_context=True
        )

        transpose_mviews_all_itr_logs = PythonOperator(
            task_id = 'transpose_mviews_all_itr_logs',
            python_callable=transpose_mviews_all_itr_logs,
            provide_context=True
        ) 

        update_missing_dates = PythonOperator(
            task_id = 'update_missing_dates',
            python_callable=update_missing_dates,
            provide_context=True
        ) 

        update_missing_dates_dept = PythonOperator(
            task_id = 'update_missing_dates_dept',
            python_callable=update_missing_dates_dept,
            provide_context=True
        ) 

        create_mviews_fact_all_itr_logs = PythonOperator(
            task_id = 'create_mviews_fact_all_itr_logs',
            python_callable=create_mviews_fact_all_itr_logs,
            provide_context=True
        )

        create_fact_all_itr_logs = PythonOperator(
            task_id = 'create_fact_all_itr_logs',
            python_callable=create_fact_all_itr_logs,
            provide_context=True
        )

        update_fact_all_itr_logs_dropped = PythonOperator(
            task_id = 'update_fact_all_itr_logs_dropped',
            python_callable=update_fact_all_itr_logs_dropped,
            provide_context=True
        )

        update_fact_all_itr_logs_dept = PythonOperator(
            task_id = 'update_fact_all_itr_logs_dept',
            python_callable=update_fact_all_itr_logs_dept,
            provide_context=True
        )


        create_mviews_all_itr_logs >> update_branch_codes >> create_mviews_all_itrs >> create_mviews_all_itrs_createdate >> create_mviews_all_itr_logs_2 >> create_mviews_itr_sourcestore >> update_all_dropstatus >> transpose_mviews_all_itr_logs >> update_missing_dates >> update_missing_dates_dept >> create_mviews_fact_all_itr_logs >> create_fact_all_itr_logs >> update_fact_all_itr_logs_dropped >> update_fact_all_itr_logs_dept

    finish = DummyOperator(
        task_id = "finish"
    ) 

    #start >> itr >> it >> itr_logs >> finish
    start >> itr >> it >> itr_logs >> [stores, control, packaging] >> all_dept >> finish
    #start >> stores >> finish