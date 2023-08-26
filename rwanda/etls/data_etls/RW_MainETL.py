import os
import sys

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from datetime import datetime
from datetime import date, timedelta
from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup


from rwanda_sub_tasks.ordersETLs.ordersscreendetails import (fetch_sap_orderscreendetails,update_to_source_orderscreen)
from rwanda_sub_tasks.ordersETLs.payments import fetch_sap_payments
from rwanda_sub_tasks.ordersETLs.customers import fetch_sap_customers
from rwanda_sub_tasks.ordersETLs.prescriptions import fetch_prescriptions
from rwanda_sub_tasks.ordersETLs.salesorders import (fetch_sap_orders, update_source_orders_line)
from rwanda_sub_tasks.ordersETLs.discounts import fetch_sap_discounts
from rwanda_sub_tasks.ordersETLs.users import fetch_sap_users
from rwanda_sub_tasks.ordersETLs.orderscreendetailsc1 import (fetch_sap_orderscreendetailsc1,update_to_source_orderscreenc1)
from rwanda_sub_tasks.ordersETLs.invoices import fetch_sap_invoices
from rwanda_sub_tasks.ordersETLs.web_payments import fetch_sap_web_payments
from rwanda_sub_tasks.ordersETLs.ojdt import fetch_sap_ojdt
from rwanda_sub_tasks.ordersETLs.order_checking_details import fetch_order_checking_details
from rwanda_sub_tasks.ordersETLs.optom_queue import fetch_optom_queue_mgmt
from rwanda_sub_tasks.ordersETLs.items import (fetch_sap_items, fetch_item_groups)
from rwanda_sub_tasks.ordersETLs.branch_targets import fetch_sap_branch_targets
from rwanda_sub_tasks.ordersETLs.incentive_slab import fetch_sap_incentive_slab
from rwanda_sub_tasks.ordersETLs.purchaseorder import fetch_purchase_orders

DAG_ID = 'RW_Main_Pipeline'

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
    schedule_interval='45 20 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    with TaskGroup('orders') as orders:

        fetch_sap_orderscreendetails = PythonOperator(
            task_id = 'fetch_sap_orderscreendetails',
            python_callable = fetch_sap_orderscreendetails,
            provide_context = True
        )

        update_to_source_orderscreen = PythonOperator(
            task_id = 'update_to_source_orderscreen',
            python_callable = update_to_source_orderscreen,
            provide_context = True
        )

        fetch_sap_orderscreendetails >> update_to_source_orderscreen 

    with TaskGroup('orderlog') as orderlog:

        fetch_sap_orderscreendetailsc1 = PythonOperator(
            task_id = 'fetch_sap_orderscreendetailsc1',
            python_callable = fetch_sap_orderscreendetailsc1,
            provide_context = True
        )

        update_to_source_orderscreenc1 = PythonOperator(
            task_id = 'update_to_source_orderscreenc1',
            python_callable = update_to_source_orderscreenc1,
            provide_context = True
        )

        fetch_sap_orderscreendetails >> update_to_source_orderscreen    

    with TaskGroup('payments') as payments:

        fetch_sap_payments = PythonOperator(
            task_id = 'fetch_sap_payments',
            python_callable = fetch_sap_payments,
            provide_context = True
        )
       
        fetch_sap_web_payments = PythonOperator(
            task_id = 'fetch_sap_web_payments',
            python_callable = fetch_sap_web_payments,
            provide_context = True
        )
        fetch_sap_ojdt = PythonOperator(
            task_id = 'fetch_sap_ojdt',
            python_callable = fetch_sap_ojdt,
            provide_context = True
        )

        fetch_sap_invoices = PythonOperator(
            task_id = 'fetch_sap_invoices',
            python_callable = fetch_sap_invoices,
            provide_context = True
        )       
        fetch_sap_payments >> fetch_sap_web_payments >> fetch_sap_ojdt >> fetch_sap_invoices

    with TaskGroup('customers') as customers:

        fetch_sap_customers = PythonOperator(
            task_id = 'fetch_sap_customers',
            python_callable = fetch_sap_customers,
            provide_context = True
        )
   

    with TaskGroup('prescriptions') as prescriptions:

        fetch_prescriptions = PythonOperator(
            task_id = 'fetch_prescriptions',
            python_callable = fetch_prescriptions,
            provide_context = True
        )


    with TaskGroup('view') as view:

        fetch_order_checking_details = PythonOperator(
            task_id = 'fetch_order_checking_details',
            python_callable = fetch_order_checking_details,
            provide_context = True
        )

        fetch_optom_queue_mgmt = PythonOperator(
            task_id = 'fetch_optom_queue_mgmt',
            python_callable = fetch_optom_queue_mgmt,
            provide_context = True
        )        
        
        fetch_order_checking_details >> fetch_optom_queue_mgmt
        

    with TaskGroup('salesorders') as salesorders:

        fetch_sap_orders = PythonOperator(
            task_id = 'fetch_sap_orders',
            python_callable = fetch_sap_orders,
            provide_context = True
        )  

        update_source_orders_line = PythonOperator(
            task_id = 'update_source_orders_line',
            python_callable = update_source_orders_line,
            provide_context = True
        )

        fetch_sap_orders >> update_source_orders_line

    with TaskGroup('discounts') as discounts:

        fetch_sap_discounts = PythonOperator(
            task_id = 'fetch_sap_discounts',
            python_callable = fetch_sap_discounts,
            provide_context = True
        )


    with TaskGroup('users') as users:
        fetch_sap_users = PythonOperator(
            task_id = 'fetch_sap_users',
            python_callable = fetch_sap_users,
            provide_context = True
        )  

    with TaskGroup('items') as items:
        fetch_sap_items = PythonOperator(
            task_id = 'fetch_sap_items',
            python_callable = fetch_sap_items,
            provide_context = True
        ) 

        fetch_item_groups = PythonOperator(
            task_id = 'fetch_item_groups',
            python_callable = fetch_item_groups,
            provide_context = True
        )

        fetch_sap_users >> fetch_sap_items >> fetch_item_groups

    with TaskGroup('targets') as targets:
        
        fetch_sap_branch_targets = PythonOperator(
            task_id = 'fetch_sap_branch_targets',
            python_callable = fetch_sap_branch_targets,
            provide_context = True
        ) 

        fetch_sap_incentive_slab = PythonOperator(
            task_id = 'fetch_sap_incentive_slab',
            python_callable = fetch_sap_incentive_slab,
            provide_context = True
        )

        fetch_sap_branch_targets >> fetch_sap_incentive_slab
    
    with TaskGroup('purchaseorders') as purchaseorders:

        fetch_purchase_orders = PythonOperator(
            task_id = 'fetch_purchase_orders',
            python_callable = fetch_purchase_orders,
            provide_context = True
        )
  
    finish = DummyOperator(
        task_id = "finish"
    )
    
    start >> orders >> orderlog >> payments >> customers >> prescriptions >> view >> salesorders >> discounts >> purchaseorders >> users >> items >> targets >> finish