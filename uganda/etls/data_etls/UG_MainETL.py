import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from uganda_sub_tasks.ordersETLs.orderscreendetailsc1 import (
    fetch_sap_orderscreendetailsc1, 
    update_to_source_orderscreenc1
)
from uganda_sub_tasks.ordersETLs.ordersscreendetails import (
    fetch_sap_orderscreendetails,
      update_to_source_orderscreen
)
from uganda_sub_tasks.ordersETLs.ajua_info import fetch_ajua_info
from uganda_sub_tasks.inventory_transfer.itr_logs import fetch_sap_itr_logs
from uganda_sub_tasks.inventory_transfer.transfer_request import fetch_sap_invt_transfer_request
from uganda_sub_tasks.inventory_transfer.transfer_details import fetch_sap_inventory_transfer
from uganda_sub_tasks.ordersETLs.purchaseorder import fetch_purchase_orders
from uganda_sub_tasks.ordersETLs.incentive_slab import fetch_sap_incentive_slab
from uganda_sub_tasks.ordersETLs.branch_targets import fetch_sap_branch_targets
from uganda_sub_tasks.ordersETLs.insurance import (fetch_sap_insurance)
from uganda_sub_tasks.ordersETLs.items import (fetch_sap_items, fetch_item_groups)
from uganda_sub_tasks.ordersETLs.optom_queue import fetch_optom_queue_mgmt
from uganda_sub_tasks.ordersETLs.order_checking_details import fetch_order_checking_details
from uganda_sub_tasks.ordersETLs.ojdt import fetch_sap_ojdt
from uganda_sub_tasks.ordersETLs.web_payments import fetch_sap_web_payments
from uganda_sub_tasks.ordersETLs.invoices import fetch_sap_invoices
from uganda_sub_tasks.ordersETLs.users import fetch_sap_users
from uganda_sub_tasks.ordersETLs.discounts import fetch_sap_discounts
from uganda_sub_tasks.ordersETLs.salesorders import (fetch_sap_orders, update_source_orders_line)
from uganda_sub_tasks.ordersETLs.prescriptions import fetch_prescriptions
from uganda_sub_tasks.ordersETLs.customers import fetch_sap_customers
from uganda_sub_tasks.ordersETLs.payments import fetch_sap_payments
from uganda_sub_tasks.gsheets.sop import (fetch_sop_branch_info,fetch_sop)
from uganda_sub_tasks.gsheets.ajuatodrop import fetch_npsreviews_with_issues
from uganda_sub_tasks.postgres.insurance_efficiency import update_approvals_efficiency
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.example_dags.subdags.subdag import subdag
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow import DAG
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))


DAG_ID = 'UG_Main_Pipeline'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
    'start_date': datetime(2021, 12, 13),
    'email': ['ian.gathumbi@optica.africa', 'wairimu@optica.africa', 'douglas.kathurima@optica.africa'],
    'email_on_failure': True,
    'email_on_retry': False,
}


with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval='30 18 * * *',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    with TaskGroup('orders') as orders:

        fetch_sap_orderscreendetails = PythonOperator(
            task_id='fetch_sap_orderscreendetails',
            python_callable=fetch_sap_orderscreendetails,
            provide_context=True
        )

        update_to_source_orderscreen = PythonOperator(
            task_id='update_to_source_orderscreen',
            python_callable=update_to_source_orderscreen,
            provide_context=True
        )

        fetch_sap_orderscreendetails >> update_to_source_orderscreen

    with TaskGroup('orderlog') as orderlog:

        fetch_sap_orderscreendetailsc1 = PythonOperator(
            task_id='fetch_sap_orderscreendetailsc1',
            python_callable=fetch_sap_orderscreendetailsc1,
            provide_context=True
        )

        update_to_source_orderscreenc1 = PythonOperator(
            task_id='update_to_source_orderscreenc1',
            python_callable=update_to_source_orderscreenc1,
            provide_context=True
        )

        fetch_sap_orderscreendetailsc1 >> update_to_source_orderscreenc1

    with TaskGroup('payments') as payments:

        fetch_sap_payments = PythonOperator(
            task_id='fetch_sap_payments',
            python_callable=fetch_sap_payments,
            provide_context=True
        )

        fetch_sap_web_payments = PythonOperator(
            task_id='fetch_sap_web_payments',
            python_callable=fetch_sap_web_payments,
            provide_context=True
        )
        fetch_sap_ojdt = PythonOperator(
            task_id='fetch_sap_ojdt',
            python_callable=fetch_sap_ojdt,
            provide_context=True
        )

        fetch_sap_invoices = PythonOperator(
            task_id='fetch_sap_invoices',
            python_callable=fetch_sap_invoices,
            provide_context=True
        )

        fetch_sap_payments >> fetch_sap_web_payments >> fetch_sap_ojdt >> fetch_sap_invoices

    with TaskGroup('customers') as customers:

        fetch_sap_customers = PythonOperator(
            task_id='fetch_sap_customers',
            python_callable=fetch_sap_customers,
            provide_context=True
        )

    with TaskGroup('prescriptions') as prescriptions:

        fetch_prescriptions = PythonOperator(
            task_id='fetch_prescriptions',
            python_callable=fetch_prescriptions,
            provide_context=True
        )

    with TaskGroup('view') as view:

        fetch_order_checking_details = PythonOperator(
            task_id='fetch_order_checking_details',
            python_callable=fetch_order_checking_details,
            provide_context=True
        )

        fetch_optom_queue_mgmt = PythonOperator(
            task_id='fetch_optom_queue_mgmt',
            python_callable=fetch_optom_queue_mgmt,
            provide_context=True
        )

        fetch_order_checking_details >> fetch_optom_queue_mgmt

    with TaskGroup('salesorders') as salesorders:

        fetch_sap_orders = PythonOperator(
            task_id='fetch_sap_orders',
            python_callable=fetch_sap_orders,
            provide_context=True
        )

        update_source_orders_line = PythonOperator(
            task_id='update_source_orders_line',
            python_callable=update_source_orders_line,
            provide_context=True
        )

        fetch_sap_orders >> update_source_orders_line

    with TaskGroup('discounts') as discounts:

        fetch_sap_discounts = PythonOperator(
            task_id='fetch_sap_discounts',
            python_callable=fetch_sap_discounts,
            provide_context=True
        )

    with TaskGroup('users') as users:

        fetch_sap_users = PythonOperator(
            task_id='fetch_sap_users',
            python_callable=fetch_sap_users,
            provide_context=True
        )

    with TaskGroup('items') as items:

        fetch_sap_items = PythonOperator(
            task_id='fetch_sap_items',
            python_callable=fetch_sap_items,
            provide_context=True
        )

        fetch_item_groups = PythonOperator(
            task_id='fetch_item_groups',
            python_callable=fetch_item_groups,
            provide_context=True
        )

        fetch_sap_items >> fetch_item_groups

    with TaskGroup('targets') as targets:

        fetch_sap_branch_targets = PythonOperator(
            task_id='fetch_sap_branch_targets',
            python_callable=fetch_sap_branch_targets,
            provide_context=True
        )

        fetch_sap_incentive_slab = PythonOperator(
            task_id='fetch_sap_incentive_slab',
            python_callable=fetch_sap_incentive_slab,
            provide_context=True
        )

        fetch_sap_branch_targets >> fetch_sap_incentive_slab

    with TaskGroup('insurance') as insurance:
        fetch_sap_insurance = PythonOperator(
            task_id='fetch_sap_insurance',
            python_callable=fetch_sap_insurance,
            provide_context=True
        )
        fetch_sap_insurance

    finish = DummyOperator(
        task_id="finish"
    )

    with TaskGroup('purchaseorders') as purchaseorders:

        fetch_purchase_orders = PythonOperator(
            task_id='fetch_purchase_orders',
            python_callable=fetch_purchase_orders,
            provide_context=True
        )

    with TaskGroup('itrlog') as itrlog:

        fetch_sap_itr_logs = PythonOperator(
            task_id='fetch_sap_itr_logs',
            python_callable=fetch_sap_itr_logs,
            provide_context=True
        )

    with TaskGroup('inventorytransferdetails') as inventorytransferdetails:

        fetch_sap_inventory_transfer = PythonOperator(
            task_id='fetch_sap_inventory_transfer',
            python_callable=fetch_sap_inventory_transfer,
            provide_context=True
        )

    with TaskGroup('itrdetails') as itrdetails:

        fetch_sap_invt_transfer_request = PythonOperator(
            task_id='fetch_sap_invt_transfer_request',
            python_callable=fetch_sap_invt_transfer_request,
            provide_context=True
        )

    with TaskGroup('nps_survey') as nps_survey:

        fetch_ajua_info = PythonOperator(
            task_id='fetch_ajua_info',
            python_callable=fetch_ajua_info,
            provide_context=True
        )

        fetch_npsreviews_with_issues = PythonOperator(
            task_id='fetch_npsreviews_with_issues',
            python_callable=fetch_npsreviews_with_issues,
            provide_context=True
        )

        fetch_ajua_info >> fetch_npsreviews_with_issues
    
    with TaskGroup('sop') as sop:

        fetch_sop_branch_info = PythonOperator(
            task_id='fetch_sop_branch_info',
            python_callable=fetch_sop_branch_info,
            provide_context=True
        )

        fetch_sop = PythonOperator(
            task_id='fetch_sop',
            python_callable=fetch_sop,
            provide_context=True
        )

        fetch_sop_branch_info >> fetch_sop

    update_approvals_efficiency = PythonOperator(
            task_id='update_approvals_efficiency',
            python_callable=update_approvals_efficiency,
            provide_context=True
        )

    start >> orders >> orderlog >> payments >> customers >> prescriptions >> view >> salesorders >> discounts >> users >> items >> insurance >> targets >> purchaseorders >> nps_survey >> itrlog >> inventorytransferdetails >> itrdetails >> sop >> update_approvals_efficiency >> finish
