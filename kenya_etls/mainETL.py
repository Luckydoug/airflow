import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
# from airflow.example_dags.subdags.subdag import subdag
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator

# from tmp.python_test
DAG_ID = 'Main_ETLs_Pipeline'

default_args = {
    'owner': 'Data Team',
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
    schedule_interval='00 17 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    """
    GET DIMENSIONS
    """
    with TaskGroup('dimensions') as dimensions:

        """
        GET ITEMS
        """
        with TaskGroup('items') as items:

            from sub_tasks.dimensionsETLs.items import(fetch_sap_items, fetch_item_groups, create_items_live)

            fetch_sap_items = PythonOperator(
                task_id = 'fetch_sap_items',
                python_callable=fetch_sap_items,
                provide_context=True
            )

            fetch_item_groups = PythonOperator(
                task_id = 'fetch_item_groups',
                python_callable=fetch_item_groups,
                provide_context=True
            )

            create_items_live = PythonOperator(
                task_id = 'create_items_live',
                python_callable=create_items_live,
                provide_context=True
            )

            fetch_sap_items >> fetch_item_groups >> create_items_live

        """
        GET USERS
        """
        with TaskGroup('users') as users:

            from sub_tasks.dimensionsETLs.users import(fetch_sap_users, create_dim_users)

            fetch_sap_users = PythonOperator(
                task_id = 'fetch_sap_users',
                python_callable=fetch_sap_users,
                provide_context=True
            )

            create_dim_users = PythonOperator(
                task_id = 'create_dim_users',
                python_callable=create_dim_users,
                provide_context=True
            )

            fetch_sap_users >> create_dim_users

        """
        GET WAREHOUSES
        """
        with TaskGroup('warehouse') as warehouse:

            from sub_tasks.dimensionsETLs.warehouses import(fetch_sap_warehouses)

            fetch_sap_warehouses = PythonOperator(
                task_id = 'fetch_sap_warehouses',
                python_callable=fetch_sap_warehouses,
                provide_context=True
            )

        """
        GET INSURANCE
        """
        with TaskGroup('insurances') as insurances:

            from sub_tasks.dimensionsETLs.insurance import(fetch_sap_insurance)

            fetch_sap_insurance = PythonOperator(
                task_id = 'fetch_sap_insurance',
                python_callable=fetch_sap_insurance,
                provide_context=True
            )

        """
        GET CUSTOMERS
        """
        with TaskGroup('customers') as customers:

            from sub_tasks.dimensionsETLs.customers import(fetch_sap_customers, create_dim_customers)

            fetch_sap_customers = PythonOperator(
                task_id = 'fetch_sap_customers',
                python_callable=fetch_sap_customers,
                provide_context=True
            )

            create_dim_customers = PythonOperator(
                task_id = 'create_dim_customers',
                python_callable=create_dim_customers,
                provide_context=True
            )

            fetch_sap_customers >> create_dim_customers

        """
        GET WHSE HOURS
        """
        with TaskGroup('working_hours') as working_hours:

            from sub_tasks.dimensionsETLs.whse_hours import (fetch_sap_whse_hours, create_mviews_whse_hrs, 
            create_mviews_branch_hours_array, create_dim_branch_hrs)

            fetch_sap_whse_hours = PythonOperator(
                task_id = 'fetch_sap_whse_hours',
                python_callable=fetch_sap_whse_hours,
                provide_context=True
            )

            create_mviews_whse_hrs = PythonOperator(
                task_id = 'create_mviews_whse_hrs',
                python_callable=create_mviews_whse_hrs,
                provide_context=True
            )

            create_mviews_branch_hours_array = PythonOperator(
                task_id = 'create_mviews_branch_hours_array',
                python_callable=create_mviews_branch_hours_array,
                provide_context=True
            )

            create_dim_branch_hrs = PythonOperator(
                task_id = 'create_dim_branch_hrs',
                python_callable=create_dim_branch_hrs,
                provide_context=True
            )

            fetch_sap_whse_hours >> create_mviews_whse_hrs >> create_mviews_branch_hours_array >> create_dim_branch_hrs

        """
        GET TECHNICIANS
        """
        with TaskGroup('technicians') as technicians:

            from sub_tasks.dimensionsETLs.technicians import (fetch_sap_technicians)

            fetch_sap_technicians = PythonOperator(
                task_id = 'fetch_sap_technicians',
                python_callable=fetch_sap_technicians,
                provide_context=True
            )

        insurances >> items >> users >> warehouse >> customers >> working_hours >> technicians

    """
    GOODS RECEIPT
    """
    with TaskGroup('goods_receipt') as goods_receipt:

        from sub_tasks.ordersETLs.goods_receipts import (fetch_goods_receipt, goods_receipt_live)

        fetch_goods_receipt = PythonOperator(
            task_id = 'fetch_goods_receipt',
            python_callable=fetch_goods_receipt,
            provide_context=True
        )

        goods_receipt_live = PythonOperator(
            task_id = 'goods_receipt_live',
            python_callable=goods_receipt_live,
            provide_context=True
        )

        fetch_goods_receipt >> goods_receipt_live

    """
    GET ORDERS
    """
    with TaskGroup('orders') as orders:

        from sub_tasks.ordersETLs.salesorders import (fetch_sap_orders, source_orders_header_with_prescriptions, 
        update_source_orders_line, update_mviews_salesorders_with_item_whse, create_mviews_source_orders_line_with_item_details,
        create_mviews_salesorders_line_cl_and_acc, create_order_live, create_fact_orders_header_with_categories)

        fetch_sap_orders = PythonOperator(
            task_id = 'fetch_sap_orders',
            python_callable=fetch_sap_orders,
            provide_context=True
        )

        source_orders_header_with_prescriptions = PythonOperator(
            task_id = 'source_orders_header_with_prescriptions',
            python_callable=source_orders_header_with_prescriptions,
            provide_context=True
        )

        update_source_orders_line = PythonOperator(
            task_id = 'update_source_orders_line',
            python_callable=update_source_orders_line,
            provide_context=True
        )

        update_mviews_salesorders_with_item_whse = PythonOperator(
            task_id = 'update_mviews_salesorders_with_item_whse',
            python_callable=update_mviews_salesorders_with_item_whse,
            provide_context=True
        )

        create_mviews_source_orders_line_with_item_details = PythonOperator(
            task_id = 'create_mviews_source_orders_line_with_item_details',
            python_callable=create_mviews_source_orders_line_with_item_details,
            provide_context=True
        )

        create_mviews_salesorders_line_cl_and_acc = PythonOperator(
            task_id = 'create_mviews_salesorders_line_cl_and_acc',
            python_callable=create_mviews_salesorders_line_cl_and_acc,
            provide_context=True
        )

        create_order_live = PythonOperator(
            task_id = 'create_order_live',
            python_callable=create_order_live,
            provide_context=True
        )

        create_fact_orders_header_with_categories = PythonOperator(
            task_id = 'create_fact_orders_header_with_categories',
            python_callable=create_fact_orders_header_with_categories,
            provide_context=True
        )

        fetch_sap_orders >> source_orders_header_with_prescriptions >> update_source_orders_line >> update_mviews_salesorders_with_item_whse >> create_mviews_source_orders_line_with_item_details >> create_mviews_salesorders_line_cl_and_acc >> create_order_live >> create_fact_orders_header_with_categories

    """
    GET FINANCES
    """
    with TaskGroup('finance') as finance:

        """
        GET INVOICES
        """
        with TaskGroup('invoices') as invoices:

            from sub_tasks.paymentsETLs.invoices import (fetch_sap_invoices)

            fetch_sap_invoices = PythonOperator(
                task_id = 'fetch_sap_invoices',
                python_callable=fetch_sap_invoices,
                provide_context=True
            )
        
        """
        GET EXPENSES
        """
        with TaskGroup('expenses') as expenses:

            from sub_tasks.paymentsETLs.expenses import (fetch_sap_expenses)

            fetch_sap_expenses = PythonOperator(
                task_id = 'fetch_sap_expenses',
                python_callable=fetch_sap_expenses,
                provide_context=True
            )

        """
        GET PAYMENTS
        """
        with TaskGroup('payments') as payments:

            from sub_tasks.paymentsETLs.payments import (fetch_sap_payments, create_customers_mop,create_customers_mop_new)
            from sub_tasks.paymentsETLs.web_payments import (fetch_sap_web_payments)

            fetch_sap_payments = PythonOperator(
                task_id = 'fetch_sap_payments',
                python_callable=fetch_sap_payments,
                provide_context=True
            )

            fetch_sap_web_payments = PythonOperator(
                task_id = 'fetch_sap_web_payments',
                python_callable=fetch_sap_web_payments,
                provide_context=True
            )

            create_customers_mop = PythonOperator(
                task_id = 'create_customers_mop',
                python_callable=create_customers_mop,
                provide_context=True
            )

            create_customers_mop_new = PythonOperator(
                task_id = 'create_customers_mop_new',
                python_callable=create_customers_mop_new,
                provide_context=True
            )

            [fetch_sap_payments,fetch_sap_web_payments] >> create_customers_mop >> create_customers_mop_new

        payments >> expenses >> invoices


    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> dimensions >> goods_receipt >> orders >> finance >> finish