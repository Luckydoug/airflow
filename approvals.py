import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.example_dags.subdags.subdag import subdag
from airflow.utils.task_group import TaskGroup
from datetime import datetime


from sub_tasks.ordersETLs.approvals import(order_insurance_schemes,create_source_orderscreenc1_approvals, 
update_source_orderscreenc1_approvals, create_source_orderscreenc1_approvals_2, 
create_source_orderscreenc1_approvals_3, create_source_orderscreenc1_approvals_4,
create_source_orderscreenc1_approvals_5, create_source_orderscreenc1_approvals_trans, 
create_fact_orderscreenc1_approvals, alter_fact_orderscreenc1_approvals,
index_fact_orderscreenc1_approvals,
get_collected_orders_for_approvals, 
v_all_approval_orders, order_rejection_count,
rejected_orders, approvals_not_rejected, approvals_rejected_once, approvals_rejected_twice)

# from tmp.python_test
DAG_ID = 'Approvals_ETLs_Pipeline'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'start_date': datetime(2021, 12, 13)
    
}


with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='00 6 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    with TaskGroup('approvals') as approvals:
    
        order_insurance_schemes = PythonOperator(
            task_id = 'order_insurance_schemes',
            python_callable=order_insurance_schemes,
            provide_context=True
        )

        create_source_orderscreenc1_approvals = PythonOperator(
            task_id = 'create_source_orderscreenc1_approvals',
            python_callable=create_source_orderscreenc1_approvals,
            provide_context=True
        )

        update_source_orderscreenc1_approvals = PythonOperator(
            task_id = 'update_source_orderscreenc1_approvals',
            python_callable=update_source_orderscreenc1_approvals,
            provide_context=True
        )

        create_source_orderscreenc1_approvals_2 = PythonOperator(
            task_id = 'create_source_orderscreenc1_approvals_2',
            python_callable=create_source_orderscreenc1_approvals_2,
            provide_context=True
        )

        create_source_orderscreenc1_approvals_3 = PythonOperator(
            task_id = 'create_source_orderscreenc1_approvals_3',
            python_callable=create_source_orderscreenc1_approvals_3,
            provide_context=True
        )

        create_source_orderscreenc1_approvals_4 = PythonOperator(
            task_id = 'create_source_orderscreenc1_approvals_4',
            python_callable=create_source_orderscreenc1_approvals_4,
            provide_context=True
        )

        create_source_orderscreenc1_approvals_5 = PythonOperator(
            task_id = 'create_source_orderscreenc1_approvals_5',
            python_callable=create_source_orderscreenc1_approvals_5,
            provide_context=True
        )

        create_source_orderscreenc1_approvals_trans = PythonOperator(
            task_id = 'create_source_orderscreenc1_approvals_trans',
            python_callable=create_source_orderscreenc1_approvals_trans,
            provide_context=True
        )

        create_fact_orderscreenc1_approvals = PythonOperator(
            task_id = 'create_fact_orderscreenc1_approvals',
            python_callable=create_fact_orderscreenc1_approvals,
            provide_context=True
        )

        alter_fact_orderscreenc1_approvals = PythonOperator(
            task_id = 'alter_fact_orderscreenc1_approvals',
            python_callable=alter_fact_orderscreenc1_approvals,
            provide_context=True
        )

        index_fact_orderscreenc1_approvals = PythonOperator(
            task_id = 'index_fact_orderscreenc1_approvals',
            python_callable=index_fact_orderscreenc1_approvals,
            provide_context=True
        )

        order_insurance_schemes >> create_source_orderscreenc1_approvals >> update_source_orderscreenc1_approvals >> create_source_orderscreenc1_approvals_2 >> create_source_orderscreenc1_approvals_3 >> create_source_orderscreenc1_approvals_4 >> create_source_orderscreenc1_approvals_5 >> create_source_orderscreenc1_approvals_trans >> create_fact_orderscreenc1_approvals >> alter_fact_orderscreenc1_approvals >> index_fact_orderscreenc1_approvals

        """
        get_collected_orders_for_approvals = PythonOperator(
            task_id = 'get_collected_orders_for_approvals',
            python_callable=get_collected_orders_for_approvals,
            provide_context=True
        )

        create_source_orderscreenc1_approvals_trans = PythonOperator(
            task_id = 'create_source_orderscreenc1_approvals_trans',
            python_callable=create_source_orderscreenc1_approvals_trans,
            provide_context=True
        )

        create_fact_orderscreenc1_approvals = PythonOperator(
            task_id = 'create_fact_orderscreenc1_approvals',
            python_callable=create_fact_orderscreenc1_approvals,
            provide_context=True
        )

        order_insurance_schemes = PythonOperator(
            task_id = 'order_insurance_schemes',
            python_callable=order_insurance_schemes,
            provide_context=True
        )

        v_all_approval_orders = PythonOperator(
            task_id = 'v_all_approval_orders',
            python_callable=v_all_approval_orders,
            provide_context=True
        )

        order_rejection_count = PythonOperator(
            task_id = 'order_rejection_count',
            python_callable=order_rejection_count,
            provide_context=True
        )

        rejected_orders = PythonOperator(
            task_id = 'rejected_orders',
            python_callable=rejected_orders,
            provide_context=True
        )

        approvals_not_rejected = PythonOperator(
            task_id = 'approvals_not_rejected',
            python_callable=approvals_not_rejected,
            provide_context=True
        )

        approvals_rejected_once = PythonOperator(
            task_id = 'approvals_rejected_once',
            python_callable=approvals_rejected_once,
            provide_context=True
        )

        approvals_rejected_twice = PythonOperator(
            task_id = 'approvals_rejected_twice',
            python_callable=approvals_rejected_twice,
            provide_context=True
        )

        create_source_orderscreenc1_approvals >> update_source_orderscreenc1_approvals >> create_source_orderscreenc1_approvals_2 >> get_collected_orders_for_approvals >> create_source_orderscreenc1_approvals_trans >> create_fact_orderscreenc1_approvals >> order_insurance_schemes >> v_all_approval_orders >> order_rejection_count >> rejected_orders >> approvals_not_rejected >> approvals_rejected_once >> approvals_rejected_twice
        """

        """
        
        index_source_orderscreenc1_approvals = PythonOperator(
            task_id = 'index_source_orderscreenc1_approvals',
            python_callable=index_source_orderscreenc1_approvals,
            provide_context=True
        )

        
        index_fact_orderscreenc1_approvals = PythonOperator(
            task_id = 'index_fact_orderscreenc1_approvals',
            python_callable=index_fact_orderscreenc1_approvals,
            provide_context=True
        )
        
        create_source_orderscreenc1_approvals >> update_source_orderscreenc1_approvals >> create_source_orderscreenc1_approvals_trans >> index_source_orderscreenc1_approvals >> create_fact_orderscreenc1_approvals >> index_fact_orderscreenc1_approvals >> order_insurance_schemes >> v_all_approval_orders >> order_rejection_count >> rejected_orders >> approvals_not_rejected >> approvals_rejected_once >> approvals_rejected_twice
        """
    
    finish = DummyOperator(
        task_id = "finish"
    )

    start >> approvals >> finish