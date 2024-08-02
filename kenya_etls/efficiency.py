import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.example_dags.subdags.subdag import subdag
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta



DAG_ID = 'Main_Efficiency_ETLs_Pipeline'

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
    schedule_interval='05 20 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    """
    GET START TIME
    """

    with TaskGroup('start_time') as start_time:

        from sub_tasks.runtimes.time import (get_start_time)

        get_start_time = PythonOperator(
            task_id = 'get_start_time',
            python_callable=get_start_time,
            provide_context=True
        )

    """
    FETCH GSHEETS
    """
    
    with TaskGroup('gsheets_data') as gsheets_data:

        with TaskGroup('order_iisues') as order_iisues:

            from sub_tasks.gsheets.orders_issues import (fetch_orders_with_issues, update_orders_with_issues, 
            orders_with_issues_live)

            fetch_orders_with_issues = PythonOperator(
                task_id = 'fetch_orders_with_issues',
                python_callable=fetch_orders_with_issues,
                provide_context=True
            )

            update_orders_with_issues = PythonOperator(
                task_id = 'update_orders_with_issues',
                python_callable=update_orders_with_issues,
                provide_context=True
            )

            orders_with_issues_live = PythonOperator(
                task_id = 'orders_with_issues_live',
                python_callable=orders_with_issues_live,
                provide_context=True
            )

            fetch_orders_with_issues >> update_orders_with_issues >> orders_with_issues_live

        with TaskGroup('time_iisues') as time_iisues:

            from sub_tasks.gsheets.time_issues import (fetch_time_with_issues, update_time_with_issues, 
            time_with_issues_live)

            fetch_time_with_issues = PythonOperator(
                task_id = 'fetch_time_with_issues',
                python_callable=fetch_time_with_issues,
                provide_context=True
            )
            
            update_time_with_issues = PythonOperator(
                task_id = 'update_time_with_issues',
                python_callable=update_time_with_issues,
                provide_context=True
            )

            time_with_issues_live = PythonOperator(
                task_id = 'time_with_issues_live',
                python_callable=time_with_issues_live,
                provide_context=True
            )

            fetch_time_with_issues >> update_time_with_issues >> time_with_issues_live

        with TaskGroup('cutoffs') as cutoffs:

            from sub_tasks.gsheets.cutoff import (fetch_cutoffs, update_cutoffs, create_cutoffs_live)

            fetch_cutoffs = PythonOperator(
                task_id = 'fetch_cutoffs',
                python_callable=fetch_cutoffs,
                provide_context=True
            )

            update_cutoffs = PythonOperator(
                task_id = 'update_cutoffs',
                python_callable=update_cutoffs,
                provide_context=True
            )

            create_cutoffs_live = PythonOperator(
                task_id = 'create_cutoffs_live',
                python_callable=create_cutoffs_live,
                provide_context=True
            )

            fetch_cutoffs >> update_cutoffs >> create_cutoffs_live

        # with TaskGroup('novax') as novax:

        #     from sub_tasks.gsheets.novax import (fetch_novax_data, create_dim_novax_data, fetch_dhl_data, 
        #     create_dim_dhl_data, create_dhl_with_orderscreen_data)

        #     fetch_novax_data = PythonOperator(
        #         task_id = 'fetch_novax_data',
        #         python_callable=fetch_novax_data,
        #         provide_context=True
        #     )

        #     fetch_dhl_data = PythonOperator(
        #         task_id = 'fetch_dhl_data',
        #         python_callable=fetch_dhl_data,
        #         provide_context=True
        #     )


        #     create_dim_novax_data = PythonOperator(
        #         task_id = 'create_dim_novax_data',
        #         python_callable=create_dim_novax_data,
        #         provide_context=True
        #     )

        #     create_dim_dhl_data = PythonOperator(
        #         task_id = 'create_dim_dhl_data',
        #         python_callable=create_dim_dhl_data,
        #         provide_context=True
        #     )

        #     create_dhl_with_orderscreen_data = PythonOperator(
        #         task_id = 'create_dhl_with_orderscreen_data',
        #         python_callable=create_dhl_with_orderscreen_data,
        #         provide_context=True
        #     )

        #     [fetch_novax_data, fetch_dhl_data] >> create_dim_novax_data >> create_dim_dhl_data >> create_dhl_with_orderscreen_data
    
        with TaskGroup('branchstock_cutoff') as branchstock_cutoff:

            from sub_tasks.gsheets.branchstockcutoffs import(fetch_branchstock_cutoffs, create_branchstock_cutoffs)

            fetch_branchstock_cutoffs = PythonOperator(
                task_id = 'fetch_branchstock_cutoffs',
                python_callable=fetch_branchstock_cutoffs,
                provide_context=True
            )

            create_branchstock_cutoffs = PythonOperator(
                task_id = 'create_branchstock_cutoffs',
                python_callable=create_branchstock_cutoffs,
                provide_context=True
            )

            fetch_branchstock_cutoffs >> create_branchstock_cutoffs

        with TaskGroup('itr_issues') as itr_issues:

            from sub_tasks.gsheets.itr_issues import(fetch_itr_issues, create_dim_itr_issues)
            from sub_tasks.gsheets.itr_time_issues import(fetch_itr_time_issues, create_dim_itr_time_issues)

            fetch_itr_issues = PythonOperator(
                task_id = 'fetch_itr_issues',
                python_callable=fetch_itr_issues,
                provide_context=True
            )

            fetch_itr_time_issues = PythonOperator(
                task_id = 'fetch_itr_time_issues',
                python_callable=fetch_itr_time_issues,
                provide_context=True
            )

            create_dim_itr_issues = PythonOperator(
                task_id = 'create_dim_itr_issues',
                python_callable=create_dim_itr_issues,
                provide_context=True
            )

            create_dim_itr_time_issues = PythonOperator(
                task_id = 'create_dim_itr_time_issues',
                python_callable=create_dim_itr_time_issues,
                provide_context=True
            )

            [fetch_itr_issues, fetch_itr_time_issues] >> create_dim_itr_issues >> create_dim_itr_time_issues

        with TaskGroup('user_mapping') as user_mapping:

            from sub_tasks.gsheets.branch_user_mapping import(fetch_branch_user_mappings)

            fetch_branch_user_mappings = PythonOperator(
                task_id = 'fetch_branch_user_mappings',
                python_callable=fetch_branch_user_mappings,
                provide_context=True
            )


        order_iisues >> time_iisues >> cutoffs >> branchstock_cutoff >> itr_issues >> user_mapping
        
    """
    FETCH ORDERSCREEN   
    """
    
    with TaskGroup('orderscreen_etls') as orderscreen_etls:

        with TaskGroup('orderscreen_details') as orderscreen_details:
            from sub_tasks.ordersETLs.ordersscreendetails import (fetch_sap_orderscreendetails, 
            update_to_source_orderscreen,create_source_orderscreen_staging, create_fact_orderscreen)

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

            create_source_orderscreen_staging = PythonOperator(
                task_id = 'create_source_orderscreen_staging',
                python_callable=create_source_orderscreen_staging,
                provide_context=True
            )

            create_fact_orderscreen = PythonOperator(
                task_id = 'create_fact_orderscreen',
                python_callable=create_fact_orderscreen,
                provide_context=True
            )

            fetch_sap_orderscreendetails >> update_to_source_orderscreen >> create_source_orderscreen_staging >> create_fact_orderscreen
        
        with TaskGroup('orderscreenc1_etls') as orderscreenc1_etls:
            
            from sub_tasks.ordersETLs.orderscreendetailsc1 import (fetch_sap_orderscreendetailsc1, 
            update_to_source_orderscreenc1, create_source_orderscreenc1_staging, 
            create_source_orderscreenc1_staging2, create_source_orderscreenc1_staging3, 
            create_source_orderscreenc1_staging4, create_source_orderscreenc1_staging4, 
            update_orderscreenc1_staging4, get_collected_orders, transpose_orderscreenc1, index_trans,
            create_fact_orderscreenc1_new,get_techs)
            
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

            create_source_orderscreenc1_staging = PythonOperator(
                task_id = 'create_source_orderscreenc1_staging',
                python_callable=create_source_orderscreenc1_staging,
                provide_context=True
            )

            create_source_orderscreenc1_staging2 = PythonOperator(
                task_id = 'create_source_orderscreenc1_staging2',
                python_callable=create_source_orderscreenc1_staging2,
                provide_context=True
            )

            create_source_orderscreenc1_staging3 = PythonOperator(
                task_id = 'create_source_orderscreenc1_staging3',
                python_callable=create_source_orderscreenc1_staging3,
                provide_context=True
            )

            create_source_orderscreenc1_staging4 = PythonOperator(
                task_id = 'create_source_orderscreenc1_staging4',
                python_callable=create_source_orderscreenc1_staging4,
                provide_context=True
            )

            update_orderscreenc1_staging4 = PythonOperator(
                task_id = 'update_orderscreenc1_staging4',
                python_callable=update_orderscreenc1_staging4,
                provide_context=True
            )

            get_collected_orders = PythonOperator(
                task_id = 'get_collected_orders',
                python_callable=get_collected_orders,
                provide_context=True
            )

            transpose_orderscreenc1 = PythonOperator(
                task_id = 'transpose_orderscreenc1',
                python_callable=transpose_orderscreenc1,
                provide_context=True
            )

            index_trans = PythonOperator(
                task_id = 'index_trans',
                python_callable=index_trans,
                provide_context=True
            )

            create_fact_orderscreenc1_new = PythonOperator(
                task_id = 'create_fact_orderscreenc1_new',
                python_callable=create_fact_orderscreenc1_new,
                provide_context=True
            )

            get_techs = PythonOperator(
                task_id = 'get_techs',
                python_callable=get_techs,
                provide_context=True
            )

            fetch_sap_orderscreendetailsc1 >> update_to_source_orderscreenc1 >> create_source_orderscreenc1_staging >> create_source_orderscreenc1_staging2 >> create_source_orderscreenc1_staging3 >> create_source_orderscreenc1_staging4 >> update_orderscreenc1_staging4 >> get_collected_orders >> transpose_orderscreenc1 >> index_trans >> create_fact_orderscreenc1_new >> get_techs

        orderscreen_details >> orderscreenc1_etls
    
    """
    FETCH DROPPED ORDERS
    """
    
    with TaskGroup('dropped_etls') as dropped_etls:

        from sub_tasks.ordersETLs.dropped_orders import (get_source_dropped_orders,update_source_dropped_orders,
        get_source_dropped_orders_staging, create_fact_dropped_orders)

        get_source_dropped_orders = PythonOperator(
            task_id = 'get_source_dropped_orders',
            python_callable=get_source_dropped_orders,
            provide_context=True
        )

        update_source_dropped_orders = PythonOperator(
            task_id = 'update_source_dropped_orders',
            python_callable=update_source_dropped_orders,
            provide_context=True
        )

        get_source_dropped_orders_staging = PythonOperator(
            task_id = 'get_source_dropped_orders_staging',
            python_callable=get_source_dropped_orders_staging,
            provide_context=True
        )

        create_fact_dropped_orders = PythonOperator(
            task_id = 'create_fact_dropped_orders',
            python_callable=create_fact_dropped_orders,
            provide_context=True
        )

        get_source_dropped_orders >> update_source_dropped_orders >> get_source_dropped_orders_staging  >> create_fact_dropped_orders

    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> start_time >> gsheets_data >> orderscreen_etls >> dropped_etls >> finish
