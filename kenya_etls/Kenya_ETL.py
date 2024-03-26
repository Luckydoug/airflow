import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

DAG_ID = 'Kenya_ETL'

default_args = {
    'owner': 'Data Team',
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
    'start_date': datetime(2021, 12, 13),
    'email': ['ian.gathumbi@optica.africa'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='00 21 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
        )

    """
    GET MAIN TABLES
    """
    with TaskGroup('main_tables') as main_tables:
        
        from sub_tasks.api_login.api_login import login

        login = PythonOperator(
            task_id = 'login',
            python_callable=login,
            provide_context=True
                )
        
        """
        GET ORDERSCREEN
        """
        with TaskGroup('fetch_orderscreen') as fetch_orderscreen:
            
            from sub_tasks.ordersETLs.ordersscreendetails import (fetch_sap_orderscreendetails,update_to_source_orderscreen)

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

        """
        GET ORDERSCREENC1
        """
        with TaskGroup('fetch_orderscreenc1') as fetch_orderscreenc1:

            from sub_tasks.ordersETLs.orderscreendetailsc1 import (fetch_sap_orderscreendetailsc1,update_to_source_orderscreenc1)

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
        
        """
        GET PRESCRIPTIONS
        """
        from sub_tasks.conversions.prescriptions import fetch_prescriptions

        fetch_prescriptions = PythonOperator(
            task_id = 'fetch_prescriptions',
            python_callable=fetch_prescriptions,
            provide_context=True
            )

        """
        GET OPTOM_QUEUE
        """
        from sub_tasks.conversions.optomqueue import fetch_optom_queue_mgmt

        fetch_optom_queue_mgmt = PythonOperator(
            task_id = 'fetch_optom_queue_mgmt',
            python_callable=fetch_optom_queue_mgmt,
            provide_context=True
            )
        
        login >> fetch_orderscreen >> fetch_orderscreenc1 >> fetch_prescriptions >> fetch_optom_queue_mgmt

    with TaskGroup('branch_efficiency') as branch_efficiency:

        from sub_tasks.postgres.printing_identifier import update_printing_identifier

        update_printing_identifier = PythonOperator(
            task_id = 'update_printing_identifier',
            python_callable=update_printing_identifier,
            provide_context=True
            )
        
        from sub_tasks.postgres.optom_queue_mgmt import (refresh_optom_queue_time, refresh_optom_queue_no_et)

        refresh_optom_queue_time = PythonOperator(
            task_id = 'refresh_optom_queue_time',
            python_callable=refresh_optom_queue_time,
            provide_context=True
            )

        refresh_optom_queue_no_et = PythonOperator(
            task_id = 'refresh_optom_queue_no_et',
            python_callable=refresh_optom_queue_no_et,
            provide_context=True
            )
        
        from sub_tasks.emails.preauth_emails import fetch_preauth_requests

        fetch_preauth_requests = PythonOperator(
            task_id = 'fetch_preauth_requests',
            python_callable=fetch_preauth_requests,
            provide_context=True
            )

        from sub_tasks.postgres.insurance_efficiency import (update_insurance_efficiency_before_feedback,update_insurance_efficiency_after_feedback)

        update_insurance_efficiency_before_feedback = PythonOperator(
            task_id = 'update_insurance_efficiency_before_feedback',
            python_callable=update_insurance_efficiency_before_feedback,
            provide_context=True
            )

        
        update_insurance_efficiency_after_feedback = PythonOperator(
            task_id = 'update_insurance_efficiency_after_feedback',
            python_callable=update_insurance_efficiency_after_feedback,
            provide_context=True
            )
        
        update_printing_identifier >> refresh_optom_queue_time >> refresh_optom_queue_no_et >> fetch_preauth_requests \
        >> update_insurance_efficiency_before_feedback >> update_insurance_efficiency_after_feedback

    with TaskGroup('lensstore') as lensstore:

        from sub_tasks.postgres.lensstore_efficiency import (update_lensstore_efficiency_from_receiving,update_lensstore_efficiency_from_mainstore)
        
        update_lensstore_efficiency_from_receiving = PythonOperator(
            task_id = 'update_lensstore_efficiency_from_receiving',
            python_callable=update_lensstore_efficiency_from_receiving,
            provide_context=True
            )

        update_lensstore_efficiency_from_mainstore = PythonOperator(
            task_id = 'update_lensstore_efficiency_from_mainstore',
            python_callable=update_lensstore_efficiency_from_mainstore,
            provide_context=True
            )
        
        update_lensstore_efficiency_from_receiving >> update_lensstore_efficiency_from_mainstore
    
    finish = DummyOperator(
        task_id = "finish"
        ) 
    
    start >> main_tables >> branch_efficiency >> lensstore >> finish