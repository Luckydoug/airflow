import os
import sys

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from datetime import datetime
from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

from sub_tasks.daily_salereport.monthly_gross_sales import (create_gross_payments,summary_gross_payments,summary_net_payments,shops_ytd_net_sales)
from sub_tasks.daily_salereport.monthly_net_sales import  create_net_sales

# from tmp.python_test
DAG_ID = 'KE_Monthly_Gross_Sales_ETL'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'start_date': datetime(2021, 12, 13)
    
}

with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='30 02 01 * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    """
    UPDATE
    """
    with TaskGroup('update') as update:
        
        create_gross_payments = PythonOperator(
            task_id = 'create_gross_payments',
            python_callable=create_gross_payments,
            provide_context=True
        )

        summary_gross_payments = PythonOperator(
            task_id = 'summary_gross_payments',
            python_callable=summary_gross_payments,
            provide_context=True
        ) 

        summary_net_payments = PythonOperator(
            task_id = 'summary_net_payments',
            python_callable=summary_net_payments,
            provide_context=True
        ) 

        shops_ytd_net_sales = PythonOperator(
            task_id = 'shops_ytd_net_sales',
            python_callable=shops_ytd_net_sales,
            provide_context=True
        )       

        create_gross_payments >> summary_gross_payments >> summary_net_payments >> shops_ytd_net_sales

    with TaskGroup('final') as final:
        create_net_sales = PythonOperator(
        task_id = 'create_net_sales',
        python_callable=create_net_sales,
        provide_context=True
    )  
        
        create_net_sales

    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> update >> final >> finish
    