import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from sub_tasks.ordersETLs.overseas import(
    create_source_orderscreenc1_overseas,
    update_source_orderscreenc1_overseas, 
    transpose_overseas, 
    create_fact_orderscreenc1_overseas
)

# from tmp.python_test
DAG_ID = 'Overseas1_ETLs_Pipeline'

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
    schedule_interval='30 23 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    with TaskGroup('overseas') as overseas:

        create_source_orderscreenc1_overseas = PythonOperator(
            task_id = 'create_source_orderscreenc1_overseas',
            python_callable=create_source_orderscreenc1_overseas,
            provide_context=True
        )

        update_source_orderscreenc1_overseas = PythonOperator(
            task_id = 'update_source_orderscreenc1_overseas',
            python_callable=update_source_orderscreenc1_overseas,
            provide_context=True
        )

        transpose_overseas = PythonOperator(
            task_id = 'transpose_overseas',
            python_callable=transpose_overseas,
            provide_context=True
        )

        create_fact_orderscreenc1_overseas = PythonOperator(
            task_id = 'create_fact_orderscreenc1_overseas',
            python_callable=create_fact_orderscreenc1_overseas,
            provide_context=True
        )
        
        create_source_orderscreenc1_overseas >> update_source_orderscreenc1_overseas >> transpose_overseas >> create_fact_orderscreenc1_overseas

        #create_source_orderscreenc1_overseas >> update_source_orderscreenc1_overseas >> create_source_orderscreenc1_overseas_trans >> create_fact_orderscreenc1_overseas
       
    finish = DummyOperator(
        task_id = "finish"
    ) 


    start >> overseas >> finish
    