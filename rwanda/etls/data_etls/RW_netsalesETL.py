import os
import sys

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

from rwanda_sub_tasks.daily_salereport.dailynetpayments import (daily_net_payments_rwanda,daily_mtd_payments_rwanda,mtd_daily_net_payments_rwanda)
from rwanda_sub_tasks.daily_salereport.dailynetpayments_smtp import daily_netsales_email


# from tmp.python_test
DAG_ID = 'RW_Daily_Net_Sales'

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
    schedule_interval='15 4 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    """
    UPDATE
    """
    with TaskGroup('daily_update') as daily_update:
        
        daily_net_payments_rwanda = PythonOperator(
            task_id = 'daily_net_payments_rwanda',
            python_callable=daily_net_payments_rwanda,
            provide_context=True
        )
        daily_net_payments_rwanda 

    with TaskGroup('mtd_daily_update') as mtd_daily_update:
        daily_mtd_payments_rwanda = PythonOperator(
            task_id = 'daily_mtd_payments_rwanda',
            python_callable=daily_mtd_payments_rwanda,
            provide_context=True
        )
        daily_mtd_payments_rwanda

    with TaskGroup('update_final') as update_final:

        mtd_daily_net_payments_rwanda = PythonOperator(
        task_id = 'mtd_daily_net_payments_rwanda',
        python_callable=mtd_daily_net_payments_rwanda,
        provide_context=True
        )
        mtd_daily_net_payments_rwanda

    with TaskGroup('smtp') as smtp:

        daily_netsales_email = PythonOperator(
            task_id = 'daily_netsales_email',
            python_callable=daily_netsales_email,
            provide_context=True
        )

        daily_netsales_email
        
    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> daily_update >> mtd_daily_update >> update_final >> smtp >> finish