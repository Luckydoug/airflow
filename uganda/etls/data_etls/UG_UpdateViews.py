import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.cron_operator import CronOperator
# from airflow.operators.simple_dag_operator import SimpleDagOperator
# from airflow.sensors.timedelta_sensor import TimeDeltaSensor
from airflow.sensors.time_sensor import TimeSensor


from uganda_sub_tasks.postgres.salesorders_views import (refresh_order_line_with_details,refresh_salesorders_line_cl_and_rr,
                                                    refresh_fact_orders_header,refresh_order_contents)

from uganda_sub_tasks.postgres.prescriptions_views import refresh_et_conv
from uganda_sub_tasks.postgres.incentives import refresh_lens_silh

DAG_ID = 'UG_Update_Views'

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
    schedule_interval='30 19 * * *',
    catchup=False
    ) as dag:
    

    start = DummyOperator(
        task_id = "start"
    )

    with TaskGroup('salesorders') as salesorders:

        refresh_order_line_with_details = PythonOperator(
            task_id = 'refresh_order_line_with_details',
            python_callable = refresh_order_line_with_details,
            provide_context = True
        )  

        refresh_salesorders_line_cl_and_rr = PythonOperator(
            task_id = 'refresh_salesorders_line_cl_and_rr',
            python_callable = refresh_salesorders_line_cl_and_rr,
            provide_context = True
        )

        refresh_fact_orders_header = PythonOperator(
            task_id = 'refresh_fact_orders_header',
            python_callable = refresh_fact_orders_header,
            provide_context = True
        )

        refresh_order_contents = PythonOperator(
            task_id = 'refresh_order_contents',
            python_callable = refresh_order_contents,
            provide_context = True
        )

        refresh_order_line_with_details >> refresh_salesorders_line_cl_and_rr >> refresh_fact_orders_header >> refresh_order_contents

    with TaskGroup('prescriptions') as prescriptions:

        refresh_et_conv = PythonOperator(
            task_id = 'refresh_et_conv',
            python_callable = refresh_et_conv,
            provide_context = True
        )
    
    with TaskGroup('lens_silh') as lens_silh:

        refresh_lens_silh = PythonOperator(
            task_id = 'refresh_lens_silh',
            python_callable = refresh_lens_silh,
            provide_context = True
        )
        
    finish = DummyOperator(
        task_id = "finish"
    )

    start >> salesorders >> prescriptions >> lens_silh >> finish