import os
import sys

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.task_group import TaskGroup
# from airflow.example_dags.subdags.subdag import subdag
# from airflow.operators.bash_operator import BashOperator
# from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


# from tmp.python_test
DAG_ID = 'Conversions_ETL_Pipeline'

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
    schedule_interval='00 21 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    """
    ORDER CHECKING
    """
    with TaskGroup('orderchecking') as orderchecking:

        # from sub_tasks.conversions.order_checking_details import (fetch_order_checking_details, create_fact_order_checking_details,
        #     create_view_rx_pres, create_view_rx_clrr, create_view_rx_pay, create_view_rx_conv)
        from sub_tasks.conversions.order_checking_details import (fetch_order_checking_details, create_view_rx_conv)

        fetch_order_checking_details = PythonOperator(
            task_id = 'fetch_order_checking_details',
            python_callable=fetch_order_checking_details,
            provide_context=True
        )

        # create_fact_order_checking_details = PythonOperator(
        #     task_id = 'create_fact_order_checking_details',
        #     python_callable=create_fact_order_checking_details,
        #     provide_context=True
        # )

        # create_view_rx_pres = PythonOperator(
        #     task_id = 'create_view_rx_pres',
        #     python_callable=create_view_rx_pres,
        #     provide_context=True
        # )

        # create_view_rx_clrr = PythonOperator(
        #     task_id = 'create_view_rx_clrr',
        #     python_callable=create_view_rx_clrr,
        #     provide_context=True
        # )

        # create_view_rx_pay = PythonOperator(
        #     task_id = 'create_view_rx_pay',
        #     python_callable=create_view_rx_pay,
        #     provide_context=True
        # )

        create_view_rx_conv = PythonOperator(
            task_id = 'create_view_rx_conv',
            python_callable=create_view_rx_conv,
            provide_context=True
        )

        # fetch_order_checking_details >> create_fact_order_checking_details >> create_view_rx_pres >> create_view_rx_clrr >> create_view_rx_pay >> create_view_rx_conv
        fetch_order_checking_details >> create_view_rx_conv
    """
    OPTOM QUEUE
    """
    with TaskGroup('optomqueue') as optomqueue:

        from sub_tasks.conversions.optomqueue import (fetch_optom_queue_mgmt)

        fetch_optom_queue_mgmt = PythonOperator(
            task_id = 'fetch_optom_queue_mgmt',
            python_callable=fetch_optom_queue_mgmt,
            provide_context=True
        )

    """
    PRESCRIPTIONS
    """
    with TaskGroup('prescriptions') as prescriptions:

        from sub_tasks.conversions.prescriptions import (fetch_prescriptions, fact_prescriptions,create_et_conv)

        fetch_prescriptions = PythonOperator(
            task_id = 'fetch_prescriptions',
            python_callable=fetch_prescriptions,
            provide_context=True
        )

        fact_prescriptions = PythonOperator(
            task_id = 'fact_prescriptions',
            python_callable=fact_prescriptions,
            provide_context=True
        )

        # calculate_rx = PythonOperator(
        #     task_id = 'calculate_rx',
        #     python_callable=calculate_rx,
        #     provide_context=True
        # )

        
        create_et_conv = PythonOperator(
            task_id = 'create_et_conv',
            python_callable=create_et_conv,
            provide_context=True
        )

        fetch_prescriptions >> fact_prescriptions >> create_et_conv

    with TaskGroup('registrations') as registrations:

        from sub_tasks.dimensionsETLs.customers import(create_reg_conv)

        create_reg_conv = PythonOperator(
            task_id = 'create_reg_conv',
            python_callable=create_reg_conv,
            provide_context=True
        )


    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> orderchecking >> optomqueue >> prescriptions >> registrations >> finish
    # start >> orderchecking >> optomqueue >> prescriptions >> finish
