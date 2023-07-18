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


# from tmp.python_test
DAG_ID = 'Operations_Efficiency'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'start_date': datetime(2021, 12, 13)
    
}

with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='30 2 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    with TaskGroup('order_efficiency') as order_efficiency:
        from kenya_automation.alex_daily_report.departments_orders_efficiency import (update_calculated_field)
        
        update_calculated_field = PythonOperator(
            task_id = 'update_calculated_field',
            python_callable = update_calculated_field,
            provide_context = True
        )

        update_calculated_field

    with TaskGroup('replacements') as replacements:
        from kenya_automation.alex_daily_report.replacement import (replacements)
        
        replacements = PythonOperator(
            task_id = 'replacements',
            python_callable = replacements,
            provide_context = True
        )
        replacements

    with TaskGroup('cutoff') as cutoff:
        from kenya_automation.alex_daily_report.cut_off import (cutoff)
        
        cutoff = PythonOperator(
            task_id = 'cutoff',
            python_callable = cutoff,
            provide_context = True
        )
        cutoff

    with TaskGroup('awaiting_feedback') as awaiting_feedback:
        from kenya_automation.alex_daily_report.lens_store_awaiting_feedback import (awaiting_feedback)
        
        awaiting_feedback = PythonOperator(
            task_id = 'awaiting_feedback',
            python_callable = awaiting_feedback,
            provide_context = True
        )
        awaiting_feedback

    with TaskGroup('damage_suppy_time') as damage_suppy_time:
        from kenya_automation.alex_daily_report.damage_supply_time import (damage_suppy_time)
        
        damage_suppy_time = PythonOperator(
            task_id = 'damage_suppy_time',
            python_callable = damage_suppy_time,
            provide_context = True
        )
        damage_suppy_time

    with TaskGroup('order_efficiency_smtp') as order_efficiency_smtp:
        from kenya_automation.alex_daily_report.report_smtp import (order_efficiency_smtp)
        
        order_efficiency_smtp = PythonOperator(
            task_id = 'order_efficiency_smtp',
            python_callable = order_efficiency_smtp,
            provide_context = True
        )
        order_efficiency_smtp

    finish = DummyOperator(
        task_id = "finish"
    )

    start >> order_efficiency >> replacements >> cutoff >> awaiting_feedback >> damage_suppy_time >> order_efficiency_smtp >> finish