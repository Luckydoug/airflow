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


from sub_tasks.ordersETLs.insurance_companies import(insurance_feedback_time, 
create_source_orderscreenc1_ins_companies_trans, index_orderscreenc1_ins_companies_trans, 
create_fact_orderscreenc1_ins_companies)

# from tmp.python_test
DAG_ID = 'Insurance_Companies_ETLs_Pipeline'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'start_date': datetime(2021, 12, 13)
    
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

    with TaskGroup('tables') as tables:

        insurance_feedback_time = PythonOperator(
            task_id = 'insurance_feedback_time',
            python_callable=insurance_feedback_time,
            provide_context=True
        )

        create_source_orderscreenc1_ins_companies_trans = PythonOperator(
            task_id = 'create_source_orderscreenc1_ins_companies_trans',
            python_callable=create_source_orderscreenc1_ins_companies_trans,
            provide_context=True
        )

        index_orderscreenc1_ins_companies_trans = PythonOperator(
            task_id = 'index_orderscreenc1_ins_companies_trans',
            python_callable=index_orderscreenc1_ins_companies_trans,
            provide_context=True
        )
    
        create_fact_orderscreenc1_ins_companies = PythonOperator(
            task_id = 'create_fact_orderscreenc1_ins_companies',
            python_callable=create_fact_orderscreenc1_ins_companies,
            provide_context=True
        )
        

        insurance_feedback_time >> create_source_orderscreenc1_ins_companies_trans >> index_orderscreenc1_ins_companies_trans >> create_fact_orderscreenc1_ins_companies

    
    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> tables >> finish

    """
    with TaskGroup('insurance_desk') as insurance_desk:

        create_source_orderscreenc1_insurance_trans = PythonOperator(
            task_id = 'create_source_orderscreenc1_insurance_trans',
            python_callable=create_source_orderscreenc1_insurance_trans,
            provide_context=True
        )

        index_source_orderscreenc1_insurance_trans = PythonOperator(
            task_id = 'index_source_orderscreenc1_insurance_trans',
            python_callable=index_source_orderscreenc1_insurance_trans,
            provide_context=True
        )

        create_fact_orderscreenc1_insurance = PythonOperator(
            task_id = 'create_fact_orderscreenc1_insurance',
            python_callable=create_fact_orderscreenc1_insurance,
            provide_context=True
        )

        index_fact_orderscreenc1_insurance = PythonOperator(
            task_id = 'index_fact_orderscreenc1_insurance',
            python_callable=index_fact_orderscreenc1_insurance,
            provide_context=True
        )

        create_source_orderscreenc1_insurance_trans >> index_source_orderscreenc1_insurance_trans >> create_fact_orderscreenc1_insurance >> index_fact_orderscreenc1_insurance
    """
    