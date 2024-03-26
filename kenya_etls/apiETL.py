import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from sub_tasks.api_login.api_login import login
from sub_tasks.api_login.api_login import login_uganda
from sub_tasks.api_login.api_login import login_rwanda

# from tmp.python_test
DAG_ID = 'API_LOGIN_Pipeline'

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
    schedule_interval='00,30 19,20,21,22,23,0,1,2,3,4,5 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    with TaskGroup('api_login') as api_login:

            login = PythonOperator(
                task_id = 'login',
                python_callable=login,
                provide_context=True
            ) 

    with TaskGroup('api_login_ug') as api_login_ug:
        login_uganda = PythonOperator(
            task_id = 'login_uganda',
            python_callable=login_uganda,
            provide_context=True
        )          

    with TaskGroup('api_login_rw') as api_login_rw:
        login_rwanda = PythonOperator(
            task_id = 'login_rwanda',
            python_callable=login_rwanda,
            provide_context=True
        ) 
          
          
    finish = DummyOperator(
            task_id = "finish"
        )

start >> api_login >> api_login_ug >> api_login_rw >> finish