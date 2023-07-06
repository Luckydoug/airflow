import os
import sys

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from datetime import datetime

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from sub_tasks.gsheets.branch_user_mapping import (fetch_branch_user_mappings, create_dim_branch_user_mapping)
from sub_tasks.gsheets.branches import (fetch_branch_tiers, create_dim_branches)
from sub_tasks.gsheets.holidays import (fetch_holidays)
from sub_tasks.gsheets.exempt_users import (fetch_exempt_users)
from sub_tasks.gsheets.riders import (fetch_rider_times)
# from sub_tasks.gsheets.sop import (fetch_sop_branch_info, fetch_sop)
from sub_tasks.gsheets.routes import (fetch_routesdata)

# from sub_tasks.gsheets.novaxnew import (fetch_novax_data1,fetch_dhl_data1,create_dim_novax_data1)

# from tmp.python_test
DAG_ID = 'Gsheet_ETL_Pipeline'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'start_date': datetime(2021, 12, 13)
    
}


with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='30 16 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    """
    HOLIDAYS
    """
    with TaskGroup('holidays') as holidays:

        fetch_holidays = PythonOperator(
            task_id = 'fetch_holidays',
            python_callable=fetch_holidays,
            provide_context=True
        )

    """
    RIDERS TIMINGS
    """
    with TaskGroup('riders') as riders:

        fetch_rider_times = PythonOperator(
            task_id = 'fetch_rider_times',
            python_callable=fetch_rider_times,
            provide_context=True
        )

    """
    ROUTES
    """
    with TaskGroup('routes') as routes:

        fetch_routesdata = PythonOperator(
            task_id = 'fetch_routesdata',
            python_callable=fetch_routesdata,
            provide_context=True
        )

    """
    EXEMPT USERS
    """
    with TaskGroup('exempt_users') as exempt_users:

        fetch_exempt_users = PythonOperator(
            task_id = 'fetch_exempt_users',
            python_callable=fetch_exempt_users,
            provide_context=True
        )

    """
    BRANCHES
    """
    with TaskGroup('branches') as branches:

        fetch_branch_user_mappings = PythonOperator(
            task_id = 'fetch_branch_user_mappings',
            python_callable=fetch_branch_user_mappings,
            provide_context=True
        )

        create_dim_branch_user_mapping = PythonOperator(
            task_id = 'create_dim_branch_user_mapping',
            python_callable=create_dim_branch_user_mapping,
            provide_context=True
        )

        fetch_branch_tiers = PythonOperator(
            task_id = 'fetch_branch_tiers',
            python_callable=fetch_branch_tiers,
            provide_context=True
        )

        create_dim_branches = PythonOperator(
            task_id = 'create_dim_branches',
            python_callable=create_dim_branches,
            provide_context=True
        )
        
        fetch_branch_user_mappings >> create_dim_branch_user_mapping >> fetch_branch_tiers >> create_dim_branches

    # with TaskGroup('sop') as sop:

    #     fetch_sop_branch_info = PythonOperator(
    #         task_id = 'fetch_sop_branch_info',
    #         python_callable=fetch_sop_branch_info,
    #         provide_context=True
    #     )

    #     fetch_sop = PythonOperator(
    #         task_id = 'fetch_sop',
    #         python_callable=fetch_sop,
    #         provide_context=True
    #     )

    #     fetch_sop_branch_info >> fetch_sop
        


    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> exempt_users >> holidays >> branches >> riders >> routes >> finish
