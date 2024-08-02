from airflow.models import variable
import os
import sys
from datetime import datetime,timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

DAG_ID = 'HRMS_Data'

default_args = {
    'owner': 'Data Team',
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
    schedule_interval='50 07 * * *',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )


    with TaskGroup('attendance_register') as attendance_register:

        from sub_tasks.hrms_data.attendance_register import (update_GetAttendanceRegisterData)  
        update_GetAttendanceRegisterData = PythonOperator(
            task_id='update_GetAttendanceRegisterData',
            python_callable=update_GetAttendanceRegisterData,
            provide_context=True
        )

        from sub_tasks.hrms_data.employee_information import (update_GetEmployeeInformation)
        update_GetEmployeeInformation = PythonOperator(
            task_id='update_GetEmployeeInformation',
            python_callable=update_GetEmployeeInformation,
            provide_context=True
        )

        from sub_tasks.hrms_data.attendance_raw_data import (update_GetAttendanceRawData)  
        update_GetAttendanceRawData = PythonOperator(
            task_id='update_GetAttendanceRawData',
            python_callable=update_GetAttendanceRawData,
            provide_context=True
        )

        from sub_tasks.hrms_data.employee_contact_email import (update_GetEmployeeContactEmail)  
        update_GetEmployeeContactEmail = PythonOperator(
            task_id='update_GetEmployeeContactEmail',
            python_callable=update_GetEmployeeContactEmail,
            provide_context=True
        )

        from sub_tasks.hrms_data.employee_seperate_information import (update_GetSeparateEmployeeInformation) 
        update_GetSeparateEmployeeInformation = PythonOperator(
            task_id='update_GetSeparateEmployeeInformation',
            python_callable=update_GetSeparateEmployeeInformation,
            provide_context=True
        )

        update_GetAttendanceRegisterData >> update_GetEmployeeInformation >> update_GetAttendanceRawData >> update_GetEmployeeContactEmail >> update_GetSeparateEmployeeInformation


    finish = DummyOperator(
        task_id="finish"
    )

    start >> attendance_register >> finish

    """
    From Optica Data Team
    Unleash the Power of Automation
    
    """