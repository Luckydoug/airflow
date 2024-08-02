import sys
sys.path.append(".")
import psycopg2
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
import datetime
import pandas as pd
import holidays as pyholidays
from sub_tasks.data.connect_mawingu import (pg_execute, engine) 
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import create_unganda_engine
engine = create_unganda_engine()
from sub_tasks.libraries.utils import FromDate, ToDate


def GetSeparateEmployeeInformation(url):     

    SessionId = return_session_id(country="Rwanda:HRMS")

    headers = {
        'Authorization': f'Bearer {SessionId}',
        'Content-Type': 'application/json'
    }
    
    employee_information = pd.DataFrame()

    response = requests.get(url, headers=headers)
    print(response)
    if response.status_code == 200:
        data = response.json()
        df =data['result']
        df = pd.DataFrame(df)
        employee_information = employee_information.append(df)
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None     
    
    print(employee_information)
    print(employee_information.columns)

    employee_information = employee_information.rename(
        columns= {
            'Emp_Code':'emp_code',
            'Emp_Payroll_No':'payroll_no',
            'Emp_First_Name':'emp_first_name',
            'Emp_Middle_Name':'emp_middle_name',
            'Emp_Last_Name':'emp_last_name',
            'Emp_Name':'emp_name' ,
            'RequestDate':'requestdate',
            'RequestTypeCode':'requesttypecode',
            'RequestTypeName':'requesttypename',
            'RelievingDate':'relievingdate',
            'FinalDate':'finaldate',
            'NoticePeriodType':'noticeperiodtype',
            'NoticePeriod':'noticeperiod',
            'Comments':'comments'
            }
            )
    print(employee_information.columns)    

    return employee_information

def update_GetSeparateEmployeeInformation():
    url = "http://52.71.65.50:8094/API//Employee/GetSeparateEmployeeInformation"
    dfs_list  = GetSeparateEmployeeInformation(url)

    query = """truncate mawingu_staging.source_seperate_employee_information;"""
    query = pg_execute(query)
    dfs_list.to_sql('source_seperate_employee_information', con = engine, schema='mawingu_staging', if_exists = 'append', index=False)      
    
    print("Data Updated")





    


       