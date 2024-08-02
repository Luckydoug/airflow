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
from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import createe_engine
engine = createe_engine()
from sub_tasks.libraries.utils import FromDate, ToDate


def GetEmployeeContactEmail(url):     

    SessionId = return_session_id(country="Kenya:HRMS")

    headers = {
        'Authorization': f'Bearer {SessionId}',
        'Content-Type': 'application/json'
    }
    
    employee_information = pd.DataFrame()
    # params = {
    #     "FromDate": from_date,
    #     "ToDate": to_date
    #    }
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
            'Emp_Payroll_No':'payroll_no',
            'Emp_Name':'name',
            'EmailType':'emailtype',
            'EmailAddress':'emailaddress',
            'Is_Default':'is_default'     
            }
            )
    print(employee_information.columns)    

    return employee_information

def update_GetEmployeeContactEmail():
    url = "http://52.71.65.50:8094/API//Employee/GetEmployeeContactEmail"
    dfs_list  = GetEmployeeContactEmail(url)

    query = """truncate mabawa_staging.source_contact_email;"""
    query = pg_execute(query)
    dfs_list.to_sql('source_contact_email', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)      
    
    print("Data Updated")

# update_GetEmployeeContactEmail()



    


       