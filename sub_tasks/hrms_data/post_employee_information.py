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
# from sub_tasks.libraries.utils import FromDate, ToDate



def fetch_data(url, from_date, to_date):   

    branches = f"""
    select branch_code from reports_tables.branch_data bd
    where branch_code not in ('ONA','MKT','TDA','HOM','NWE')
    """
    
    branches = pd.read_sql_query(branches,con = engine)
    branches_list = branches['branch_code'].to_list()


    SessionId = return_session_id(country="Kenya:HRMS")

    headers = {
        'Authorization': f'Bearer {SessionId}',
        'Content-Type': 'application/json'
    }
    
    dataframe = pd.DataFrame()

    for location in branches_list:
        params = {
            "FromDate": from_date,
            "ToDate": to_date,
            "Locations": [location]
        }
        response = requests.get(url, headers=headers, json=params)
        if response.status_code == 200:
            data = response.json()
            df =data['result']
            for obj in df:
                obj['branch'] = location     

            df = pd.DataFrame(df)
            dataframe = dataframe.append(df)
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return None 

    print(dataframe)
    if len(dataframe):
        attendance_data = pd.DataFrame(dataframe)
        print(attendance_data)
    # concatenated_df = pd.concat(dfs, ignore_index=True)
        attendance_data = attendance_data.rename(
            columns= {'Emp_Payroll_No':'payroll_no',
                'Emp_Name':'employee_name',
                'ICardNo':'icard_no',
                'InOutDateTime':'inoutdatetime',
                'InOut':'in_out'}
        )
        return attendance_data
    else:
        return pd.DataFrame()   

url = "http://52.71.65.50:8094/api/Attendance/GetAttendanceRawData"
from_date = "04-Jan-2024"
to_date = "04-Jan-2024"

def update_to_datawarehouse():
    dfs_list  = fetch_data(url, from_date, to_date)
    print(dfs_list)
    query = """truncate mabawa_staging.source_attendance_data;"""
    query = pg_execute(query)
    dfs_list.to_sql('source_attendance_data', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)







    


       
       