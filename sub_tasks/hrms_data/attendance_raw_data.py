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



def GetAttendanceRawData(url, from_date, to_date):   
    branches = f"""
    select location_code as branch_code from reports_tables.hrms_locations bd
    --where location_code <> 'MER'
    """

    branches = pd.read_sql_query(branches,con = engine)    
    branches_list = branches['branch_code'].to_list()
    print(branches_list)

    SessionId = return_session_id(country="Kenya:HRMS")

    headers = {
        'Authorization': f'Bearer {SessionId}',
        'Content-Type': 'application/json'
    }

    dataframe = pd.DataFrame()

    for location in branches_list:
        print(location)
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

    if len(dataframe):
        attendance_data = pd.DataFrame(dataframe)
        print(attendance_data)
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

def update_GetAttendanceRawData():
    url = "http://52.71.65.50:8094/api/Attendance/GetAttendanceRawData"
    from_date = FromDate
    to_date = ToDate
    print(from_date,to_date)

    dfs_list  = GetAttendanceRawData(url, from_date, to_date)

    dfs_list = dfs_list.set_index(['payroll_no','inoutdatetime'])    
    print("Data Indexed")

    upsert(engine=engine,
       df=dfs_list,
       schema='mabawa_staging',
       table_name='source_attendance_data',
       if_row_exists='update',
       create_table=False,
       add_new_columns=True)
    
    print("Data Updated")


# update_GetAttendanceRawData()



       