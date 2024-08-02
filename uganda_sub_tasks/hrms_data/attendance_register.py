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
print(FromDate, ToDate)


def GetAttendanceRegisterData(url, from_date, to_date):     

    SessionId = return_session_id(country="Uganda:HRMS")

    headers = {
        'Authorization': f'Bearer {SessionId}',
        'Content-Type': 'application/json'
    }
    
    attendance_register = pd.DataFrame()

    params = {
        "FromDate": from_date,
        "ToDate": to_date
       }
    response = requests.get(url, headers=headers, json=params)
    print(response)
    if response.status_code == 200:
        data = response.json()
        df =data['result']
        df = pd.DataFrame(df)
        attendance_register = attendance_register.append(df)
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None 
    
    
    print(attendance_register)
    print(attendance_register.columns)

    attendance_register = attendance_register.rename(
        columns= {
            'Emp_Payroll_No':'payroll_no',
            'Emp_Name':'employee_name',
            'ShiftDate':'shift_date',
            'ShiftDay':'shift_day',
            'ShiftName':'shift_name',
            'ShiftStartTime':'shift_start_time',
            'ShiftEndTime':'shift_end_time',
            'ArrTime':'arr_time',
            'DepTime':'dep_time',
            'LateArrMinutes':'late_arr_minutes',
            'EarlyDepMinutes':'early_dep_minutes',
            'LostMinutes':'lost_minutes',
            'WorkedMinutes':'worked_minutes',
            'ClockedMinutes':'clocked_minutes',
            'OT1Minutes':'ot1_minutes',
            'OT2Minutes':'ot2_minutes',
            'AbsentMinutes':'absent_minutes',
            'Remarks':'remarks',
        }
    )
    print(attendance_register)   

    return attendance_register

def update_GetAttendanceRegisterData():
    url = "http://52.71.65.50:8094/api/Attendance/GetAttendanceRegisterData"
    from_date = FromDate
    to_date = ToDate

    dfs_list  = GetAttendanceRegisterData(url, from_date, to_date)

    print('dfs_list')
    print(dfs_list)
    
    dfs_list = dfs_list.set_index(['payroll_no','shift_date'])   
    print("Data Indexed")
    
    upsert(engine=engine,
       df=dfs_list,
       schema='mawingu_staging',
       table_name='source_attendance_register',
       if_row_exists='update',
       create_table=False,
       add_new_columns=True)
    
    print("Data Updated")







    


       