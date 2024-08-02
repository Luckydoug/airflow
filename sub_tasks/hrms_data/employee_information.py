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


def GetEmployeeInformation(url):     

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
            'Emp_Code':'emp_code',
            'Emp_Payroll_No':'payroll_no',
            'Emp_Title':'emp_title',
            'Emp_First_Name':'employee_first_name',
            'Emp_Middle_Name':'employee_middle_name',
            'Emp_Last_Name':'employee_last_name',
            'Emp_Name':'emp_name',
            'Gender':'gender',
            'DateOfJoining':'date_of_joining',
            'DepartmentCode':'department_code',
            'DepartmentName':'department_name',
            'Sub_DepartmentCode':'sub_department_code',
            'Sub_DepartmentName':'sub_department_name',
            'LocationCode':'location_code',
            'LocationName':'location_name',
            'DesignationCode':'designation_code',
            'DesignationName':'designation_name',
            'Employment_Type':'employment_type',
            'Birth_Date' : 'birth_date' ,
            'ReligionCode': 'religion_code', 
            'ReligionName':'religion_name',
            'TribeName':'tribe_name', 
            'TribeCode':'tribe_code',
            'Marital_Status': 'marital_status',
            'Anniversary_Date':'anniversary_date',
            'Emp_Children':'emp_children',
            'Emp_Dependents':'emp_dependents',
            'NationalityCode':'nationality_code',
            'NationalityName':'nationality_name',
            'Mother_Tongue':'mother_tongue', 
            'EthnicityCode':'ethinicity_code',
            'EthnicityName':'ethinicity_name',
            'ClassCode':'classcode', 
            'ClassName':'classname', 
            'CategoryCode':'category_code',
            'CategoryName':'category_name',
            'Week_Off_Type':'week_off_type',
            'Days':'days', 
            'Holiday_Calendar_Code':'holiday_calendar_code',
            'Holiday_Calendar_Name':'holiday_calendar_name',
            'OrganizationUnitCode':'organization_unit_code',
            'OrganizationUnitName':'organization_unit_name',
            'Probation':'probation',
            'Contract':'contract',
            'PositionCode':'position_code',
            'PositionName':'position_name',
            'MasterOneCode':'masteronecode', 
            'MasterOneName':'masteronename', 
            'MasterTwoCode':'mastertwocode', 
            'MasterTwoName':'mastertwoname',
            'Residing_In_Metro_City':'residing_in_metro_city'
            }
            )
    print(employee_information.columns)    

    return employee_information

def update_GetEmployeeInformation():
    url = " http://52.71.65.50:8094/API/Employee/GetEmployeeInformation"
    dfs_list  = GetEmployeeInformation(url)

    query = """truncate mabawa_staging.source_employee_information;"""
    query = pg_execute(query)
    dfs_list.to_sql('source_employee_information', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)      
    
    print("Data Updated")
    

      