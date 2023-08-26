import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pygsheets
import io
import pandas as pd
from datetime import date
from airflow.models import Variable
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 

from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine) 

def fetch_sop_branch_info():
    
    gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
    sh = gc.open_by_key('1fn8yaI3-1X6uq4Z_Vydxq3vl3F5jLryp09E6po_TJAI')
    sh = sh[0]
    values = sh.get_all_values()
    sh = pd.DataFrame(values)
    csv_string = sh.to_csv(index=False,header=False)
    sh = pd.read_csv(io.StringIO(csv_string), na_values='')

    
    sh.rename(columns={
        'Branch Name':'branch',
        'Branch Code':'branch_code',
        'Srm':'srm',
        'Rm':'rm'
            }, inplace = True)

    query = """truncate mabawa_staging.sop_branch_info;"""
    query = pg_execute(query)

    sh[['branch','branch_code','srm','rm']].to_sql('sop_branch_info', con=engine, schema='mabawa_staging', if_exists = 'append', index=False)


def fetch_sop():
    gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
    sh = gc.open_by_key('1fn8yaI3-1X6uq4Z_Vydxq3vl3F5jLryp09E6po_TJAI')
    sh = sh[2]
    sh = pd.DataFrame(sh.get_all_records())

    sh.rename(columns={
        'Date':'sop_date',
        'Audit Person':'audit_person',
        'SRM Name':'srm_name',
        'RM Name':'rm_name',
        'Branch':'branch',
        'SOP':'sop',
        'No of times':'no_of_times',
        'SAP Count':'sap_count',
        '%_ge Non-compliance':'perc_non_compliance',
        'Order Number (If any)':'order_number',
        'Name of Non-compliant Staff':'name_of_non_compliant_staff',
        'Remark on Morning Cleaning':'remark_on_morning_cleaning',
        'Time of Check':'time_of_check',
        'Designation':'designation',
        'Name of Branch Staff':'name_of_branch_staff',
        'Time of Late Reporting':'time_of_late_reporting',
        'Allocated Lunch Timings':'allocated_lunch_timings',
        'Time gone for Lunch':'time_gone_for_lunch',
        'Time of Unauthorized Exit':'time_of_unauthorized_exit',
        'Time of Returning':'time_of_returning',
        'Time of Late Main Door Opening':'time_of_late_opening',
        'Time of Early Closure':'time_of_early_closure'
        },inplace=True)
    
    cols = ['sop_date', 'audit_person','srm_name', 'rm_name', 'branch', 'sop', 'no_of_times',
            'sap_count', 'perc_non_compliance', 'order_number',
            'name_of_non_compliant_staff', 'remark_on_morning_cleaning',
            'time_of_check', 'designation', 'name_of_branch_staff',
            'time_of_late_reporting', 'allocated_lunch_timings',
            'time_gone_for_lunch', 'time_of_unauthorized_exit', 'time_of_returning',
            'time_of_late_opening', 'time_of_early_closure']

    query = """truncate mabawa_staging.source_sop;"""
    query = pg_execute(query)

    sh['sop_date'] = pd.to_datetime(sh['sop_date'], dayfirst=True, errors='coerce')

    sh[cols].to_sql('source_sop', con=engine, schema='mabawa_staging', if_exists = 'append', index=False)

    


