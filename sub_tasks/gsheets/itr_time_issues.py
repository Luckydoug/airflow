import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pygsheets
import pandas as pd
from airflow.models import Variable
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 
from datetime import date


from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine) 


def fetch_itr_time_issues():
    
    gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
    sh = gc.open_by_key('1cnpNo85Hncf9cdWBfkQ1dn0TYnGfs-PjVGp1XMjk2Wo')
    wk1 = sh[3]
    wk1 = pd.DataFrame(wk1.get_all_records())

    wk1.rename (columns = {
                            'DATE':'issue_date', 
                            'START TIME':'issue_start_time', 
                            'END TIME':'issue_end_time', 
                            'ISSUE DESCRIPTION':'issue_desc',
                            'ISSUE CATEGORY':'issue_category',
                            'DEPARTMENT':'issue_dept'
                            }
          ,inplace=True)

    #wk1['issue_date'] = pd.to_datetime(wk1['issue_date'])
    #wk1['itr_no'] = pd.to_numeric(wk1['itr_no'])

    truncate = """truncate mabawa_staging.source_itr_time_issues;"""
    truncate = pg_execute(truncate)

    wk1.to_sql('source_itr_time_issues', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    
    return 'something' 

def create_dim_itr_time_issues():

    query = """
    truncate mabawa_dw.dim_itr_time_issues;
    insert into mabawa_dw.dim_itr_time_issues
    SELECT to_date(issue_date, 'DD-MM-YYYY') as issue_date, 
    issue_start_time::time, 
    issue_end_time::time, issue_desc, issue_category, issue_dept
    FROM mabawa_staging.source_itr_time_issues;
    """
    query = pg_execute(query)
    
    return "Created"