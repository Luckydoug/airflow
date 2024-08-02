import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pandas as pd
from pandas.io.json._normalize import nested_to_record 
from sqlalchemy import create_engine
from airflow.models import Variable
from pandas.io.json._normalize import nested_to_record 
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from datetime import date
import datetime
import pytz
import businesstimedelta
import pandas as pd
import holidays as pyholidays
from workalendar.africa import Kenya
import pygsheets
from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine) 
# from sub_tasks.api_login.api_login import(login)


def fetch_time_with_issues():
    
     gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
     sh = gc.open_by_key('1cnpNo85Hncf9cdWBfkQ1dn0TYnGfs-PjVGp1XMjk2Wo')
     wk1 = sh[1]
     wk1 = pd.DataFrame(wk1.get_all_records())
     wk1.rename (columns = {'DATE':'issue_date', 
                       'START TIME':'issue_start_time', 
                       'END TIME':'issue_end_time', 
                        'ISSUE DESCRIPTION':'issue_desc',
                       'ISSUE CATEGORY':'issue_category',  
                       'DEPARTMENT':'issue_dept'}
            ,inplace=True)

     print("Data Pulled")

     #data['issue_date'] = pd.to_datetime(data['issue_date'])

     truncate = """drop table mabawa_staging.landing_time_with_issues;"""
     truncate = pg_execute(truncate)

     print("Truncated Table")

     wk1.to_sql('landing_time_with_issues', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
     print("Data Inserted")

     return 'something' 

def update_time_with_issues():
    
     data = pd.read_sql("""
     SELECT distinct
          to_date(issue_date,'dd-MM-yy') as issue_date, cast(replace(issue_start_time,'.',':')as time) as issue_start_time, 
          cast(replace(issue_end_time,'.',':')as time) as issue_end_time, issue_desc, issue_category, issue_dept
     FROM mabawa_staging.landing_time_with_issues;
     """, con=engine)

     print("Data Pulled")

     truncate = """truncate mabawa_staging.source_time_with_issues;"""
     truncate = pg_execute(truncate)

     print("Truncated Table")

     data.to_sql('source_time_with_issues', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
     print("Inserted table")

     return 'something'

def time_with_issues_live():

    time = pd.read_sql("""
    SELECT distinct
        issue_date, issue_start_time, issue_end_time, issue_desc, issue_category, 
        (case when issue_dept = 'CONTROL ROOM' then 'CONTROL' else issue_dept END) as issue_dept
    FROM mabawa_staging.source_time_with_issues;
    """, con=engine)

    print("Data Pulled")

    truncate = """truncate mabawa_dw.dim_time_with_issues;"""
    truncate = pg_execute(truncate)

    print("Truncated Table")

    time.to_sql('dim_time_with_issues', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    print("Data Inserted")
    
    return 'something'

# update_time_with_issues()