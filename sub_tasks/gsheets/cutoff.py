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


def fetch_cutoffs():
    
     gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
     sh = gc.open_by_key('1ggKhfX9eUHPlr26k0k6s5f1eDoGIp--MU-phya7O1lg')
     wk1 = sh[0]
     wk1 = pd.DataFrame(wk1.get_all_records())
     wk1.rename (columns = {'Area':'cutoff_dept', 
                       'Status':'cut_off_status', 
                       'SLA':'cut_off_sla', 
                        'Target':'cut_off_target'}
            ,inplace=True)
     truncate_table = """truncate mabawa_staging.landing_dept_cutoff;"""
     truncate_table = pg_execute(truncate_table)

     wk1.to_sql('landing_dept_cutoff', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
     return 'something' 

def update_cutoffs():

     data = pd.read_sql("""
     SELECT distinct
          (case when cutoff_dept = 'Control Room' then 'Control'
          else cutoff_dept end) as cutoff_dept, 
          (case when cut_off_status = 'Control Room' then 'Control'
          else cut_off_status end) as cut_off_status, 
          cut_off_sla, cut_off_target
     FROM mabawa_staging.landing_dept_cutoff
     order by cutoff_dept
     """, con=engine)

     truncate_table ="""truncate mabawa_staging.source_dept_cutoff;"""
     truncate_table = pg_execute(truncate_table)
     data.to_sql('source_dept_cutoff', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
     return 'something'
# update_cutoffs()
def create_cutoffs_live():

    data = pd.read_sql("""
    SELECT 
        (case when cutoff_dept = 'Designer Store' then 'Designer'
        else cutoff_dept end) as cutoff_dept, 
        (case when cut_off_status = 'Designer Store' then 'Designer'
        else cut_off_status end) as cut_off_status,
        cut_off_sla, cut_off_target
    FROM mabawa_staging.source_dept_cutoff;
    """, con=engine)

    print("Fetched Data")

    drop_table ="""drop table mabawa_dw.dim_dept_cutoff;"""
    drop_table = pg_execute(drop_table)

    print("Dropped Table")

    data.to_sql('dim_dept_cutoff', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    print("Inserted Cut Offs")
    return 'something'

# fetch_cutoffs()
# update_cutoffs()
# create_cutoffs_live()