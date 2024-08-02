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


def fetch_orders_with_issues():

     gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
     sh = gc.open_by_key('1cnpNo85Hncf9cdWBfkQ1dn0TYnGfs-PjVGp1XMjk2Wo')
     wk1 = sh[0]
     wk1 = pd.DataFrame(wk1.get_all_records())
     wk1.rename (columns = {'DATE':'issue_date', 
                       'ORDER NUMBER':'order_no', 
                       'ISSUE CATEGORY':'issue_category', 
                       'ISSUE DESCRIPTION':'issue_desc', 
                       'DEPARTMENT':'issue_dept',
                       'SURFACING':'issue_dept',
                       'DESIGNER STORE':'issue_dept'}
            ,inplace=True)
     
     #data['issue_date'] = pd.to_datetime(data['issue_date'])

     truncate_table = """drop table mabawa_staging.landing_orders_with_issues;"""
     truncate_table = pg_execute(truncate_table)

     wk1.to_sql('landing_orders_with_issues', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
     
     return 'something' 


def update_orders_with_issues():
    
     data = pd.read_sql("""
     SELECT distinct 
     to_date(issue_date,'dd-MM-yy') as issue_date, order_no::numeric, issue_category, issue_desc, issue_dept
     FROM mabawa_staging.landing_orders_with_issues
     where issue_date is not null 
     and length(issue_date) > 0
     """, con=engine)

     print("Data Pulled")

     truncate = """truncate mabawa_staging.source_orders_with_issues;"""
     truncate = pg_execute(truncate)

     print("Truncated Table")

     data.to_sql('source_orders_with_issues', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
     print("Inserted Data")
     
     return 'something'

def orders_with_issues_live():

    orders = pd.read_sql("""
    SELECT 
     issue_date, order_no, so.doc_entry ,issue_category, issue_desc, 
     (case when issue_dept = 'CONTROL ROOM' then 'CONTROL' else issue_dept END) as issue_dept
     FROM mabawa_staging.source_orders_with_issues o
     left join mabawa_staging.source_orderscreen so on o.order_no = so.doc_no 
    """, con=engine)

    truncate_live = """drop table mabawa_dw.dim_orders_with_issues;"""
    truncate_live = pg_execute(truncate_live)
    orders.to_sql('dim_orders_with_issues', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    return 'something'