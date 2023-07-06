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


def fetch_itr_issues():
    
    gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
    sh = gc.open_by_key('1cnpNo85Hncf9cdWBfkQ1dn0TYnGfs-PjVGp1XMjk2Wo')
    wk1 = sh[2]
    wk1 = pd.DataFrame(wk1.get_all_records())

    wk1.rename (columns = {
                            'DATE':'issue_date', 
                            '  ITR Number':'itr_no', 
                            'ISSUE DESCRIPTION':'issue_desc', 
                            'DEPARTMENT':'issue_dept',
                            'DEPARTMENT 2':'issue_dept_2',
                            'DEPARTMENT 3':'issue_dept_3',
                            'DEPARTMENT 4':'issue_dept_4'
                            }
          ,inplace=True)

    #wk1['issue_date'] = pd.to_datetime(wk1['issue_date'])
    #wk1['itr_no'] = pd.to_numeric(wk1['itr_no'])

    truncate = """truncate mabawa_staging.source_itr_issues;"""
    truncate = pg_execute(truncate)

    wk1.to_sql('source_itr_issues', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
     
    """
     wk1 = wk1.dropna()

     wk1 = wk1.set_index(['branch_code','cutoff_type'])

     upsert(engine=engine,
       df=wk1,
       schema='mabawa_staging',
       table_name='source_branchstock_cutoffs',
       if_row_exists='update',
       create_table=False)
    """
    
    return 'something' 

def create_dim_itr_issues():

  query="""
  truncate mabawa_dw.dim_itr_issues;
  insert into mabawa_dw.dim_itr_issues
  SELECT 
    to_date(issue_date, 'DD-MM-YYYY') as issue_date, itr_no, issue_desc, 
    issue_dept, issue_dept_2, issue_dept_3, issue_dept_4
  FROM mabawa_staging.source_itr_issues;
  """
  query = pg_execute(query)
  return "Created"