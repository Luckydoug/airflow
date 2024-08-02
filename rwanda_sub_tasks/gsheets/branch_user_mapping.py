import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pygsheets
import pandas as pd
from datetime import date
from airflow.models import Variable
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 



from sub_tasks.data.connect_voler import (pg_execute, pg_fetch_all, engine) 


def fetch_branch_user_mappings():
    
    gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
    sh = gc.open_by_key('1SzZP5-BtzhGBkfeI168AeByjjKMm0u_8tUZgJjCmxhQ')
    exec = sh.worksheet_by_title('Executive View (ALL)')
    exec = pd.DataFrame(exec.get_all_records())
    exec.rename (columns = {
                            'Person':'person_name', 
                            'BI Username':'user_name'
                            }
          ,inplace=True)

    query = """truncate voler_staging.source_exec_users;"""
    query = pg_execute(query)

    exec.to_sql('source_exec_users', con = engine, schema='voler_staging', if_exists = 'append', index=False)

    df = sh.worksheet_by_title('Branch Mapping -  RW')
    df = pd.DataFrame(df.get_all_records())
    df.rename (columns = {
                          'Zone':'branch_zone', 
                          'Branch Code':'branch_code',
                          'Branch':'branch_name', 
                          'Branch BI Username':'branch_username',
                          'Email':'branch_email', 
                          'RM':'rm_name',
                          'RM BI Username':'rm_username', 
                          'RM Email':'rm_email',
                          'Senior RM':'senior_rm_name',
                          'SRM BI Username':'srm_username', 
                          'SRM Email':'srm_email',
                          'Field Trainer':'field_trainer',
                          'Field Trainer Email':'field_trainer_email'
                          }
        ,inplace=True)

    query = """truncate voler_staging.source_branch_user_mapping;"""
    query = pg_execute(query)

    df.to_sql('source_branch_user_mapping', con = engine, schema='voler_staging', if_exists = 'append', index=False)

    query1 = """
    update voler_staging.source_branch_user_mapping
    set branch_code = 'NAN'
    where branch_code is null
    """
    
    query1 = pg_execute(query1)
    
def create_dim_branch_user_mapping():

    data = pd.read_sql("""
    SELECT 
        user_name, branch_code
    FROM voler_mviews.v_branch_user_mapping
    where user_name is not null 
    and branch_code is not null
    and user_name <> ''
    and branch_code <> ''
    """, con=engine)

    data = data.drop_duplicates()

    query = """truncate voler_dw.branch_user_mapping;"""
    query = pg_execute(query)
     
    data.to_sql('branch_user_mapping', con = engine, schema='voler_dw', if_exists = 'append', index=False)

# fetch_branch_user_mappings()
# create_dim_branch_user_mapping()