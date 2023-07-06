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



from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine) 


def fetch_branch_tiers():
    
    gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
    sh = gc.open_by_key('1CkdkB13PLFzekMwLlyf4_TD1-FnSIbvtWUTxinU5PVY')
    sh = sh[0]
    sh = pd.DataFrame(sh.get_all_records())
    sh.rename (columns = {
                            'BRANCH CODE':'branch_code', 
                            'TIER':'tier',
                            'County':'county',
                            'Region':'region'
                            }
          ,inplace=True)
    
    query = """truncate mabawa_staging.source_branch_tiers;"""
    query = pg_execute(query)

    sh.to_sql('source_branch_tiers', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)

    query1 = """
    update mabawa_staging.source_branch_tiers
    set branch_code = 'NAN'
    where branch_code is null
    """
    query1 = pg_execute(query1)
    
def create_dim_branches():

    df = pd.read_sql("""
    SELECT 
        branch_code, branch_name, tier, county, region
    FROM mabawa_mviews.v_dim_branch;
    """, con=engine)

    query = """truncate table mabawa_dw.dim_branches;"""
    query = pg_execute(query)

    df.to_sql('dim_branches', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)