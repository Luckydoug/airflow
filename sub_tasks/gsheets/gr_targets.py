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

def fetch_gr_targets():
    gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
    sh = gc.open_by_key('1U1l1OsifHM15mLX1XVNm_kALGfxXBYDNDtlnDNvPOKs')
    sh = sh[0]
    values = sh.get_all_values()
    sh = pd.DataFrame(values)
    csv_string = sh.to_csv(index=False,header=False)
    sh = pd.read_csv(io.StringIO(csv_string), na_values='')

    drop_table = """truncate table mabawa_dw.google_reviews_targets;"""
    drop_table = pg_execute(drop_table)
    
    sh[sh['country']=='KE'][['branch','target','target_before_nov_2023']].to_sql('google_reviews_targets', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)

    print('google reviews targets pulled')

def create_gr_summary():

    query = """
    refresh materialized view mabawa_mviews.google_reviews_summary
    """

    query = pg_execute(query)

    
# fetch_gr_targets()




