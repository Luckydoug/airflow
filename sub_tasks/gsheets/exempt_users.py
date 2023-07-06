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



from sub_tasks.data.connect import (pg_execute, engine) 


def fetch_exempt_users():
    
    gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
    sh = gc.open_by_key('1PSMyFWboNvoHCDwgaWGXj84R6T9f1jQhYSs_HbUrAaE')
    sh = sh[0]
    sh = pd.DataFrame(sh.get_all_records())
    sh.rename (columns = {
                            'user':'user'
                            }
          ,inplace=True)
    
    query = """truncate mabawa_staging.source_exempt_users;"""
    query = pg_execute(query)

    sh.to_sql('source_exempt_users', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)

    query2 = """
    INSERT INTO mabawa_staging.source_exempt_users ("user") VALUES('majulu');
    INSERT INTO mabawa_staging.source_exempt_users ("user") VALUES('dmwaiboro');
    INSERT INTO mabawa_staging.source_exempt_users ("user") VALUES('pwamukota');
    INSERT INTO mabawa_staging.source_exempt_users ("user") VALUES('mogutu');
    """
    query2 = pg_execute(query2)