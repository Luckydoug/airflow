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


def fetch_holidays():
    
    gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
    sh = gc.open_by_key('1yRlrhjTOv94fqNpZIiA9xToKbDi9GaWkdoGrG4FcdKs')
    sh = sh[0]
    sh = pd.DataFrame(sh.get_all_records())
    sh.rename (columns = {
                            'Date':'holiday_date', 
                            'Holiday':'holiday_name'
                            }
          ,inplace=True)

    sh = sh.set_index(['holiday_date'])
    
    upsert(engine=engine,
       df=sh,
       schema='mabawa_dw',
       table_name='dim_holidays',
       if_row_exists='update',
       create_table=False)