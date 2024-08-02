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

def fetch_routesdata():

     gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
     sh = gc.open_by_key('1U32LqEJ8sy9YUmSP4nsK4HSHPJTEEmtaZJ9to-joZms')
     routes = sh[0]
     routes = pd.DataFrame(routes.get_all_records())
     print(routes)
     print('g-sheet data')

     truncate_table = """truncate mabawa_staging.source_rider_routes;"""
     truncate_table = pg_execute(truncate_table)

     routes.to_sql('source_rider_routes', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)   
     
     print('rider_routes pulled')

# fetch_routesdata()   
       