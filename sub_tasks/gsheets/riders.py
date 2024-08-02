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
# from pangres import upsert, DocsExampleTable
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

def fetch_rider_times():

     gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/opticabi-a4e46c63f7d9.json')
     sh = gc.open_by_key('1pl19z9XHZkXtSyAm5OQyyCes7u99kjXrUo9ZxLmPpzY')
     riders = sh[1]
     riders = pd.DataFrame(riders.get_all_records())

     print('The riders information has been fetched')
     print(riders.shape)

     # Drop the rows with blank dates
     print(riders)
     riders = riders[riders['Date']!=""]

     # Change the date column to datetime
     riders['Date'] = pd.to_datetime(riders['Date'], dayfirst=True)
     print('The rows with blank dates have been dropped successfully')
     print(riders.shape)

     # Renaming the columns
     riders.rename(columns = {'Date':'trip_date', 
                       'Trip':'trip',
                       'Westlands time from Hq':'westlands_time_from_hq',
                       'Westlands time back at Hq':'westlands_time_back_at_hq', 
                       'Karen time from Hq':'karen_time_from_hq', 
                       'Karen time back at Hq':'karen_time_back_at_hq',
                       'Eastlands time from Hq':'eastlands_time_from_hq',
                       'Eastlands time back at Hq':'eastlands_time_back_at_hq',
                       'Thika time from Hq':'thika_time_from_hq',
                       'Thika time back at Hq':'thika_time_back_at_hq',
                       'Mombasa time from Hq':'mombasa_time_from_hq',
                       'Mombasa time back at Hq':'mombasa_time_back_at_hq',
                       'Rongai time from Hq':'rongai_time_from_hq',
                       'Rongai time back at Hq':'rongai_time_back_at_hq',
                       'Upcountry':'upcountry',
                       'CBD time from Hq':'cbd_time_from_hq',
                       'CBD time back at Hq':'cbd_time_back_at_hq',
                       'CBD2 time from Hq':'cbd2_time_from_hq',
                       'CBD2 time back at Hq':'cbd2_time_back_at_hq',
                       'Rider 5 - Thika Town from HQ':'rider5_thikatown_from_hq',
                       'Rider 5 - Thika Town time back at Hq':'rider5_thikatown_time_back_at_hq'
                       },inplace=True)

     print('The columns have been renamed successfully')

     # Truncate the existing table before appending the new table
     truncate_table = """truncate mabawa_staging.source_riders;"""
     truncate_table = pg_execute(truncate_table)

     riders.to_sql('source_riders', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)   
     
     print('Riders information has been successfully appended')

# fetch_rider_times()
