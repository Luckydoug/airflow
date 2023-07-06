import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pandas as pd
import math
from pandas.io.json._normalize import nested_to_record 
from sqlalchemy import create_engine
from airflow.models import Variable
from pandas.io.json._normalize import nested_to_record 
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from datetime import date
import datetime
import pytz
import io
import businesstimedelta
import pandas as pd
import holidays as pyholidays
from workalendar.africa import Kenya
import pygsheets

from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine) 
from sub_tasks.api_login.api_login import(login)

def fetch_insurance_errors_to_drop():

     gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
     sh = gc.open_by_key('16oFwly1sKlX48xL3LXLZKfHv7ClI-84o7DZzmlxCZeE')
     sh = sh[0]
     values = sh.get_all_values()
     sh = pd.DataFrame(values)
     csv_string = sh.to_csv(index=False,header=False)
     sh = pd.read_csv(io.StringIO(csv_string), na_values='')

     sh.rename (columns = {
                    'Date':'date', 
                    'Order Number':'order_no',
                    'Branch':'branch', 
                    'Error Transfer to (Branch Code)':'error_to_transfer_to',
                    'Reason':'reason'
                         }
            ,inplace=True)   

     print('renamed successfully')

     query = """truncate mabawa_staging.source_drop_insurance_errors;"""
     query = pg_execute(query)

     sh['date'] = pd.to_datetime(sh['date'],dayfirst=True)
     sh['order_no'] = sh['order_no'].apply("{:.0f}".format).astype(str)


     sh[['date','order_no','branch','error_to_transfer_to','reason']].to_sql('source_drop_insurance_errors', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)

     # truncate_table = """truncate mabawa_staging.source_npsreviews_with_issues;"""
     # truncate_table = pg_execute(truncate_table)

     # wk1.to_sql('source_npsreviews_with_issues', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)   
     
     # print('orders with issues pulled')



# fetch_insurance_errors_to_drop()