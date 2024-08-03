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
import pygsheets

from sub_tasks.data.connect_mawingu import (pg_execute, pg_fetch_all, engine) 


def fetch_insurance_errors_to_drop():

     gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
     sh = gc.open_by_key('16oFwly1sKlX48xL3LXLZKfHv7ClI-84o7DZzmlxCZeE')
     sh = sh[1]
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

     query = """truncate mawingu_staging.source_drop_insurance_errors;"""
     query = pg_execute(query)

     sh['date'] = pd.to_datetime(sh['date'],dayfirst=True,errors='coerce')
    #  sh['order_no'] = sh['order_no'].apply("{:.0f}".format).astype(str)


     sh[['date','order_no','branch','error_to_transfer_to','reason']].to_sql('source_drop_insurance_errors', con = engine, schema='mawingu_staging', if_exists = 'append', index=False)


# fetch_insurance_errors_to_drop()