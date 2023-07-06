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



from sub_tasks.data.connect_mawingu import (pg_execute, pg_fetch_all, engine)  


def fetch_hr_staff_codes():
    
    gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
    sh = gc.open_by_key('14c0OPHThawt3SbQ9lEk2whGhmzKgfKlW3j7KXhrDaQQ')
    sh = sh[0]
    sh = pd.DataFrame(sh.get_all_records())
    
    query = """truncate mawingu_staging.source_hr_staff_codes;"""
    query = pg_execute(query)

    sh.to_sql('source_hr_staff_codes', con = engine, schema='mawingu_staging', if_exists = 'append', index=False)


# fetch_hr_staff_codes()