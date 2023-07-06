from os import truncate
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
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from datetime import datetime
import pandas as pd

from sub_tasks.data.connect import (pg_execute, engine) 


def get_start_time():

    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    new_dt_string = pd.to_datetime(dt_string)
    data = [['time',new_dt_string]]
    df = pd.DataFrame(data,columns=['Label','Time'])

    truncate = """truncate ticketing_dw.ticketing_runtime;"""
    truncate = pg_execute(truncate)

    df.to_sql('ticketing_runtime', con = engine, schema='ticketing_dw', if_exists = 'append', index=False)
    return 'something' 