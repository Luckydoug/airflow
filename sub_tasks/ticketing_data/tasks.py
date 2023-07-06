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
from datetime import date
import pandas as pd

from sub_tasks.data.connect import (pg_execute, engine, connection) 


def fetch_source_tasks():
    
    data = pd.read_sql("""
    SELECT id, object_id, object_type, `number`, dept_id, staff_id, team_id, lock_id, flags, duedate, closed, created, updated
    FROM mwangaza.ost_task;
    """, con=connection)

    truncate = """truncate ticketing_staging.source_tasks;"""
    truncate = pg_execute(truncate)

    data.to_sql('source_tasks', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    return 'something' 

def fetch_source_task_data():
    
    data = pd.read_sql("""
    SELECT task_id, title
    FROM mwangaza.ost_task__cdata;
    """, con=connection)

    truncate = """truncate ticketing_staging.source_task_data;"""
    truncate = pg_execute(truncate)

    data.to_sql('source_task_data', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    return 'something' 

def create_live_tasks():
    
    query = """
    truncate ticketing_dw.dim_tasks;
    insert into ticketing_dw.dim_tasks
    SELECT task_id, task_title, object_id, object_type, ticket_no, dept_id, staff_id, team_id, lock_id, flags, duedate, closed, created, updated
    FROM ticketing_staging.v_dim_tasks;

    """
    query = pg_execute(query)
    return 'something' 






