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


def fetch_source_sla():
    
    data = pd.read_sql("""
    SELECT id, schedule_id, flags, grace_period, name, notes, created, updated
    FROM mwangaza.ost_sla;
    """, con=connection)

    truncate = """truncate ticketing_staging.source_sla;"""
    truncate = pg_execute(truncate)

    data.to_sql('source_sla', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    return 'something' 






