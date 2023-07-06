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


def fetch_source_teams():
    
    data = pd.read_sql("""
    SELECT team_id, lead_id, flags, name, notes, created, updated
    FROM mwangaza.ost_team;
    """, con=connection)

    truncate = """truncate ticketing_staging.source_teams;"""
    truncate = pg_execute(truncate)

    data.to_sql('source_teams', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    return 'something' 

def fetch_source_team_members():
    
    data = pd.read_sql("""
    SELECT team_id, staff_id, flags
    FROM mwangaza.ost_team_member;
    """, con=connection)

    truncate = """truncate ticketing_staging.source_team_members;"""
    truncate = pg_execute(truncate)

    data.to_sql('source_team_members', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    return 'something' 

def create_teams_live():
    
    query = """
    truncate ticketing_dw.dim_teams;
    insert into ticketing_dw.dim_teams
    SELECT team_id, lead_id, flags, team_name, notes, created, updated, staff_id, staff_username, staff_fname, staff_lname, staff_email
    FROM ticketing_staging.v_dim_teams;
    """
    query = pg_execute(query)
    return 'something' 







