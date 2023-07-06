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


def fetch_source_staff():
    
    data = pd.read_sql("""
    SELECT staff_id, dept_id, role_id, username, firstname, lastname, passwd, backend, email, phone, phone_ext, mobile, signature, lang, timezone, locale, notes, isactive, isadmin, isvisible, onvacation, assigned_only, show_assigned_tickets, change_passwd, max_page_size, auto_refresh_rate, default_signature_type, default_paper_size, extra, permissions, created, lastlogin, passwdreset, updated
    FROM mwangaza.ost_staff;
    """, con=connection)

    truncate = """truncate ticketing_staging.source_ticket_staff;"""
    truncate = pg_execute(truncate)

    data.to_sql('source_ticket_staff', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    return 'something' 

def fetch_source_users():
    
    data = pd.read_sql("""
    SELECT id, org_id, default_email_id, status, name, created, updated
    FROM mwangaza.ost_user;
    """, con=connection)

    truncate = """truncate ticketing_staging.source_users;"""
    truncate = pg_execute(truncate)

    data.to_sql('source_users', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    return 'something' 

def fetch_source_user_email():
    
    data = pd.read_sql("""
    SELECT id, user_id, flags, address
    FROM mwangaza.ost_user_email;
    """, con=connection)

    truncate = """truncate ticketing_staging.source_user_email;"""
    truncate = pg_execute(truncate)

    data.to_sql('source_user_email', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    return 'something' 

def create_users_live():
    
    query = """
    truncate ticketing_dw.dim_users;
    insert into ticketing_dw.dim_users
    SELECT id, org_id, default_email_id, user_email, status, user_name, created, updated
    FROM ticketing_staging.v_dim_users;

    """
    query = pg_execute(query)
    return 'something' 

def create_staff_live():
    
    query = """
    truncate ticketing_dw.dim_staff;
    insert into ticketing_dw.dim_staff
    SELECT staff_id, dept_id, role_id, user_name, firstname, lastname, email, phone, phone_ext, mobile, isactive, isadmin, isvisible, onvacation, assigned_only, show_assigned_tickets, permissions, created, lastlogin, updated
    FROM ticketing_staging.v_dim_staff;
    """
    query = pg_execute(query)
    return 'something' 








