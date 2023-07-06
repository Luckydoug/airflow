import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pandas as pd
from datetime import date
from airflow.models import Variable
from sqlalchemy import create_engine
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 

from sub_tasks.data.connect import (pg_execute, engine, connection) 


def fetch_source_depts():
    
    data = pd.read_sql("""
    SELECT id, pid, tpl_id, sla_id, schedule_id, email_id, autoresp_email_id, manager_id, flags, name, signature, ispublic, group_membership, ticket_auto_response, message_auto_response, `path`, updated, created
    FROM mwangaza.ost_department;
    """, con=connection)

    truncate = """truncate ticketing_staging.source_departments;"""
    truncate = pg_execute(truncate)

    data.to_sql('source_departments', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    
    return 'something'
    
def fetch_emails():

    data = pd.read_sql("""
    SELECT email_id, noautoresp, priority_id, dept_id, topic_id, email, name, userid, userpass, mail_active, mail_host, mail_protocol, mail_encryption, mail_folder, mail_port, mail_fetchfreq, mail_fetchmax, mail_archivefolder, mail_delete, mail_errors, mail_lasterror, mail_lastfetch, smtp_active, smtp_host, smtp_port, smtp_secure, smtp_auth, smtp_auth_creds, smtp_userid, smtp_userpass, smtp_spoofing, notes, created, updated
    FROM mwangaza.ost_email;
    """, con=connection)
    
    truncate = """truncate ticketing_staging.source_emails;"""
    truncate = pg_execute(truncate)

    data.to_sql('source_emails', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    return 'something' 

def create_dept_live():

    query = """
    truncate ticketing_dw.dim_depts;
    insert into ticketing_dw.dim_depts
    SELECT dept_id, pid, tpl_id, dept_sla_id, schedule_id, email_id, email, autoresp_email_id, manager_id, flags, dept_name, dept_signature, ispublic, group_membership, ticket_auto_response, message_auto_response, "path", updated, created
    FROM ticketing_staging.v_dim_depts;
    """
    query = pg_execute(query)

    return 'something' 

