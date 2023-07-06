import sys
sys.path.append(".")

#import libraries
import json
import pytz
import psycopg2
import requests
import datetime
import pandas as pd
import businesstimedelta
import holidays as pyholidays
from datetime import date
from airflow.models import Variable
from sqlalchemy import create_engine
from workalendar.africa import Kenya
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 

from sub_tasks.data.connect import (pg_execute, engine, connection)

def fetch_source_tickets_thread():
    
    data = pd.read_sql("""
    SELECT id, object_id, object_type, extra, lastresponse, lastmessage, created
    FROM mwangaza.ost_thread;
    """, con=connection)

    data = data.set_index('id')

    upsert(engine=engine,
       df=data,
       schema='ticketing_staging',
       table_name='source_tickets_thread',
       if_row_exists='update',
       create_table=False)

    #data.to_sql('source_tickets_thread', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    return 'something' 

def fetch_source_tickets_thread_entry():
    
    data = pd.read_sql("""
    SELECT id, pid, thread_id, staff_id, user_id, `type`, flags, poster, editor, editor_type, source, title, body, format, ip_address, extra, recipients, created, updated
    FROM mwangaza.ost_thread_entry
    where cast(created as date) >= cast( (now()-3) as date)
    """, con=connection)

    data = data.set_index('id')

    upsert(engine=engine,
       df=data,
       schema='ticketing_staging',
       table_name='source_tickets_thread_entry',
       if_row_exists='update',
       create_table=False)
       
    #data.to_sql('source_tickets_thread', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    return 'something' 

def create_source_ticket_response():
    
    query = """
    truncate ticketing_staging.ticket_response;
    insert into ticketing_staging.ticket_response
    SELECT "number", "type", ticket_created, first_reply
    FROM ticketing_staging.v_ticket_response;
    """

    query = pg_execute(query)
    return 'something' 

def create_ticket_response():
    
    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)
    
    df = pd.read_sql("""
    SELECT
        t.ticket_id, t.ticket_pid, ticket_no, ticket_subject, list_id, user_id, user_name, user_staff_uname, user_dept_id, 
        user_dept, user_email_id, status_id, dept_id, sla_id, topic_id, staff_id, team_id, email_id, lock_id, flags, 
        sort, ip_address, ticket_source, source_extra, isoverdue, isanswered, duedate, est_duedate, reopened, closed, 
        is_open, is_outgoing, lastupdate, created, updated, tr.first_reply 
    FROM ticketing_dw.fact_tickets t 
    left join ticketing_staging.ticket_response tr on t.ticket_no = tr."number" 
    """, con=engine)

    
    df['created'] = pd.to_datetime(df['created'])
    df['first_reply'] = pd.to_datetime(df['first_reply'])

    workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(18),
        working_days=[0,1, 2, 3, 4])

    saturday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(16),
        working_days=[5])

    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    cal = Kenya()
    hl = data.values.tolist()
    #print(hl)
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    businesshrs = businesstimedelta.Rules([workday, saturday, holidays])

    def Busmin(start, end):
        if end>=start:
            return float(businesshrs.difference(start,end).hours)*float(60)+float(businesshrs.difference(start,end).seconds)/float(60)
        else:
            return ""

    df['first_response_time']=df.apply(lambda row: Busmin(row['created'], row['first_reply']), axis=1)
    df['first_response_time'] = pd.to_numeric(df['first_response_time'])

    truncate = """drop table ticketing_dw.ticket_response;"""
    truncate = pg_execute(truncate)

    df.to_sql('ticket_response', con = engine, schema='ticketing_dw', if_exists = 'append', index=False)
    return 'something' 