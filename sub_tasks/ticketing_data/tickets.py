import sys
sys.path.append(".")

#import libraries
import pytz
import json
import psycopg2
import requests
import datetime
import pandas as pd
from datetime import date
from airflow.models import Variable
from sqlalchemy import create_engine
from pangres import upsert, DocsExampleTable
from pandas.io.json._normalize import nested_to_record 
from sqlalchemy import create_engine, text, VARCHAR



import businesstimedelta
import pandas as pd
import holidays as pyholidays
from workalendar.africa import Kenya

from sub_tasks.data.connect import (pg_execute, engine, connection) 


def fetch_source_tickets():
    
    data = pd.read_sql("""
    SELECT ticket_id, ticket_pid, `number`, user_id, user_email_id, status_id, dept_id, sla_id, topic_id, staff_id, team_id, email_id, lock_id, flags, sort, ip_address, source, source_extra, isoverdue, isanswered, duedate, est_duedate, reopened, closed, lastupdate, created, updated
    FROM mwangaza.ost_ticket
    where cast(created as date) >= cast( (now()-3) as date)
    """, con=connection)
    
    data = data.set_index('ticket_id')

    upsert(engine=engine,
       df=data,
       schema='ticketing_staging',
       table_name='source_tickets',
       if_row_exists='update',
       create_table=False)

    # truncate = """truncate ticketing_staging.source_tickets;"""
    # truncate = pg_execute(truncate)

    # data.to_sql('source_tickets', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    return 'something' 

def fetch_source_ticket_data():
    
    data = pd.read_sql("""
    SELECT ticket_id, topic_name, subject, priority
    FROM mwangaza.ost_ticket__cdata;
    """, con=connection)

    truncate = """truncate ticketing_staging.source_ticketdata;"""
    truncate = pg_execute(truncate)

    data.to_sql('source_ticketdata', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    return 'something' 

def fetch_source_ticket_statuses():
    
    data = pd.read_sql("""
    SELECT id, name, state, mode, flags, sort, properties, created, updated
    FROM mwangaza.ost_ticket_status;
    """, con=connection)

    truncate = """truncate ticketing_staging.source_ticket_statuses;"""
    truncate = pg_execute(truncate)

def fetch_source_ticketlist():
    
    data = pd.read_sql("""
    SELECT id, name, name_plural, sort_mode, masks, `type`, configuration, notes, created, updated
    FROM mwangaza.ost_list;
    """, con=connection)

    truncate = """truncate ticketing_staging.source_ticketlist;"""
    truncate = pg_execute(truncate)

    data.to_sql('source_ticketlist', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    return 'something' 

def fetch_source_ticketlist_items():
    
    data = pd.read_sql("""
    SELECT id, list_id, status, value, extra, sort, properties
    FROM mwangaza.ost_list_items
    """, con=connection)

    truncate = """truncate ticketing_staging.source_ticketlist_items;"""
    truncate = pg_execute(truncate)

    data.to_sql('source_ticketlist_items', con = engine, schema='ticketing_staging', if_exists = 'append', index=False)
    return 'something' 

def create_tickets_live():
    
    query = """
    truncate ticketing_dw.fact_tickets;
    insert into ticketing_dw.fact_tickets
    SELECT ticket_id, ticket_pid, ticket_no, ticket_subject, list_id, user_id, user_name, user_staff_uname, user_dept_id, user_dept, user_email_id, user_email, status_id, dept_id, email, sla_id, topic_id, staff_id, team_id, email_id, lock_id, flags, sort, ip_address, ticket_source, source_extra, isoverdue, isanswered, duedate, est_duedate, reopened, closed, is_open, is_outgoing, lastupdate, created, updated
    FROM ticketing_staging.v_fact_tickets_2;
    """
    query = pg_execute(query)
    return 'something' 

def update_fact_tickets():
    
    query = """
    update ticketing_dw.fact_tickets t 
    set is_outgoing = 1 
    where user_email in (SELECT distinct email 
    FROM ticketing_staging.source_emails)
    """
    query = pg_execute(query)

    query1 = """
    update ticketing_dw.fact_tickets t 
    set isanswered  = 1 
    where status_id = 7
    """
    query1 = pg_execute(query1)


    query2 = """
    update ticketing_dw.fact_tickets t 
    set is_open = 0
    where created::date <= '2022-05-31'
    and dept_id not in (25,10)
    """
    query2 = pg_execute(query2)

    return 'something'

def create_mviews_tickets():
    
    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)
    
    df = pd.read_sql("""
    SELECT ticket_id, ticket_pid, ticket_no, ticket_subject, list_id, user_id, user_name, user_staff_uname, user_dept_id, user_dept, user_email_id, status_id, dept_id, sla_id, topic_id, staff_id, team_id, email_id, lock_id, flags, sort, ip_address, ticket_source, source_extra, isoverdue, isanswered, duedate, est_duedate, reopened, closed, is_open, is_outgoing, lastupdate, created, updated
    FROM ticketing_dw.fact_tickets t
    where t.is_open  = 0
    and closed::date >= (current_date-5)::date
    """, con=engine)

    
    df['created'] = pd.to_datetime(df['created'])
    df['closed'] = pd.to_datetime(df['closed'])

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

    df['service_time']=df.apply(lambda row: Busmin(row['created'], row['closed']), axis=1)
    df['service_time'] = pd.to_numeric(df['service_time'])
    
    df = df.set_index('ticket_id')

    upsert(engine=engine,
       df=df,
       schema='ticketing_mviews',
       table_name='fact_tickets',
       if_row_exists='update',
       create_table=False)

    # truncate = """truncate ticketing_mviews.fact_tickets;"""
    # truncate = pg_execute(truncate)

    # df.to_sql('fact_tickets', con = engine, schema='ticketing_mviews', if_exists = 'append', index=False)
    return 'something' 



