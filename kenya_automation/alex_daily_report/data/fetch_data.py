from airflow.models import Variable
from sqlalchemy import create_engine
import psycopg2
from datetime import date
import datetime
import pytz
import businesstimedelta
import pandas as pd
import holidays as pyholidays
from workalendar.africa import Kenya
import pygsheets
import mysql.connector as database
import urllib.parse
import datetime


today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
formatted_date = yesterday.strftime('%Y-%m-%d')


from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.api_login.api_login import(login)

def repdata(database, engine):
    query = f"""
    with replacement as (
    select sid.replaced_itemcode as "Item No.",i.item_desc as "Item/Service Description",sid.itr__status as "ITR Status",si.doc_no as "ITR Number",si.internal_no as "Internal Number",si.post_date as "ITR Date",
    si.exchange_type as "Exchange Type",sid.sales_orderno as "Sales Order number",sid.sales_order_entry as "Sales Order entry",
    sid.sales_order_branch as "Sales Branch",sid.draft_order_entry as "Draft order entry",sid.draft_orderno as "Order Number",si.createdon as "Creation Date",
    si.creationtime_incl_secs as "Creatn Time - Incl. Secs",sid.picker_name as "Picker Name"
    from {database}_staging.source_itr_details sid
    inner join {database}_staging.source_itr si on si.internal_no = sid.doc_internal_id 
    left join {database}_staging.source_items i on i.item_code = sid.replaced_itemcode
    where si.exchange_type = 'Replacement') 
    select * from replacement 
    where "ITR Date"::date between '{yesterday}' and '{yesterday}'
    and "Sales Branch" <> ''
    """
    Repdata =  pd.read_sql_query(query, con=engine) 
    return Repdata

def to_warehouse(database, engine):
    query1 =  f""" 
    select doc_no as "Document Number",si.internal_no as "Internal Number",doc_status as "Document Status", post_date as "Posting Date", 
    exchange_type as "Exchange Type", remarks as "Remarks", filler as "Filter",to_warehouse_code as "To Warehouse Code",
    createdon as "Creation Date", to_char(creationtime_incl_secs, 'FM999:09:99'::text) AS "Creatn Time - Incl. Secs"
    from {database}_staging.source_itr si  
    inner join {database}_staging.source_users su on su.user_signature = si.user_signature 
    where createdon::date between '{yesterday}' and '{yesterday}'
    and su.user_code in ('BRS','manager')
    """
    return pd.read_sql_query(query1,con=engine)

