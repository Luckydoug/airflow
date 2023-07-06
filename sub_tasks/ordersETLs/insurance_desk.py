import sys

from numpy import nan
sys.path.append(".")

#import libraries
from io import StringIO
import json
import psycopg2
import requests
import pandas as pd
from pandas.io.json._normalize import nested_to_record 
from sqlalchemy import create_engine
from airflow.models import Variable
from pandas.io.json._normalize import nested_to_record 
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from datetime import date
import datetime
import pytz
import businesstimedelta
import pandas as pd
import holidays as pyholidays
from workalendar.africa import Kenya

from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.api_login.api_login import(login)
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")

SessionId = login()

#FromDate = '2022/02/21'
#ToDate = '2022/02/22'

FromDate = date.today().strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')


# api details
#orderscreen_url = 'https://41.72.211.10:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderDetails&pageNo=1'
pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderDetailsC1&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
pagecount_payload={}
pagecount_headers = {}

def create_source_orderscreenc1_insurance_trans():

    df = pd.read_sql("""
    SELECT 
        doc_entry, odsc_date, odsc_time_int, 
        odsc_time, odsc_datetime, odsc_status, 
        (rownum||'_'||odsc_new_status)::text as odsc_new_status, 
        odsc_doc_no, 
        odsc_createdby, odsc_usr_dept, is_dropped
    FROM mabawa_staging.source_orderscreenc1_approvals
    where odsc_date::date >= '2021-11-01'
    """, con=engine)

    data = df.pivot_table(index='doc_entry', columns=['odsc_new_status'], values=['odsc_datetime','odsc_createdby', 'odsc_usr_dept', 'is_dropped'], aggfunc='min')
    
    # rename columns
    def rename(col):
        if isinstance(col, tuple):
            col = '_'.join(str(c) for c in col)
        return col
    
    data.columns = map(rename, data.columns)
    data['doc_entry'] = data.index

    #drop_table = """drop table mabawa_staging.source_orderscreenc1_approvals_trans;"""
    #drop_table = pg_execute(drop_table)

    data.to_sql('source_orderscreenc1_insurance_trans', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    
    return "something"

def index_source_orderscreenc1_insurance_trans():

    add_key = """alter table mabawa_staging.source_orderscreenc1_insurance_trans add PRIMARY KEY (doc_entry)"""
    add_key = pg_execute(add_key)
    return "something"

def create_fact_orderscreenc1_insurance():

    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)
    
    df = pd.read_sql("""
    SELECT *
    FROM mabawa_staging.source_orderscreenc1_approvals_trans;
    """, con=engine)

    df["odsc_datetime_1_Pre-Auth Initiated For Optica Insurance"] = pd.to_datetime(df["odsc_datetime_1_Pre-Auth Initiated For Optica Insurance"])
    df["odsc_datetime_1_Upload Attachment"] = pd.to_datetime(df["odsc_datetime_1_Upload Attachment"])
    df["odsc_datetime_1_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_1_Sent Pre-Auth to Insurance Company"])
    df["odsc_datetime_1_Rejected by Optica Insurance"] = pd.to_datetime(df["odsc_datetime_1_Rejected by Optica Insurance"])
    df["odsc_datetime_1_Insurance Company Needs Info from Branch"] = pd.to_datetime(df["odsc_datetime_1_Insurance Company Needs Info from Branch"])

    df["odsc_datetime_2_Upload Attachment"] = pd.to_datetime(df["odsc_datetime_2_Upload Attachment"])
    df["odsc_datetime_2_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_2_Sent Pre-Auth to Insurance Company"])
    df["odsc_datetime_2_Rejected by Optica Insurance"] = pd.to_datetime(df["odsc_datetime_2_Rejected by Optica Insurance"])
    df["odsc_datetime_2_Insurance Company Needs Info from Branch"] = pd.to_datetime(df["odsc_datetime_2_Insurance Company Needs Info from Branch"])

    df["odsc_datetime_1_Used Old Approval for New Order Value Higher th"] = pd.to_datetime(df["odsc_datetime_1_Used Old Approval for New Order Value Higher th"])
    df["odsc_datetime_1_Used Old Approval for New Order Value Lower or "] = pd.to_datetime(df["odsc_datetime_1_Used Old Approval for New Order Value Lower or "])

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
    #hl = cal.holidays(2021)
    hl = data.values.tolist()
    #print(hl)
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    businesshrs = businesstimedelta.Rules([workday, saturday, holidays])

    def BusHrs(start, end):
        if end>=start:
            return float(businesshrs.difference(start,end).hours)*float(60)+float(businesshrs.difference(start,end).seconds)/float(60)
        else:
            return ""

    # insurance desk
    df['upld1_sentpreauth_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_Upload Attachment"], row["odsc_datetime_1_Sent Pre-Auth to Insurance Company"]), axis=1)
    df['upld1_ins_rej_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_Upload Attachment"], row["odsc_datetime_1_Rejected by Optica Insurance"]), axis=1)
    df['upld1_ins_info_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_Upload Attachment"], row["odsc_datetime_1_Insurance Company Needs Info from Branch"]), axis=1)

    df['upld2_sentpreauth_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_2_Upload Attachment"], row["odsc_datetime_2_Sent Pre-Auth to Insurance Company"]), axis=1)
    df['upld2_ins_rej_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_2_Upload Attachment"], row["odsc_datetime_2_Rejected by Optica Insurance"]), axis=1)
    df['upld2_ins_info_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_2_Upload Attachment"], row["odsc_datetime_2_Insurance Company Needs Info from Branch"]), axis=1)
    #df['upld1_old_apprv_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_Upload Attachment"], row["odsc_datetime_1_Insurance Company Needs Info from Branch"]), axis=1)

    # insurance desk
    df['upld1_sentpreauth_diff'] = pd.to_numeric(df['upld1_sentpreauth_diff'])
    df['upld1_ins_rej_diff'] = pd.to_numeric(df['upld1_ins_rej_diff'])
    df['upld1_ins_info_diff'] = pd.to_numeric(df['upld1_ins_info_diff'])

    df['upld2_sentpreauth_diff'] = pd.to_numeric(df['upld2_sentpreauth_diff'])
    df['upld2_ins_rej_diff'] = pd.to_numeric(df['upld2_ins_rej_diff'])
    df['upld2_ins_info_diff'] = pd.to_numeric(df['upld2_ins_info_diff'])

    #query = """drop table mabawa_dw.fact_orderscreenc1_approvals;"""
    #query = pg_execute(query)
    
    df.to_sql('fact_orderscreenc1_insurance', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    
    return "something"

def index_fact_orderscreenc1_insurance():

    add_key = """alter table mabawa_dw.fact_orderscreenc1_insurance add PRIMARY KEY (doc_entry)"""
    add_key = pg_execute(add_key)

    #add_indexes = """"""
    #add_indexes = pg_execute(add_indexes)
    return "something"