import sys

from numpy import nan
sys.path.append(".")

#import libraries
from io import StringIO
import json
import psycopg2
import requests
import datetime
import pandas as pd
import businesstimedelta
from datetime import date
import holidays as pyholidays
from workalendar.africa import Kenya
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 

from airflow.models import Variable

from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.api_login.api_login import(login)

conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")

def insurance_feedback_time():

    query = """
    truncate mabawa_mviews.v_insurancce_feedback_time;
    insert into mabawa_mviews.v_insurancce_feedback_time
    SELECT doc_entry, doc_no, odsc_lineid, odsc_createdby, odsc_usr_dept, odsc_remarks, feedback_date, feedback_time
    FROM mabawa_staging.v_insurancce_feedback_time;
    """
    query = pg_execute(query)

    return "Created Feedback Time Table"

def create_source_orderscreenc1_ins_companies_trans():

    df = pd.read_sql("""
    SELECT 
        a.doc_entry, a.odsc_lineid,
        a.odsc_date, a.odsc_time_int, 
        a.odsc_time, a.odsc_datetime, a.odsc_status, 
        a.corrected_scheme_type as scheme_type,
        (a.corrected_scheme_no ||' '||a.status_rownum||'_'||a.odsc_new_status)::text as odsc_new_status, 
        a.odsc_doc_no, 
        a.odsc_createdby, a.odsc_usr_dept, a.is_dropped, a.odsc_remarks,
        (f.feedback_date||' '||f.feedback_time)::timestamp as feedback_datetime
    FROM mabawa_staging.source_orderscreenc1_approvals_5 a 
        left join mabawa_mviews.v_insurancce_feedback_time f on a.doc_entry = f.doc_entry 
        and a.odsc_lineid = f.odsc_lineid 
    where odsc_date::date >= '2021-11-01' 
    """, con=engine)

    df = df.pivot_table(index='doc_entry', columns=['odsc_new_status'], values=['odsc_datetime','odsc_createdby', 'odsc_usr_dept', 'is_dropped', 'feedback_datetime'], aggfunc='min')
    
    # rename columns
    def rename(col):
        if isinstance(col, tuple):
            col = '_'.join(str(c) for c in col)
        return col
    
    df.columns = map(rename, df.columns)
    df['doc_entry'] = df.index

    drop_table = """drop table mabawa_staging.source_orderscreenc1_ins_companies_trans;"""
    drop_table = pg_execute(drop_table)

    df.to_sql('source_orderscreenc1_ins_companies_trans', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    
    return "Successfully Transposed"

def index_orderscreenc1_ins_companies_trans():

    add_key = """alter table mabawa_staging.source_orderscreenc1_ins_companies_trans add PRIMARY KEY (doc_entry)"""
    add_key = pg_execute(add_key)

    return "Successfully Indexed"

def create_fact_orderscreenc1_ins_companies():

    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)
    
    df = pd.read_sql("""
    SELECT *
    FROM mabawa_staging.source_orderscreenc1_ins_companies_trans;
    """, con=engine)

    print ("Data Fetched")

    df["odsc_datetime_SCHEME 1 1_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Sent Pre-Auth to Insurance Company"])
    df["odsc_datetime_SCHEME 1 2_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Sent Pre-Auth to Insurance Company"])
    df["odsc_datetime_SCHEME 1 3_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Sent Pre-Auth to Insurance Company"])
    
    df["odsc_datetime_SCHEME 1 1_Insurance Fully Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Insurance Fully Approved"])
    df["odsc_datetime_SCHEME 1 2_Insurance Fully Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Insurance Fully Approved"])
    df["odsc_datetime_SCHEME 1 3_Insurance Fully Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Insurance Fully Approved"])
    
    df["odsc_datetime_SCHEME 1 1_Insurance Partially Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Insurance Partially Approved"])
    df["odsc_datetime_SCHEME 1 2_Insurance Partially Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Insurance Partially Approved"])
    df["odsc_datetime_SCHEME 1 3_Insurance Partially Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Insurance Partially Approved"])

    df["feedback_datetime_SCHEME 1 1_Insurance Fully Approved"] = pd.to_datetime(df["feedback_datetime_SCHEME 1 1_Insurance Fully Approved"])
    df["feedback_datetime_SCHEME 1 2_Insurance Fully Approved"] = pd.to_datetime(df["feedback_datetime_SCHEME 1 2_Insurance Fully Approved"])
    df["feedback_datetime_SCHEME 1 3_Insurance Fully Approved"] = pd.to_datetime(df["feedback_datetime_SCHEME 1 3_Insurance Fully Approved"])
    
    df["feedback_datetime_SCHEME 1 1_Insurance Partially Approved"] = pd.to_datetime(df["feedback_datetime_SCHEME 1 1_Insurance Partially Approved"])
    df["feedback_datetime_SCHEME 1 2_Insurance Partially Approved"] = pd.to_datetime(df["feedback_datetime_SCHEME 1 2_Insurance Partially Approved"])
    df["feedback_datetime_SCHEME 1 3_Insurance Partially Approved"] = pd.to_datetime(df["feedback_datetime_SCHEME 1 3_Insurance Partially Approved"])
        
    # insurance companies working hours
    workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(8),
        end_time=datetime.time(17),
        working_days=[0, 1, 2, 3, 4])

    """
    saturday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(16),
        working_days=[5])
    """
    
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    cal = Kenya()
    #hl = cal.holidays(2021)
    hl = data.values.tolist()
    #print(hl)
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    # insurance working hours
    businesshrs = businesstimedelta.Rules([workday, holidays])

    # normal working hours function
    def BusHrs(start, end):
        if end>=start:
            return float(businesshrs.difference(start,end).hours)*float(60)+float(businesshrs.difference(start,end).seconds)/float(60)
        else:
            return ""
    
    # insurance companies
    df['scheme_1_preauth1_f_apprv1']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Sent Pre-Auth to Insurance Company"], row["odsc_datetime_SCHEME 1 1_Insurance Fully Approved"]), axis=1)
    df['scheme_1_preauth2_f_apprv2']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Sent Pre-Auth to Insurance Company"], row["odsc_datetime_SCHEME 1 2_Insurance Fully Approved"]), axis=1)
    df['scheme_1_preauth3_f_apprv3']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Sent Pre-Auth to Insurance Company"], row["odsc_datetime_SCHEME 1 3_Insurance Fully Approved"]), axis=1)
    
    df['scheme_1_preauth1_p_apprv1']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Sent Pre-Auth to Insurance Company"], row["odsc_datetime_SCHEME 1 1_Insurance Partially Approved"]), axis=1)
    df['scheme_1_preauth2_p_apprv2']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Sent Pre-Auth to Insurance Company"], row["odsc_datetime_SCHEME 1 2_Insurance Partially Approved"]), axis=1)
    df['scheme_1_preauth3_p_apprv3']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Sent Pre-Auth to Insurance Company"], row["odsc_datetime_SCHEME 1 3_Insurance Partially Approved"]), axis=1)
    
    df['scheme_1_f_apprv1_feedback1']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Insurance Fully Approved"], row["feedback_datetime_SCHEME 1 1_Insurance Fully Approved"]), axis=1)
    df['scheme_1_f_apprv2_feedback2']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Insurance Fully Approved"], row["feedback_datetime_SCHEME 1 2_Insurance Fully Approved"]), axis=1)
    df['scheme_1_f_apprv3_feedback3']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Insurance Fully Approved"], row["feedback_datetime_SCHEME 1 3_Insurance Fully Approved"]), axis=1)
    
    df['scheme_1_p_apprv1_feedback1']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Insurance Partially Approved"], row["feedback_datetime_SCHEME 1 1_Insurance Partially Approved"]), axis=1)
    df['scheme_1_p_apprv2_feedback2']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Insurance Partially Approved"], row["feedback_datetime_SCHEME 1 2_Insurance Partially Approved"]), axis=1)
    df['scheme_1_p_apprv3_feedback3']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Insurance Partially Approved"], row["feedback_datetime_SCHEME 1 3_Insurance Partially Approved"]), axis=1)
    
    df['scheme_1_preauth1_f_feedback1']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Sent Pre-Auth to Insurance Company"], row["feedback_datetime_SCHEME 1 1_Insurance Fully Approved"]), axis=1)
    df['scheme_1_preauth2_f_feedback2']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Sent Pre-Auth to Insurance Company"], row["feedback_datetime_SCHEME 1 2_Insurance Fully Approved"]), axis=1)
    df['scheme_1_preauth3_f_feedback3']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Sent Pre-Auth to Insurance Company"], row["feedback_datetime_SCHEME 1 3_Insurance Fully Approved"]), axis=1)
    
    df['scheme_1_preauth1_p_feedback1']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Sent Pre-Auth to Insurance Company"], row["feedback_datetime_SCHEME 1 1_Insurance Partially Approved"]), axis=1)
    df['scheme_1_preauth2_p_feedback2']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Sent Pre-Auth to Insurance Company"], row["feedback_datetime_SCHEME 1 2_Insurance Partially Approved"]), axis=1)
    df['scheme_1_preauth3_p_feedback3']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Sent Pre-Auth to Insurance Company"], row["feedback_datetime_SCHEME 1 3_Insurance Partially Approved"]), axis=1)


    print ("Calculated Time Differences")

    # insurance companies
    df['scheme_1_preauth1_f_apprv1'] = pd.to_numeric(df['scheme_1_preauth1_f_apprv1'])
    df['scheme_1_preauth2_f_apprv2'] = pd.to_numeric(df['scheme_1_preauth2_f_apprv2'])
    df['scheme_1_preauth3_f_apprv3'] = pd.to_numeric(df['scheme_1_preauth3_f_apprv3'])

    df['scheme_1_preauth1_p_apprv1'] = pd.to_numeric(df['scheme_1_preauth1_p_apprv1'])
    df['scheme_1_preauth2_p_apprv2'] = pd.to_numeric(df['scheme_1_preauth2_p_apprv2'])
    df['scheme_1_preauth3_p_apprv3'] = pd.to_numeric(df['scheme_1_preauth3_p_apprv3'])

    df['scheme_1_f_apprv1_feedback1'] = pd.to_numeric(df['scheme_1_f_apprv1_feedback1'])
    df['scheme_1_f_apprv2_feedback2'] = pd.to_numeric(df['scheme_1_f_apprv2_feedback2'])
    df['scheme_1_f_apprv3_feedback3'] = pd.to_numeric(df['scheme_1_f_apprv3_feedback3'])

    df['scheme_1_p_apprv1_feedback1'] = pd.to_numeric(df['scheme_1_p_apprv1_feedback1'])
    df['scheme_1_p_apprv2_feedback2'] = pd.to_numeric(df['scheme_1_p_apprv2_feedback2'])
    df['scheme_1_p_apprv3_feedback3'] = pd.to_numeric(df['scheme_1_p_apprv3_feedback3'])

    df['scheme_1_preauth1_f_feedback1'] = pd.to_numeric(df['scheme_1_preauth1_f_feedback1'])
    df['scheme_1_preauth2_f_feedback2'] = pd.to_numeric(df['scheme_1_preauth2_f_feedback2'])
    df['scheme_1_preauth3_f_feedback3'] = pd.to_numeric(df['scheme_1_preauth3_f_feedback3'])

    df['scheme_1_preauth1_p_feedback1'] = pd.to_numeric(df['scheme_1_preauth1_p_feedback1'])
    df['scheme_1_preauth2_p_feedback2'] = pd.to_numeric(df['scheme_1_preauth2_p_feedback2'])
    df['scheme_1_preauth3_p_feedback3'] = pd.to_numeric(df['scheme_1_preauth3_p_feedback3'])

    print("Converted Numerics")

    query = """drop table mabawa_dw.fact_orderscreenc1_ins_companies;"""
    query = pg_execute(query)
    
    df.to_sql('fact_orderscreenc1_ins_companies', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    
    return "Finished Job"

def create_source_orderscreenc1_ins_companies_old():

    query = """
    truncate mabawa_staging.source_orderscreenc1_ins_companies;
    insert into mabawa_staging.source_orderscreenc1_ins_companies
    SELECT 
        doc_entry, odsc_lineid, odsc_date, odsc_time_int, 
        odsc_time, odsc_datetime, odsc_status, odsc_new_status, 
        rownum, odsc_doc_no, odsc_createdby, odsc_usr_dept, is_dropped
    FROM mabawa_staging.source_orderscreenc1_staging4
    where odsc_status in ('Sent Pre-Auth to Insurance Company', 'Insurance Partially Approved', 'Insurance Fully Approved')
    """
    query = pg_execute(query)

    return "Orderscreenc1 Ins Companies Created"

def create_source_orderscreenc1_ins_companies_trans_old():

    df = pd.read_sql("""
    SELECT 
        c.doc_entry, c.odsc_lineid,
        c.odsc_date, c.odsc_time_int, 
        c.odsc_time, c.odsc_datetime, c.odsc_status, 
        (c.rownum||'_'||c.odsc_new_status)::text as odsc_new_status, 
        c.odsc_doc_no, 
        c.odsc_createdby, c.odsc_usr_dept, is_dropped,
        (f.feedback_date||' '||f.feedback_time)::timestamp as feedback_datetime
    FROM mabawa_staging.source_orderscreenc1_ins_companies c 
    left join mabawa_mviews.v_insurancce_feedback_time f on c.doc_entry = f.doc_entry 
    and c.odsc_lineid = f.odsc_lineid 
    """, con=engine)

    df = df.pivot_table(index='doc_entry', columns=['odsc_new_status'], values=['odsc_datetime','odsc_createdby', 'odsc_usr_dept', 'is_dropped', 'feedback_datetime'], aggfunc='min')
    
    # rename columns
    def rename(col):
        if isinstance(col, tuple):
            col = '_'.join(str(c) for c in col)
        return col
    
    df.columns = map(rename, df.columns)
    df['doc_entry'] = df.index

    drop_table = """drop table mabawa_staging.source_orderscreenc1_ins_companies_trans;"""
    drop_table = pg_execute(drop_table)

    df.to_sql('source_orderscreenc1_ins_companies_trans', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    
    return "Successfully Transposed"

def create_fact_orderscreenc1_ins_companies_old():

    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)
    
    df = pd.read_sql("""
    SELECT *
    FROM mabawa_staging.source_orderscreenc1_ins_companies_trans;
    """, con=engine)

    print ("Data Fetched")

    df["odsc_datetime_SCHEME 1 1_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Sent Pre-Auth to Insurance Company"])
    df["odsc_datetime_SCHEME 1 2_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Sent Pre-Auth to Insurance Company"])
    df["odsc_datetime_SCHEME 1 3_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Sent Pre-Auth to Insurance Company"])
    
    df["odsc_datetime_SCHEME 1 1_Insurance Fully Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Insurance Fully Approved"])
    df["odsc_datetime_SCHEME 1 2_Insurance Fully Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Insurance Fully Approved"])
    df["odsc_datetime_SCHEME 1 3_Insurance Fully Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Insurance Fully Approved"])
    
    df["odsc_datetime_SCHEME 1 1_Insurance Partially Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Insurance Partially Approved"])
    df["odsc_datetime_SCHEME 1 2_Insurance Partially Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Insurance Partially Approved"])
    df["odsc_datetime_SCHEME 1 3_Insurance Partially Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Insurance Partially Approved"])

    df["feedback_datetime_SCHEME 1 1_Insurance Fully Approved"] = pd.to_datetime(df["feedback_datetime_SCHEME 1 1_Insurance Fully Approved"])
    df["feedback_datetime_SCHEME 1 2_Insurance Fully Approved"] = pd.to_datetime(df["feedback_datetime_SCHEME 1 2_Insurance Fully Approved"])
    df["feedback_datetime_SCHEME 1 3_Insurance Fully Approved"] = pd.to_datetime(df["feedback_datetime_SCHEME 1 3_Insurance Fully Approved"])
    
    df["feedback_datetime_SCHEME 1 1_Insurance Partially Approved"] = pd.to_datetime(df["feedback_datetime_SCHEME 1 1_Insurance Partially Approved"])
    df["feedback_datetime_SCHEME 1 2_Insurance Partially Approved"] = pd.to_datetime(df["feedback_datetime_SCHEME 1 2_Insurance Partially Approved"])
    df["feedback_datetime_SCHEME 1 3_Insurance Partially Approved"] = pd.to_datetime(df["feedback_datetime_SCHEME 1 3_Insurance Partially Approved"])
        
    # insurance companies working hours
    workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(8),
        end_time=datetime.time(17),
        working_days=[0, 1, 2, 3, 4])

    """
    saturday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(16),
        working_days=[5])
    """
    
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    cal = Kenya()
    #hl = cal.holidays(2021)
    hl = data.values.tolist()
    #print(hl)
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    # insurance working hours
    businesshrs = businesstimedelta.Rules([workday, holidays])

    # normal working hours function
    def BusHrs(start, end):
        if end>=start:
            return float(businesshrs.difference(start,end).hours)*float(60)+float(businesshrs.difference(start,end).seconds)/float(60)
        else:
            return ""
    
    # insurance companies
    df['scheme_1_preauth1_f_apprv1']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Sent Pre-Auth to Insurance Company"], row["odsc_datetime_SCHEME 1 1_Insurance Fully Approved"]), axis=1)
    df['scheme_1_preauth2_f_apprv2']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Sent Pre-Auth to Insurance Company"], row["odsc_datetime_SCHEME 1 2_Insurance Fully Approved"]), axis=1)
    df['scheme_1_preauth3_f_apprv3']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Sent Pre-Auth to Insurance Company"], row["odsc_datetime_SCHEME 1 3_Insurance Fully Approved"]), axis=1)
    
    df['scheme_1_preauth1_p_apprv1']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Sent Pre-Auth to Insurance Company"], row["odsc_datetime_SCHEME 1 1_Insurance Partially Approved"]), axis=1)
    df['scheme_1_preauth2_p_apprv2']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Sent Pre-Auth to Insurance Company"], row["odsc_datetime_SCHEME 1 2_Insurance Partially Approved"]), axis=1)
    df['scheme_1_preauth3_p_apprv3']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Sent Pre-Auth to Insurance Company"], row["odsc_datetime_SCHEME 1 3_Insurance Partially Approved"]), axis=1)
    
    df['scheme_1_f_apprv1_feedback1']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Insurance Fully Approved"], row["feedback_datetime_SCHEME 1 1_Insurance Fully Approved"]), axis=1)
    df['scheme_1_f_apprv2_feedback2']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Insurance Fully Approved"], row["feedback_datetime_SCHEME 1 2_Insurance Fully Approved"]), axis=1)
    df['scheme_1_f_apprv3_feedback3']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Insurance Fully Approved"], row["feedback_datetime_SCHEME 1 3_Insurance Fully Approved"]), axis=1)
    
    df['scheme_1_p_apprv1_feedback1']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Insurance Partially Approved"], row["feedback_datetime_SCHEME 1 1_Insurance Partially Approved"]), axis=1)
    df['scheme_1_p_apprv2_feedback2']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Insurance Partially Approved"], row["feedback_datetime_SCHEME 1 2_Insurance Partially Approved"]), axis=1)
    df['scheme_1_p_apprv3_feedback3']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Insurance Partially Approved"], row["feedback_datetime_SCHEME 1 3_Insurance Partially Approved"]), axis=1)
    
    df['scheme_1_preauth1_f_feedback1']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Sent Pre-Auth to Insurance Company"], row["feedback_datetime_SCHEME 1 1_Insurance Fully Approved"]), axis=1)
    df['scheme_1_preauth2_f_feedback2']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Sent Pre-Auth to Insurance Company"], row["feedback_datetime_SCHEME 1 2_Insurance Fully Approved"]), axis=1)
    df['scheme_1_preauth3_f_feedback3']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Sent Pre-Auth to Insurance Company"], row["feedback_datetime_SCHEME 1 3_Insurance Fully Approved"]), axis=1)
    
    df['scheme_1_preauth1_p_feedback1']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Sent Pre-Auth to Insurance Company"], row["feedback_datetime_SCHEME 1 1_Insurance Partially Approved"]), axis=1)
    df['scheme_1_preauth2_p_feedback2']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Sent Pre-Auth to Insurance Company"], row["feedback_datetime_SCHEME 1 2_Insurance Partially Approved"]), axis=1)
    df['scheme_1_preauth3_p_feedback3']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Sent Pre-Auth to Insurance Company"], row["feedback_datetime_SCHEME 1 3_Insurance Partially Approved"]), axis=1)


    print ("Calculated Time Differences")

    # insurance companies
    df['scheme_1_preauth1_f_apprv1'] = pd.to_numeric(df['scheme_1_preauth1_f_apprv1'])
    df['scheme_1_preauth2_f_apprv2'] = pd.to_numeric(df['scheme_1_preauth2_f_apprv2'])
    df['scheme_1_preauth3_f_apprv3'] = pd.to_numeric(df['scheme_1_preauth3_f_apprv3'])

    df['scheme_1_preauth1_p_apprv1'] = pd.to_numeric(df['scheme_1_preauth1_p_apprv1'])
    df['scheme_1_preauth2_p_apprv2'] = pd.to_numeric(df['scheme_1_preauth2_p_apprv2'])
    df['scheme_1_preauth3_p_apprv3'] = pd.to_numeric(df['scheme_1_preauth3_p_apprv3'])

    df['scheme_1_f_apprv1_feedback1'] = pd.to_numeric(df['scheme_1_f_apprv1_feedback1'])
    df['scheme_1_f_apprv2_feedback2'] = pd.to_numeric(df['scheme_1_f_apprv2_feedback2'])
    df['scheme_1_f_apprv3_feedback3'] = pd.to_numeric(df['scheme_1_f_apprv3_feedback3'])

    df['scheme_1_p_apprv1_feedback1'] = pd.to_numeric(df['scheme_1_p_apprv1_feedback1'])
    df['scheme_1_p_apprv2_feedback2'] = pd.to_numeric(df['scheme_1_p_apprv2_feedback2'])
    df['scheme_1_p_apprv3_feedback3'] = pd.to_numeric(df['scheme_1_p_apprv3_feedback3'])

    df['scheme_1_preauth1_f_feedback1'] = pd.to_numeric(df['scheme_1_preauth1_f_feedback1'])
    df['scheme_1_preauth2_f_feedback2'] = pd.to_numeric(df['scheme_1_preauth2_f_feedback2'])
    df['scheme_1_preauth3_f_feedback3'] = pd.to_numeric(df['scheme_1_preauth3_f_feedback3'])

    df['scheme_1_preauth1_p_feedback1'] = pd.to_numeric(df['scheme_1_preauth1_p_feedback1'])
    df['scheme_1_preauth2_p_feedback2'] = pd.to_numeric(df['scheme_1_preauth2_p_feedback2'])
    df['scheme_1_preauth3_p_feedback3'] = pd.to_numeric(df['scheme_1_preauth3_p_feedback3'])

    print("Converted Numerics")

    query = """drop table mabawa_dw.fact_orderscreenc1_ins_companies;"""
    query = pg_execute(query)
    
    df.to_sql('fact_orderscreenc1_ins_companies', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    
    return "Finished Job"