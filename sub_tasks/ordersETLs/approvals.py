import sys

from numpy import nan
sys.path.append(".")

#import libraries
import json
import psycopg2
import datetime
import pandas as pd
from io import StringIO
import businesstimedelta
from datetime import date
import holidays as pyholidays
from airflow.models import Variable
from workalendar.africa import Kenya
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 

from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.api_login.api_login import(login)

conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")

def order_insurance_schemes():

    add_key = """
    truncate mabawa_staging.orders_scheme_types;
    insert into mabawa_staging.orders_scheme_types
    SELECT rownum, doc_entry, insurance_company_name, insurance_scheme, insurance_name, 
    plan_scheme_name, plan_scheme_type, scheme_no, createdon, createdat_int, createdat, 
    created_datetime
    FROM mabawa_mviews.v_orders_scheme_types_5;
    """
    add_key = pg_execute(add_key)

    return "something"

def create_source_orderscreenc1_approvals():

    query = """
    truncate table mabawa_staging.source_orderscreenc1_approvals;
    insert into mabawa_staging.source_orderscreenc1_approvals
    SELECT doc_entry, odsc_lineid, odsc_date, odsc_time_int, odsc_time, odsc_datetime, 
    odsc_status, odsc_new_status, rownum, odsc_doc_no, odsc_createdby, 
    odsc_usr_dept, is_dropped, odsc_remarks 
    FROM mabawa_staging.source_orderscreenc1_staging4
    where odsc_status in 
    ('Pre-Auth Initiated For Optica Insurance','Upload Attachment',
    'Sent Pre-Auth to Insurance Company','Rejected by Optica Insurance','Rejected by Approvals Team',
    'Insurance Company Needs Info from Branch',
    'Insurance Company Needs Info from Branch', 'Used Old Approval for New Order Value Lower or Equal than Approval',
    'SMART Forwarded to Approvals Team', 'Customer Confirmed Order','Confirmed by Approvals Team',
    'Sales Order Created', 'Corrected Form Resent to Approvals Team','Corrected Form Resent to Optica Insurance',
    'Insurance Fully Approved','Insurance Partially Approved','Insurance Order Initiated', 'Draft Payments Posted')
    """
    query = pg_execute(query)

    return "Created Orderscreenc1 Approvals"

def update_source_orderscreenc1_approvals():

    query = """
    update mabawa_staging.source_orderscreenc1_approvals c
    set odsc_new_status = 'Sales Order Created Old'
    where c.odsc_new_status = 'Sales Order Created'
    """

    query = pg_execute(query)

    query2 = """
    update mabawa_staging.source_orderscreenc1_approvals c
    set odsc_new_status = 'Sales Order Created'
    where c.odsc_status  = 'Confirmed by Approvals Team'
    """

    query2 = pg_execute(query2)

    query3 = """
    update mabawa_staging.source_orderscreenc1_approvals c
    set odsc_new_status = 'Lower - Used Old Approval for New Order Value Lower or Equal than Approval'
    where c.odsc_status  = 'Used Old Approval for New Order Value Lower or Equal than Approval'
    """

    query3 = pg_execute(query3)

    query4 = """
    update mabawa_staging.source_orderscreenc1_approvals c
    set odsc_new_status = 'Higher-Used Old Approval for New Order Value Higher than Approval'
    where c.odsc_status  = 'Used Old Approval for New Order Value Higher than Approval'
    """

    query4 = pg_execute(query4)

    return "Table Updated"

def create_source_orderscreenc1_approvals_2():

    query = """
    truncate mabawa_staging.source_orderscreenc1_approvals_2;
    insert into mabawa_staging.source_orderscreenc1_approvals_2
    SELECT doc_entry,odsc_lineid, odsc_date, odsc_time_int, odsc_time, odsc_datetime, 
    odsc_status, odsc_new_status, rownum, odsc_doc_no, odsc_createdby, odsc_usr_dept, 
    is_dropped, plan_scheme_type, scheme_no, odsc_remarks
    FROM mabawa_mviews.v_source_orderscreenc1_approvals_2;
    """
    query = pg_execute(query)

    return "Created"

def create_source_orderscreenc1_approvals_3():

    query = """
    truncate mabawa_staging.source_orderscreenc1_approvals_3;
    insert into mabawa_staging.source_orderscreenc1_approvals_3
    SELECT 
    	doc_entry, odsc_lineid, odsc_date, odsc_time_int, odsc_time, odsc_datetime, 
    	odsc_status, odsc_new_status, rownum, odsc_doc_no, odsc_createdby, 
    	odsc_usr_dept, is_dropped, plan_scheme_type, scheme_no, corrected_scheme_type, 
    	corrected_scheme_no, odsc_remarks
    FROM mabawa_mviews.v_source_orderscreenc1_approvals_3
    """
    query = pg_execute(query)

    return "Created"

def create_source_orderscreenc1_approvals_4():

    query = """
    truncate mabawa_staging.source_orderscreenc1_approvals_4;
    insert into mabawa_staging.source_orderscreenc1_approvals_4
    SELECT 
        doc_entry, odsc_lineid, odsc_date, odsc_time_int, odsc_time, odsc_datetime, 
        odsc_status, odsc_new_status, new_status_lead, rownum, odsc_doc_no, 
        odsc_createdby, odsc_usr_dept, is_dropped, plan_scheme_type, scheme_no, 
        corrected_scheme_type, corrected_scheme_no, odsc_remarks
    FROM mabawa_mviews.v_source_orderscreenc1_approvals_4
    where odsc_new_status <> new_status_lead 
    """
    query = pg_execute(query)

    return "Created"

def create_source_orderscreenc1_approvals_5():

    query = """
    truncate mabawa_staging.source_orderscreenc1_approvals_5;
    insert into mabawa_staging.source_orderscreenc1_approvals_5
    SELECT 
    doc_entry, odsc_lineid, odsc_date, odsc_time_int, odsc_time, odsc_datetime, 
    odsc_status, odsc_new_status, status_rownum, rownum, new_status_lead, 
    odsc_doc_no, odsc_createdby, odsc_usr_dept, is_dropped, plan_scheme_type, 
    scheme_no, corrected_scheme_type, corrected_scheme_no, odsc_remarks
    FROM mabawa_mviews.v_source_orderscreenc1_approvals_5;
    """
    query = pg_execute(query)

    return "Created"

def get_collected_orders_for_approvals():

    data = pd.read_sql("""
    SELECT distinct doc_entry 
    FROM mabawa_staging.source_orderscreenc1
    where odsc_status = 'Collected'
    and odsc_date::date < (current_date-interval '5 day')::date
    """, con=engine)

    print("Query Data")

    #query = """drop table mabawa_staging.source_collected_orders;"""
    #pg_execute(query)
    #print("Dropped Table")
    
    data.to_sql('source_collected_orders_for_approvals', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    print("Inserted Data")

    return 'something' 

def create_source_orderscreenc1_approvals_trans():

    data = pd.read_sql("""
    SELECT 
        doc_entry, odsc_date, odsc_time_int, 
        odsc_time, odsc_datetime, odsc_status, 
        corrected_scheme_type as scheme_type,
        (corrected_scheme_no ||' '||status_rownum||'_'||odsc_new_status)::text as odsc_new_status, 
        odsc_doc_no, 
        odsc_createdby, odsc_usr_dept, is_dropped, odsc_remarks
    FROM mabawa_staging.source_orderscreenc1_approvals_5
    where odsc_date::date >= '2021-11-01'
    """, con=engine)

    print("Fetched All Orders")

    data2 = pd.read_sql("""
    SELECT distinct doc_entry 
    FROM mabawa_staging.source_orderscreenc1_staging4
    where odsc_status  in ('Insurance Order Initiated','Upload Attachment','Customer Confirmed Order','SMART Forwarded to Approvals Team')
    """, con=engine)

    print("Fetched Limiting Orders")

    data = pd.merge(data, data2, how='left', indicator=True)

    data = data[data['_merge'] == 'both']
    data = data.rename(columns = {'_merge':'old_merge'})
    print("Completed First Merge")

    print("Initiating Transposition")

    data = data.pivot_table(index='doc_entry', columns=['odsc_new_status'], values=['odsc_datetime','odsc_createdby', 'odsc_usr_dept', 'scheme_type', 'is_dropped', 'odsc_remarks'], aggfunc='min')
    
    # rename columns
    def rename(col):
        if isinstance(col, tuple):
            col = '_'.join(str(c) for c in col)
        return col
    
    data.columns = map(rename, data.columns)
    data['doc_entry'] = data.index
    print("Renamed Columns")

    drop_table = """drop table mabawa_staging.source_orderscreenc1_approvals_trans;"""
    drop_table = pg_execute(drop_table)
    print("Dropped Trans Table")

    data.to_sql('source_orderscreenc1_approvals_trans', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    print("Transposition Complete Trans Table")

    #collected = pd.read_sql("""
    #SELECT doc_entry
    #FROM mabawa_staging.source_collected_orders_for_approvals;
    #""", con=engine)

    #print("Fetched Collected Data")

    #data = pd.merge(data, collected, how='left', indicator=True)
    #data =data[data['_merge'] == 'left_only']

    #print("Completed Second Merge")
    #print(data.columns)
    #data['doc_entry'] = data.index
    #print("Created Temp Trans Table")
    #data = data.set_index('doc_entry')
    """
    upsert(engine=engine,
       df=data,
       schema='mabawa_staging',
       table_name='source_orderscreenc1_approvals_trans',
       if_row_exists='update',
       create_table=False)
    """
    #print("Completed Upsert")

    return "Successfully Transposed"

def create_fact_orderscreenc1_approvals():

    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)

    print("Fetched All Holidays")

    df = pd.read_sql("""
    SELECT *
    FROM mabawa_staging.source_orderscreenc1_approvals_trans;
    """, con=engine)

    print ("Fetched Order Data")

    # scheme 1 dates
    
    df["odsc_datetime_SCHEME 1 1_Pre-Auth Initiated For Optica Insuranc"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Pre-Auth Initiated For Optica Insuranc"])
    df["odsc_datetime_SCHEME 1 1_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Sent Pre-Auth to Insurance Company"])
    df["odsc_datetime_SCHEME 1 1_Insurance Fully Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Insurance Fully Approved"])
    df["odsc_datetime_SCHEME 1 1_Insurance Partially Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Insurance Partially Approved"])
    df["odsc_datetime_SCHEME 1 1_Rejected by Optica Insurance"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Rejected by Optica Insurance"])
    df["odsc_datetime_SCHEME 1 1_Insurance Company Needs Info from Bran"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Insurance Company Needs Info from Bran"])
    df["odsc_datetime_SCHEME 1 1_Lower - Used Old Approval for New Orde"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Lower - Used Old Approval for New Orde"])
    df["odsc_datetime_SCHEME 1 1_Corrected Form Resent to Optica Insura"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Corrected Form Resent to Optica Insura"])
    
    df["odsc_datetime_SCHEME 1 1_Upload Attachment"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Upload Attachment"])
    df["odsc_datetime_SCHEME 1 1_SMART Forwarded to Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_SMART Forwarded to Approvals Team"])
    df["odsc_datetime_SCHEME 1 1_Customer Confirmed Order"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Customer Confirmed Order"])
    df["odsc_datetime_SCHEME 1 1_Sales Order Created"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Sales Order Created"])
    df["odsc_datetime_SCHEME 1 1_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Rejected by Approvals Team"])
    df["odsc_datetime_SCHEME 1 1_Corrected Form Resent to Approvals Tea"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 1_Corrected Form Resent to Approvals Tea"])
    
    print("Converted Dates for Scheme 1 Level 1")

    df["odsc_datetime_SCHEME 1 2_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Sent Pre-Auth to Insurance Company"])
    df["odsc_datetime_SCHEME 1 2_Insurance Fully Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Insurance Fully Approved"])
    df["odsc_datetime_SCHEME 1 2_Insurance Partially Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Insurance Partially Approved"])
    df["odsc_datetime_SCHEME 1 2_Rejected by Optica Insurance"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Rejected by Optica Insurance"])
    df["odsc_datetime_SCHEME 1 2_Insurance Company Needs Info from Bran"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Insurance Company Needs Info from Bran"])
    df["odsc_datetime_SCHEME 1 2_Lower - Used Old Approval for New Orde"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Lower - Used Old Approval for New Orde"])
    df["odsc_datetime_SCHEME 1 2_Corrected Form Resent to Optica Insura"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Corrected Form Resent to Optica Insura"])
    
    df["odsc_datetime_SCHEME 1 2_Upload Attachment"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Upload Attachment"])
    df["odsc_datetime_SCHEME 1 2_SMART Forwarded to Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_SMART Forwarded to Approvals Team"])
    df["odsc_datetime_SCHEME 1 2_Customer Confirmed Order"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Customer Confirmed Order"])
    df["odsc_datetime_SCHEME 1 2_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Rejected by Approvals Team"])
    df["odsc_datetime_SCHEME 1 2_Corrected Form Resent to Approvals Tea"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 2_Corrected Form Resent to Approvals Tea"])
    
    print("Converted Dates for Scheme 1 Level 2")

    df["odsc_datetime_SCHEME 1 3_Upload Attachment"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Upload Attachment"])
    df["odsc_datetime_SCHEME 1 3_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Sent Pre-Auth to Insurance Company"])
    df["odsc_datetime_SCHEME 1 3_Rejected by Optica Insurance"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Rejected by Optica Insurance"])
    df["odsc_datetime_SCHEME 1 3_Insurance Company Needs Info from Bran"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Insurance Company Needs Info from Bran"])
    df["odsc_datetime_SCHEME 1 3_SMART Forwarded to Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_SMART Forwarded to Approvals Team"])
    df["odsc_datetime_SCHEME 1 3_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Rejected by Approvals Team"])
    df["odsc_datetime_SCHEME 1 3_Corrected Form Resent to Approvals Tea"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Corrected Form Resent to Approvals Tea"])
    df["odsc_datetime_SCHEME 1 3_Corrected Form Resent to Optica Insura"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Corrected Form Resent to Optica Insura"])
    
    #df["odsc_datetime_SCHEME 1 3_Insurance Fully Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Insurance Fully Approved"])
    #df["odsc_datetime_SCHEME 1 3_Insurance Partially Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Insurance Partially Approved"])
    #df["odsc_datetime_SCHEME 1 3_Customer Confirmed Order"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 3_Customer Confirmed Order"])
    
    print("Converted Dates for Scheme 1 Level 3")

    #df["odsc_datetime_SCHEME 1 4_Upload Attachment"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 4_Upload Attachment"])
    df["odsc_datetime_SCHEME 1 4_Rejected by Optica Insurance"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 4_Rejected by Optica Insurance"])
    df["odsc_datetime_SCHEME 1 4_Insurance Company Needs Info from Bran"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 4_Insurance Company Needs Info from Bran"])
    df["odsc_datetime_SCHEME 1 4_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 4_Rejected by Approvals Team"])
    df["odsc_datetime_SCHEME 1 4_Corrected Form Resent to Approvals Tea"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 4_Corrected Form Resent to Approvals Tea"])
    df["odsc_datetime_SCHEME 1 4_Corrected Form Resent to Optica Insura"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 4_Corrected Form Resent to Optica Insura"])
    
    print("Converted Dates for Scheme 1 Level 4")

    df["odsc_datetime_SCHEME 1 5_Rejected by Optica Insurance"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 5_Rejected by Optica Insurance"])
    df["odsc_datetime_SCHEME 1 5_Insurance Company Needs Info from Bran"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 5_Insurance Company Needs Info from Bran"])
    df["odsc_datetime_SCHEME 1 5_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 5_Rejected by Approvals Team"])
    df["odsc_datetime_SCHEME 1 5_Corrected Form Resent to Approvals Tea"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 5_Corrected Form Resent to Approvals Tea"])
    df["odsc_datetime_SCHEME 1 5_Corrected Form Resent to Optica Insura"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 5_Corrected Form Resent to Optica Insura"])
    
    print("Converted Dates for Scheme 1 Level 5")

    df["odsc_datetime_SCHEME 1 6_Rejected by Optica Insurance"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 6_Rejected by Optica Insurance"])
    df["odsc_datetime_SCHEME 1 6_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 6_Rejected by Approvals Team"])
    df["odsc_datetime_SCHEME 1 6_Corrected Form Resent to Approvals Tea"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 6_Corrected Form Resent to Approvals Tea"])
    df["odsc_datetime_SCHEME 1 6_Corrected Form Resent to Optica Insura"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 6_Corrected Form Resent to Optica Insura"])
    
    print("Converted Dates for Scheme 1 Level 6")

    #df["odsc_datetime_SCHEME 1 7_Rejected by Optica Insurance"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 7_Rejected by Optica Insurance"])
    #df["odsc_datetime_SCHEME 1 7_Corrected Form Resent to Optica Insura"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 7_Corrected Form Resent to Optica Insura"])
    #df["odsc_datetime_SCHEME 1 7_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 7_Rejected by Approvals Team"])
    #df["odsc_datetime_SCHEME 1 7_Corrected Form Resent to Approvals Tea"] = pd.to_datetime(df["odsc_datetime_SCHEME 1 7_Corrected Form Resent to Approvals Tea"])
    
    print("Converted Dates for Scheme 1 Level 7")
    
    # scheme 2 dates

    df["odsc_datetime_SCHEME 2 1_Pre-Auth Initiated For Optica Insuranc"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 1_Pre-Auth Initiated For Optica Insuranc"])
    df["odsc_datetime_SCHEME 2 1_Upload Attachment"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 1_Upload Attachment"])
    df["odsc_datetime_SCHEME 2 1_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 1_Sent Pre-Auth to Insurance Company"])
    df["odsc_datetime_SCHEME 2 1_Rejected by Optica Insurance"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 1_Rejected by Optica Insurance"])
    #df["odsc_datetime_SCHEME 2 1_Insurance Company Needs Info from Bran"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 1_Insurance Company Needs Info from Bran"])
    df["odsc_datetime_SCHEME 2 1_SMART Forwarded to Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 1_SMART Forwarded to Approvals Team"])
    df["odsc_datetime_SCHEME 2 1_Customer Confirmed Order"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 1_Customer Confirmed Order"])
    df["odsc_datetime_SCHEME 2 1_Sales Order Created"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 1_Sales Order Created"])
    df["odsc_datetime_SCHEME 2 1_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 1_Rejected by Approvals Team"])
    df["odsc_datetime_SCHEME 2 1_Corrected Form Resent to Approvals Tea"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 1_Corrected Form Resent to Approvals Tea"])
    df["odsc_datetime_SCHEME 2 1_Insurance Fully Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 1_Insurance Fully Approved"])
    df["odsc_datetime_SCHEME 2 1_Insurance Partially Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 1_Insurance Partially Approved"])

    print("Converted Dates for Scheme 2 Level 1")

    df["odsc_datetime_SCHEME 2 2_Upload Attachment"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 2_Upload Attachment"])
    df["odsc_datetime_SCHEME 2 2_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 2_Sent Pre-Auth to Insurance Company"])
    #df["odsc_datetime_SCHEME 2 2_Rejected by Optica Insurance"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 2_Rejected by Optica Insurance"])
    #df["odsc_datetime_SCHEME 2 2_Insurance Company Needs Info from Bran"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 2_Insurance Company Needs Info from Bran"])
    df["odsc_datetime_SCHEME 2 2_SMART Forwarded to Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 2_SMART Forwarded to Approvals Team"])
    df["odsc_datetime_SCHEME 2 2_Customer Confirmed Order"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 2_Customer Confirmed Order"])
    df["odsc_datetime_SCHEME 2 2_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 2_Rejected by Approvals Team"])
    df["odsc_datetime_SCHEME 2 2_Corrected Form Resent to Approvals Tea"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 2_Corrected Form Resent to Approvals Tea"])
    df["odsc_datetime_SCHEME 2 2_Insurance Fully Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 2_Insurance Fully Approved"])
    df["odsc_datetime_SCHEME 2 2_Insurance Partially Approved"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 2_Insurance Partially Approved"])

    print("Converted Dates for Scheme 2 Level 2")

    df["odsc_datetime_SCHEME 2 3_Upload Attachment"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 3_Upload Attachment"])
    #df["odsc_datetime_SCHEME 2 3_Rejected by Optica Insurance"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 3_Rejected by Optica Insurance"])
    #df["odsc_datetime_SCHEME 2 3_Insurance Company Needs Info from Bran"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 3_Insurance Company Needs Info from Bran"])
    df["odsc_datetime_SCHEME 2 3_SMART Forwarded to Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 3_SMART Forwarded to Approvals Team"])
    df["odsc_datetime_SCHEME 2 3_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 3_Rejected by Approvals Team"])
    #df["odsc_datetime_SCHEME 2 3_Corrected Form Resent to Approvals Tea"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 3_Corrected Form Resent to Approvals Tea"])
    
    print("Converted Dates for Scheme 2 Level 3")

    #df["odsc_datetime_SCHEME 2 4_Insurance Company Needs Info from Bran"] = pd.to_datetime(df["odsc_datetime_SCHEME 2 4_Insurance Company Needs Info from Bran"])
    
    print("Converted Dates for Scheme 2 Level 4")

    print("Identified Dates")

    # normal working hours
    workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(18),
        working_days=[0,1, 2, 3, 4])

    saturday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(16),
        working_days=[5])

    insurance_workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(8),
        end_time=datetime.time(17),
        working_days=[0,1, 2, 3, 4])
    
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    cal = Kenya()
    #hl = cal.holidays(2021)
    hl = data.values.tolist()
    #print(hl)
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    # normal working hours
    businesshrs = businesstimedelta.Rules([workday, saturday, holidays])

    # insurance working hours
    insbusinesshrs = businesstimedelta.Rules([insurance_workday, holidays])

    # normal working hours function
    def BusHrs(start, end):
        if end>=start:
            return float(businesshrs.difference(start,end).hours)*float(60)+float(businesshrs.difference(start,end).seconds)/float(60)
        else:
            return ""

    # insurance working hours function
    def InsHrs(start, end):
        if end>=start:
            return float(insbusinesshrs.difference(start,end).hours)*float(60)+float(insbusinesshrs.difference(start,end).seconds)/float(60)
        else:
            return ""

    # scheme 1 approvals
    df['scheme_1_upld1_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Upload Attachment"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    df['scheme_1_upld2_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Upload Attachment"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    df['scheme_1_upld3_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Upload Attachment"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    #df['scheme_1_upld4_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 4_Upload Attachment"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    df['scheme_1_smart1_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_SMART Forwarded to Approvals Team"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    df['scheme_1_smart2_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_SMART Forwarded to Approvals Team"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    df['scheme_1_smart3_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_SMART Forwarded to Approvals Team"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    df['scheme_1_cust1_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Customer Confirmed Order"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    df['scheme_1_cust2_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Customer Confirmed Order"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    #df['scheme_1_cust3_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Customer Confirmed Order"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    df['scheme_1_corrected1_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    df['scheme_1_corrected2_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    df['scheme_1_corrected3_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    df['scheme_1_corrected4_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 4_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    df['scheme_1_corrected5_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 5_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    df['scheme_1_corrected6_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 6_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    #df['scheme_1_corrected7_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 7_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 1 1_Sales Order Created"]), axis=1)
    
    print("Calculated Working Hours for Scheme 1 Approvals")

    # scheme 1 rejection 1
    df['scheme_1_upld1_rej_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Upload Attachment"], row["odsc_datetime_SCHEME 1 1_Rejected by Approvals Team"]), axis=1)
    df['scheme_1_smart1_rej_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_SMART Forwarded to Approvals Team"], row["odsc_datetime_SCHEME 1 1_Rejected by Approvals Team"]), axis=1)
    df['scheme_1_cust1_rej_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Customer Confirmed Order"], row["odsc_datetime_SCHEME 1 1_Rejected by Approvals Team"]), axis=1)
    
    print("Calculated Working Hours for Scheme 1 Rejections 1")

    # scheme 1 rejection 2
    df['scheme_1_upld2_rej2_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Upload Attachment"], row["odsc_datetime_SCHEME 1 2_Rejected by Approvals Team"]), axis=1)
    df['scheme_1_smart2_rej2_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_SMART Forwarded to Approvals Team"], row["odsc_datetime_SCHEME 1 2_Rejected by Approvals Team"]), axis=1)
    df['scheme_1_cust2_rej2_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Customer Confirmed Order"], row["odsc_datetime_SCHEME 1 2_Rejected by Approvals Team"]), axis=1)
    df['scheme_1_corrected1_rej2_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 1_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 1 2_Rejected by Approvals Team"]), axis=1)
    
    print("Calculated Working Hours for Scheme 1 Rejections 2")

    # scheme 1 rejection 3
    df['scheme_1_upld3_rej3_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Upload Attachment"], row["odsc_datetime_SCHEME 1 3_Rejected by Approvals Team"]), axis=1)
    df['scheme_1_smart3_rej3_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_SMART Forwarded to Approvals Team"], row["odsc_datetime_SCHEME 1 3_Rejected by Approvals Team"]), axis=1)
    #df['scheme_1_cust3_rej3_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Customer Confirmed Order"], row["odsc_datetime_SCHEME 1 3_Rejected by Approvals Team"]), axis=1)
    df['scheme_1_corrected2_rej3_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 2_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 1 3_Rejected by Approvals Team"]), axis=1)
    
    print("Calculated Working Hours for Scheme 1 Rejections 3")

    # scheme 1 rejection 4
    #df['scheme_1_upld4_rej4_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 4_Upload Attachment"], row["odsc_datetime_SCHEME 1 4_Rejected by Approvals Team"]), axis=1)
    df['scheme_1_corrected3_rej4_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 3_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 1 4_Rejected by Approvals Team"]), axis=1)
    
    print("Calculated Working Hours for Scheme 1 Rejections 4")

    # scheme 1 rejection 5
    df['scheme_1_corrected4_rej5_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 4_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 1 5_Rejected by Approvals Team"]), axis=1)
    
    print("Calculated Working Hours for Scheme 1 Rejections 5")

    # scheme 1 rejection 6
    df['scheme_1_corrected5_rej6_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 5_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 1 6_Rejected by Approvals Team"]), axis=1)
    
    print("Calculated Working Hours for Scheme 1 Rejections 6")

    # scheme 1 rejection 7
    #df['scheme_1_corrected6_rej7_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 1 6_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 1 7_Rejected by Approvals Team"]), axis=1)
    
    print("Calculated Working Hours for Scheme 1 Rejections 7")
    
    # scheme 2 approvals
    df['scheme_2_upld1_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 1_Upload Attachment"], row["odsc_datetime_SCHEME 2 1_Sales Order Created"]), axis=1)
    df['scheme_2_upld2_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 2_Upload Attachment"], row["odsc_datetime_SCHEME 2 1_Sales Order Created"]), axis=1)
    df['scheme_2_upld3_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 3_Upload Attachment"], row["odsc_datetime_SCHEME 2 1_Sales Order Created"]), axis=1)
    df['scheme_2_smart1_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 1_SMART Forwarded to Approvals Team"], row["odsc_datetime_SCHEME 2 1_Sales Order Created"]), axis=1)
    df['scheme_2_smart2_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 2_SMART Forwarded to Approvals Team"], row["odsc_datetime_SCHEME 2 1_Sales Order Created"]), axis=1)
    df['scheme_2_smart3_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 3_SMART Forwarded to Approvals Team"], row["odsc_datetime_SCHEME 2 1_Sales Order Created"]), axis=1)
    df['scheme_2_cust1_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 1_Customer Confirmed Order"], row["odsc_datetime_SCHEME 2 1_Sales Order Created"]), axis=1)
    df['scheme_2_cust2_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 2_Customer Confirmed Order"], row["odsc_datetime_SCHEME 2 1_Sales Order Created"]), axis=1)
    df['scheme_2_corrected1_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 1_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 2 1_Sales Order Created"]), axis=1)
    df['scheme_2_corrected2_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 2_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 2 1_Sales Order Created"]), axis=1)
    #df['scheme_2_corrected3_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 3_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 2 1_Sales Order Created"]), axis=1)
    
    print("Calculated Working Hours for Scheme 2 Approvals")

    # scheme 2 rejection 1
    df['scheme_2_upld1_rej_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 1_Upload Attachment"], row["odsc_datetime_SCHEME 2 1_Rejected by Approvals Team"]), axis=1)
    df['scheme_2_smart1_rej_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 1_SMART Forwarded to Approvals Team"], row["odsc_datetime_SCHEME 2 1_Rejected by Approvals Team"]), axis=1)
    df['scheme_2_cust1_rej_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 1_Customer Confirmed Order"], row["odsc_datetime_SCHEME 2 1_Rejected by Approvals Team"]), axis=1)
    
    print("Calculated Working Hours for Scheme 2 Rejections 1")

    # scheme 2 rejection 2
    df['scheme_2_upld2_rej2_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 2_Upload Attachment"], row["odsc_datetime_SCHEME 2 2_Rejected by Approvals Team"]), axis=1)
    df['scheme_2_smart2_rej2_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 2_SMART Forwarded to Approvals Team"], row["odsc_datetime_SCHEME 2 2_Rejected by Approvals Team"]), axis=1)
    df['scheme_2_cust2_rej2_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 2_Customer Confirmed Order"], row["odsc_datetime_SCHEME 2 2_Rejected by Approvals Team"]), axis=1)
    df['scheme_2_corrected1_rej2_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 1_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 2 2_Rejected by Approvals Team"]), axis=1)
    
    print("Calculated Working Hours for Scheme 2 Rejections 2")

    # scheme 2 rejection 3
    df['scheme_2_upld3_rej3_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 3_Upload Attachment"], row["odsc_datetime_SCHEME 2 3_Rejected by Approvals Team"]), axis=1)
    df['scheme_2_smart3_rej3_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 3_SMART Forwarded to Approvals Team"], row["odsc_datetime_SCHEME 2 3_Rejected by Approvals Team"]), axis=1)
    df['scheme_2_corrected2_rej3_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 2_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 2 3_Rejected by Approvals Team"]), axis=1)
    
    print("Calculated Working Hours for Scheme 2 Rejections 3")

    # scheme 2 rejection 4
    #df['scheme_2_corrected3_rej4_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 3_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 2 4_Rejected by Approvals Team"]), axis=1)
    #print("Calculated Working Hours for Scheme 2 Rejections 4")

    # scheme 2 rejection 5
    #df['scheme_2_corrected4_rej5_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_SCHEME 2 4_Corrected Form Resent to Approvals Tea"], row["odsc_datetime_SCHEME 2 5_Rejected by Approvals Team"]), axis=1)
    #print("Calculated Working Hours for Scheme 2 Rejections 5")

    print ("Calculated All Work Hours")


    print("Starting Numerics Conversion")

    # scheme 1 approvals
    df['scheme_1_upld1_so_diff'] = pd.to_numeric(df['scheme_1_upld1_so_diff'])
    df['scheme_1_upld2_so_diff'] = pd.to_numeric(df['scheme_1_upld2_so_diff'])
    df['scheme_1_upld3_so_diff'] = pd.to_numeric(df['scheme_1_upld3_so_diff'])
    #df['scheme_1_upld4_so_diff'] = pd.to_numeric(df['scheme_1_upld4_so_diff'])
    df['scheme_1_smart1_so_diff'] = pd.to_numeric(df['scheme_1_smart1_so_diff'])
    df['scheme_1_smart2_so_diff'] = pd.to_numeric(df['scheme_1_smart2_so_diff'])
    df['scheme_1_smart3_so_diff'] = pd.to_numeric(df['scheme_1_smart3_so_diff'])
    df['scheme_1_cust1_so_diff'] = pd.to_numeric(df['scheme_1_cust1_so_diff'])
    df['scheme_1_cust2_so_diff'] = pd.to_numeric(df['scheme_1_cust2_so_diff'])
    #df['scheme_1_cust3_so_diff'] = pd.to_numeric(df['scheme_1_cust3_so_diff'])
    df['scheme_1_corrected1_so_diff'] = pd.to_numeric(df['scheme_1_corrected1_so_diff'])
    df['scheme_1_corrected2_so_diff'] = pd.to_numeric(df['scheme_1_corrected2_so_diff'])
    df['scheme_1_corrected3_so_diff'] = pd.to_numeric(df['scheme_1_corrected3_so_diff'])
    df['scheme_1_corrected4_so_diff'] = pd.to_numeric(df['scheme_1_corrected4_so_diff'])
    df['scheme_1_corrected5_so_diff'] = pd.to_numeric(df['scheme_1_corrected5_so_diff'])
    df['scheme_1_corrected6_so_diff'] = pd.to_numeric(df['scheme_1_corrected6_so_diff'])
    #df['scheme_1_corrected7_so_diff'] = pd.to_numeric(df['scheme_1_corrected7_so_diff'])

    print("Converted Numerics for Scheme 1 Approvals")
    
    # scheme 1 rejections 1
    df['scheme_1_upld1_rej_diff'] = pd.to_numeric(df['scheme_1_upld1_rej_diff'])
    df['scheme_1_smart1_rej_diff'] = pd.to_numeric(df['scheme_1_smart1_rej_diff'])
    df['scheme_1_cust1_rej_diff'] = pd.to_numeric(df['scheme_1_cust1_rej_diff'])

    print("Converted Numerics for Scheme 1 Rejections 1")

    # scheme 1 rejections 2
    df['scheme_1_upld2_rej2_diff'] = pd.to_numeric(df['scheme_1_upld2_rej2_diff'])
    df['scheme_1_smart2_rej2_diff'] = pd.to_numeric(df['scheme_1_smart2_rej2_diff'])
    df['scheme_1_cust2_rej2_diff'] = pd.to_numeric(df['scheme_1_cust2_rej2_diff'])
    df['scheme_1_corrected1_rej2_diff'] = pd.to_numeric(df['scheme_1_corrected1_rej2_diff'])

    print("Converted Numerics for Scheme 1 Rejections 2")

    # scheme 1 rejections 3
    df['scheme_1_upld3_rej3_diff'] = pd.to_numeric(df['scheme_1_upld3_rej3_diff'])
    df['scheme_1_smart3_rej3_diff'] = pd.to_numeric(df['scheme_1_smart3_rej3_diff'])
    #df['scheme_1_cust3_rej3_diff'] = pd.to_numeric(df['scheme_1_cust3_rej3_diff'])
    df['scheme_1_corrected2_rej3_diff'] = pd.to_numeric(df['scheme_1_corrected2_rej3_diff'])

    print("Converted Numerics for Scheme 1 Rejections 3")

    # scheme 1 rejections 4
    #df['scheme_1_upld4_rej4_diff'] = pd.to_numeric(df['scheme_1_upld4_rej4_diff'])
    df['scheme_1_corrected3_rej4_diff'] = pd.to_numeric(df['scheme_1_corrected3_rej4_diff'])

    print("Converted Numerics for Scheme 1 Rejections 4")
    
    # scheme 1 rejections 5
    df['scheme_1_corrected4_rej5_diff'] = pd.to_numeric(df['scheme_1_corrected4_rej5_diff'])

    print("Converted Numerics for Scheme 1 Rejections 5")
    
    # scheme 1 rejections 6
    df['scheme_1_corrected5_rej6_diff'] = pd.to_numeric(df['scheme_1_corrected5_rej6_diff'])

    print("Converted Numerics for Scheme 1 Rejections 6")
    
    # scheme 1 rejections 7
    #df['scheme_1_corrected6_rej7_diff'] = pd.to_numeric(df['scheme_1_corrected6_rej7_diff'])

    print("Converted Numerics for Scheme 1 Rejections 7")
    
    
    # scheme 2 approvals
    df['scheme_2_upld1_so_diff'] = pd.to_numeric(df['scheme_2_upld1_so_diff'])
    df['scheme_2_upld2_so_diff'] = pd.to_numeric(df['scheme_2_upld2_so_diff'])
    df['scheme_2_upld3_so_diff'] = pd.to_numeric(df['scheme_2_upld3_so_diff'])
    df['scheme_2_smart1_so_diff'] = pd.to_numeric(df['scheme_2_smart1_so_diff'])
    df['scheme_2_smart2_so_diff'] = pd.to_numeric(df['scheme_2_smart2_so_diff'])
    df['scheme_2_smart3_so_diff'] = pd.to_numeric(df['scheme_2_smart3_so_diff'])
    df['scheme_2_cust1_so_diff'] = pd.to_numeric(df['scheme_2_cust1_so_diff'])
    df['scheme_2_cust2_so_diff'] = pd.to_numeric(df['scheme_2_cust2_so_diff'])
    df['scheme_2_corrected1_so_diff'] = pd.to_numeric(df['scheme_2_corrected1_so_diff'])
    df['scheme_2_corrected2_so_diff'] = pd.to_numeric(df['scheme_2_corrected2_so_diff'])
    #df['scheme_2_corrected3_so_diff'] = pd.to_numeric(df['scheme_2_corrected3_so_diff'])

    print("Converted Numerics for Scheme 2 Approvals")
    
    # scheme 2 rejections 1
    df['scheme_2_upld1_rej_diff'] = pd.to_numeric(df['scheme_2_upld1_rej_diff'])
    df['scheme_2_smart1_rej_diff'] = pd.to_numeric(df['scheme_2_smart1_rej_diff'])
    df['scheme_2_cust1_rej_diff'] = pd.to_numeric(df['scheme_2_cust1_rej_diff'])

    print("Converted Numerics for Scheme 2 Rejections 1")

    # scheme 2 rejections 2
    df['scheme_2_upld2_rej2_diff'] = pd.to_numeric(df['scheme_2_upld2_rej2_diff'])
    df['scheme_2_smart2_rej2_diff'] = pd.to_numeric(df['scheme_2_smart2_rej2_diff'])
    df['scheme_2_cust2_rej2_diff'] = pd.to_numeric(df['scheme_2_cust2_rej2_diff'])
    df['scheme_2_corrected1_rej2_diff'] = pd.to_numeric(df['scheme_2_corrected1_rej2_diff'])

    print("Converted Numerics for Scheme 2 Rejections 2")

    # scheme 2 rejections 3
    df['scheme_2_upld3_rej3_diff'] = pd.to_numeric(df['scheme_2_upld3_rej3_diff'])
    df['scheme_2_smart3_rej3_diff'] = pd.to_numeric(df['scheme_2_smart3_rej3_diff'])
    df['scheme_2_corrected2_rej3_diff'] = pd.to_numeric(df['scheme_2_corrected2_rej3_diff'])

    print("Converted Numerics for Scheme 2 Rejections 3")

    # scheme 2 rejections 4
    #df['scheme_2_corrected3_rej4_diff'] = pd.to_numeric(df['scheme_2_corrected3_rej4_diff'])
    # print("Converted Numerics for Scheme 2 Rejections 4")

    print("Converted Approvals Numerics")

    # insurance desk
    print("Starting Insurance Desk Working Hr Calculations")

    df['scheme_1_upld1_sentpreauth_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 1_Upload Attachment"], row["odsc_datetime_SCHEME 1 1_Sent Pre-Auth to Insurance Company"]), axis=1)
    df['scheme_1_upld2_sentpreauth2_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 2_Upload Attachment"], row["odsc_datetime_SCHEME 1 2_Sent Pre-Auth to Insurance Company"]), axis=1)
    df['scheme_1_upld3_sentpreauth3_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 3_Upload Attachment"], row["odsc_datetime_SCHEME 1 3_Sent Pre-Auth to Insurance Company"]), axis=1)
    
    df['scheme_1_upld1_old_apprv_l_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 1_Upload Attachment"], row["odsc_datetime_SCHEME 1 1_Lower - Used Old Approval for New Orde"]), axis=1)
    df['scheme_1_upld2_old_apprv2_l_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 2_Upload Attachment"], row["odsc_datetime_SCHEME 1 2_Lower - Used Old Approval for New Orde"]), axis=1)

    df['scheme_1_upld1_ins_rej_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 1_Upload Attachment"], row["odsc_datetime_SCHEME 1 1_Rejected by Optica Insurance"]), axis=1)
    df['scheme_1_upld1_ins_info_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 1_Upload Attachment"], row["odsc_datetime_SCHEME 1 1_Insurance Company Needs Info from Bran"]), axis=1)
    
    df['scheme_1_corrected_ins_info2_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 1_Corrected Form Resent to Optica Insura"], row["odsc_datetime_SCHEME 1 2_Insurance Company Needs Info from Bran"]), axis=1)
    df['scheme_1_corrected2_ins_info3_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 2_Corrected Form Resent to Optica Insura"], row["odsc_datetime_SCHEME 1 3_Insurance Company Needs Info from Bran"]), axis=1)
    df['scheme_1_corrected3_ins_info4_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 3_Corrected Form Resent to Optica Insura"], row["odsc_datetime_SCHEME 1 4_Insurance Company Needs Info from Bran"]), axis=1)
    df['scheme_1_corrected4_ins_info5_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 4_Corrected Form Resent to Optica Insura"], row["odsc_datetime_SCHEME 1 5_Insurance Company Needs Info from Bran"]), axis=1)
    
    df['scheme_1_corrected_ins_rej2_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 1_Corrected Form Resent to Optica Insura"], row["odsc_datetime_SCHEME 1 2_Rejected by Optica Insurance"]), axis=1)
    df['scheme_1_corrected2_ins_rej3_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 2_Corrected Form Resent to Optica Insura"], row["odsc_datetime_SCHEME 1 3_Rejected by Optica Insurance"]), axis=1)
    df['scheme_1_corrected3_ins_rej4_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 3_Corrected Form Resent to Optica Insura"], row["odsc_datetime_SCHEME 1 4_Rejected by Optica Insurance"]), axis=1)
    df['scheme_1_corrected4_ins_rej5_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 4_Corrected Form Resent to Optica Insura"], row["odsc_datetime_SCHEME 1 5_Rejected by Optica Insurance"]), axis=1)
    df['scheme_1_corrected5_ins_rej6_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 5_Corrected Form Resent to Optica Insura"], row["odsc_datetime_SCHEME 1 6_Rejected by Optica Insurance"]), axis=1)
    #df['scheme_1_corrected6_ins_rej7_diff']=df.apply(lambda row: InsHrs(row["odsc_datetime_SCHEME 1 6_Corrected Form Resent to Optica Insura"], row["odsc_datetime_SCHEME 1 7_Rejected by Optica Insurance"]), axis=1)
    
    print("Completed Insurance Desk Working Hr Calculations")

    print("Converting Numerics for Insurance Desk")
    # Insurance Desk
    df['scheme_1_upld1_sentpreauth_diff'] = pd.to_numeric(df['scheme_1_upld1_sentpreauth_diff'])
    df['scheme_1_upld2_sentpreauth2_diff'] = pd.to_numeric(df['scheme_1_upld2_sentpreauth2_diff'])
    df['scheme_1_upld3_sentpreauth3_diff'] = pd.to_numeric(df['scheme_1_upld3_sentpreauth3_diff'])

    df['scheme_1_upld1_old_apprv_l_diff'] = pd.to_numeric(df['scheme_1_upld1_old_apprv_l_diff'])
    df['scheme_1_upld2_old_apprv2_l_diff'] = pd.to_numeric(df['scheme_1_upld2_old_apprv2_l_diff'])

    df['scheme_1_upld1_ins_rej_diff'] = pd.to_numeric(df['scheme_1_upld1_ins_rej_diff'])
    df['scheme_1_upld1_ins_info_diff'] = pd.to_numeric(df['scheme_1_upld1_ins_info_diff'])

    df['scheme_1_corrected_ins_info2_diff'] = pd.to_numeric(df['scheme_1_corrected_ins_info2_diff'])
    df['scheme_1_corrected2_ins_info3_diff'] = pd.to_numeric(df['scheme_1_corrected2_ins_info3_diff'])
    df['scheme_1_corrected3_ins_info4_diff'] = pd.to_numeric(df['scheme_1_corrected3_ins_info4_diff'])
    df['scheme_1_corrected4_ins_info5_diff'] = pd.to_numeric(df['scheme_1_corrected4_ins_info5_diff'])

    df['scheme_1_corrected_ins_rej2_diff'] = pd.to_numeric(df['scheme_1_corrected_ins_rej2_diff'])
    df['scheme_1_corrected2_ins_rej3_diff'] = pd.to_numeric(df['scheme_1_corrected2_ins_rej3_diff'])
    df['scheme_1_corrected3_ins_rej4_diff'] = pd.to_numeric(df['scheme_1_corrected3_ins_rej4_diff'])
    df['scheme_1_corrected4_ins_rej5_diff'] = pd.to_numeric(df['scheme_1_corrected4_ins_rej5_diff'])
    df['scheme_1_corrected5_ins_rej6_diff'] = pd.to_numeric(df['scheme_1_corrected5_ins_rej6_diff'])
    #df['scheme_1_corrected6_ins_rej7_diff'] = pd.to_numeric(df['scheme_1_corrected6_ins_rej7_diff'])

    query = """drop table mabawa_dw.fact_orderscreenc1_approvals;"""
    query = pg_execute(query)

    print("Dropped Facts Table")
    
    df.to_sql('fact_orderscreenc1_approvals', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    
    print("Recreated Facts Table")
    
    return "something"

def alter_fact_orderscreenc1_approvals():

    query = """
    alter table mabawa_dw.fact_orderscreenc1_approvals add column scheme_1_upld4_so_diff float8;
    alter table mabawa_dw.fact_orderscreenc1_approvals add column scheme_1_cust3_so_diff float8;
    alter table mabawa_dw.fact_orderscreenc1_approvals add column scheme_1_corrected7_so_diff float8;
    alter table mabawa_dw.fact_orderscreenc1_approvals add column scheme_1_cust3_rej3_diff float8;
    alter table mabawa_dw.fact_orderscreenc1_approvals add column scheme_1_upld4_rej4_diff float8;
    alter table mabawa_dw.fact_orderscreenc1_approvals add column scheme_1_corrected6_rej7_diff float8;
    alter table mabawa_dw.fact_orderscreenc1_approvals add column scheme_2_corrected3_so_diff float8;
    """
    query = pg_execute(query)
    
    #alter table mabawa_dw.fact_orderscreenc1_approvals add column scheme_1_upld4_so_diff float8;
    #alter table mabawa_dw.fact_orderscreenc1_approvals add column scheme_1_cust3_so_diff float8;
    #alter table mabawa_dw.fact_orderscreenc1_approvals add column scheme_1_corrected7_so_diff float8;
    #alter table mabawa_dw.fact_orderscreenc1_approvals add column scheme_1_cust3_rej3_diff float8;
    #alter table mabawa_dw.fact_orderscreenc1_approvals add column scheme_1_upld4_rej4_diff float8;
    #alter table mabawa_dw.fact_orderscreenc1_approvals add column scheme_1_corrected6_rej7_diff float8;
    #alter table mabawa_dw.fact_orderscreenc1_approvals add column "odsc_usr_dept_SCHEME 1 7_Rejected by Approvals Team" text;
    #alter table mabawa_dw.fact_orderscreenc1_approvals add column "scheme_type_SCHEME 1 7_Rejected by Approvals Team" text;
    #alter table mabawa_dw.fact_orderscreenc1_approvals add column "odsc_datetime_SCHEME 1 7_Rejected by Approvals Team" timestamp;
    #alter table mabawa_dw.fact_orderscreenc1_approvals add column "odsc_createdby_SCHEME 1 7_Rejected by Approvals Team" text;
    #alter table mabawa_dw.fact_orderscreenc1_approvals add column "odsc_datetime_SCHEME 1 7_Corrected Form Resent to Approvals Tea" timestamp;
    return "Successfully Indexed"

def index_fact_orderscreenc1_approvals():

    #add_key = """alter table mabawa_dw.fact_orderscreenc1_approvals add PRIMARY KEY (doc_entry)"""
    #add_key = pg_execute(add_key)

    add_indexes = """
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (doc_entry);
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_upld1_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_upld2_so_diff);
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_upld3_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_smart1_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_smart2_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_smart3_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_cust1_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_cust2_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_corrected1_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_corrected2_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_corrected3_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_corrected4_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_corrected5_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_corrected6_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_upld1_rej_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_smart1_rej_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_cust1_rej_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_upld2_rej2_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_smart2_rej2_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_corrected4_rej5_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_corrected5_rej6_diff);
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_upld1_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_upld2_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_upld3_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_smart1_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_smart2_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_smart3_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_cust1_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_cust2_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_cust2_rej2_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_corrected1_rej2_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_upld3_rej3_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_smart3_rej3_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_corrected2_rej3_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_corrected1_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_corrected2_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_corrected3_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_upld1_rej_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_smart1_rej_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_cust1_rej_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_upld2_rej2_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_smart2_rej2_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_cust2_rej2_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_corrected1_rej2_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_upld3_rej3_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_smart3_rej3_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_2_corrected2_rej3_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_upld4_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_cust3_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_corrected7_so_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_cust3_rej3_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_corrected6_rej7_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree (scheme_1_upld4_rej4_diff); 
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_datetime_SCHEME 1 1_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_datetime_SCHEME 1 2_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_datetime_SCHEME 1 3_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_datetime_SCHEME 1 4_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_datetime_SCHEME 1 5_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_datetime_SCHEME 1 6_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_datetime_SCHEME 1 7_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_createdby_SCHEME 1 1_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_createdby_SCHEME 1 2_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_createdby_SCHEME 1 3_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_createdby_SCHEME 1 4_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_createdby_SCHEME 1 5_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_createdby_SCHEME 1 6_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_createdby_SCHEME 1 7_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_usr_dept_SCHEME 1 1_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_usr_dept_SCHEME 1 2_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_usr_dept_SCHEME 1 3_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_usr_dept_SCHEME 1 4_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_usr_dept_SCHEME 1 5_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_usr_dept_SCHEME 1 6_Rejected by Approvals Team");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_approvals USING btree ("odsc_usr_dept_SCHEME 1 7_Rejected by Approvals Team");
    """
    add_indexes = pg_execute(add_indexes)

    return "something"

def create_fact_orderscreenc1_approvals_old():

    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)

    print("Fetched All Holidays")

    df = pd.read_sql("""
    SELECT *
    FROM mabawa_staging.source_orderscreenc1_approvals_trans;
    """, con=engine)

    print ("Fetched Order Data")

    try:
        df["odsc_datetime_1_Pre-Auth Initiated For Optica Insurance"] = pd.to_datetime(df["odsc_datetime_1_Pre-Auth Initiated For Optica Insurance"])
    except:
        print("error")

    try:
        df["odsc_datetime_1_Upload Attachment"] = pd.to_datetime(df["odsc_datetime_1_Upload Attachment"])
    except:
        print("error")

    try:
        df["odsc_datetime_1_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_1_Sent Pre-Auth to Insurance Company"])
    except:
        print("error")

    try:
        df["odsc_datetime_1_Rejected by Optica Insurance"] = pd.to_datetime(df["odsc_datetime_1_Rejected by Optica Insurance"])
    except:
        print("error")

    try:
        df["odsc_datetime_1_Insurance Company Needs Info from Branch"] = pd.to_datetime(df["odsc_datetime_1_Insurance Company Needs Info from Branch"])
    except:
        print("error")

    try:
        df["odsc_datetime_1_Used Old Approval for New Order Value Higher th"] = pd.to_datetime(df["odsc_datetime_1_Used Old Approval for New Order Value Higher th"])
    except:
        print("error")

    try:
        df["odsc_datetime_1_Used Old Approval for New Order Value Lower or "] = pd.to_datetime(df["odsc_datetime_1_Used Old Approval for New Order Value Lower or "])
    except:
        print("error")

    try:
        df["odsc_datetime_1_SMART Forwarded to Approvals Team"] = pd.to_datetime(df["odsc_datetime_1_SMART Forwarded to Approvals Team"])
    except:
        print("error")

    try:
        df["odsc_datetime_1_Customer Confirmed Order"] = pd.to_datetime(df["odsc_datetime_1_Customer Confirmed Order"])
    except:
        print("error")
    
    try:
        df["odsc_datetime_1_Sales Order Created"] = pd.to_datetime(df["odsc_datetime_1_Sales Order Created"])
    except:
        print("error")
    
    try:
        df["odsc_datetime_1_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_1_Rejected by Approvals Team"])
    except:
        print("error")

    try:
        df["odsc_datetime_1_Corrected Form Resent to Approvals Team"] = pd.to_datetime(df["odsc_datetime_1_Corrected Form Resent to Approvals Team"])
    except:
        print("error")
    
    try:
        df["odsc_datetime_1_Insurance Fully Approved"] = pd.to_datetime(df["odsc_datetime_1_Insurance Fully Approved"])
    except:
        print("error")
    
    try:
        df["odsc_datetime_1_Insurance Partially Approved"] = pd.to_datetime(df["odsc_datetime_1_Insurance Partially Approved"])
    except:
        print("error")




    
    try:
        df["odsc_datetime_2_Upload Attachment"] = pd.to_datetime(df["odsc_datetime_2_Upload Attachment"])
    except:
        print("error")

    try:
        df["odsc_datetime_2_SMART Forwarded to Approvals Team"] = pd.to_datetime(df["odsc_datetime_2_SMART Forwarded to Approvals Team"])
    except:
        print("error")

    try:
        df["odsc_datetime_2_Customer Confirmed Order"] = pd.to_datetime(df["odsc_datetime_2_Customer Confirmed Order"])
    except:
        print("error")

    try:
        df["odsc_datetime_2_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_2_Sent Pre-Auth to Insurance Company"])
    except:
        print("error")

    try:
        df["odsc_datetime_2_Rejected by Optica Insurance"] = pd.to_datetime(df["odsc_datetime_2_Rejected by Optica Insurance"])
    except:
        print("error")
    
    try:
        df["odsc_datetime_2_Insurance Company Needs Info from Branch"] = pd.to_datetime(df["odsc_datetime_2_Insurance Company Needs Info from Branch"])
    except:
        print("error")

    try:
        df["odsc_datetime_2_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_2_Rejected by Approvals Team"])
    except:
        print("error")

    try:
        df["odsc_datetime_2_Corrected Form Resent to Approvals Team"] = pd.to_datetime(df["odsc_datetime_2_Corrected Form Resent to Approvals Team"])
    except:
        print("error")

    try:
        df["odsc_datetime_2_Insurance Fully Approved"] = pd.to_datetime(df["odsc_datetime_2_Insurance Fully Approved"])
    except:
        print("error")

    try:
        df["odsc_datetime_2_Insurance Partially Approved"] = pd.to_datetime(df["odsc_datetime_2_Insurance Partially Approved"])
    except:
        print("error")



    

    try:
        df["odsc_datetime_3_Sent Pre-Auth to Insurance Company"] = pd.to_datetime(df["odsc_datetime_3_Sent Pre-Auth to Insurance Company"])
    except:
        print("error")

    try:
        df["odsc_datetime_3_Upload Attachment"] = pd.to_datetime(df["odsc_datetime_3_Upload Attachment"])
    except:
        print("error")

    try:
        df["odsc_datetime_3_SMART Forwarded to Approvals Team"] = pd.to_datetime(df["odsc_datetime_3_SMART Forwarded to Approvals Team"])
    except:
        print("error")

    try:
        df["odsc_datetime_3_Customer Confirmed Order"] = pd.to_datetime(df["odsc_datetime_3_Customer Confirmed Order"])
    except:
        print("error")

    try:
        df["odsc_datetime_3_Corrected Form Resent to Approvals Team"] = pd.to_datetime(df["odsc_datetime_3_Corrected Form Resent to Approvals Team"])
    except:
        print("error")

    try:
        df["odsc_datetime_3_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_3_Rejected by Approvals Team"])
    except:
        print("error")

    try:
        df["odsc_datetime_3_Insurance Company Needs Info from Branch"] = pd.to_datetime(df["odsc_datetime_3_Insurance Company Needs Info from Branch"])
    except:
        print("error")

    try:
        df["odsc_datetime_3_Insurance Fully Approved"] = pd.to_datetime(df["odsc_datetime_3_Insurance Fully Approved"])
    except:
        print("error")

    try:
        df["odsc_datetime_3_Insurance Partially Approved"] = pd.to_datetime(df["odsc_datetime_3_Insurance Partially Approved"])
    except:
        print("error")
    


    try:
        df["odsc_datetime_4_Upload Attachment"] = pd.to_datetime(df["odsc_datetime_4_Upload Attachment"])
    except:
        print("error")

    try:
        df["odsc_datetime_4_Corrected Form Resent to Approvals Team"] = pd.to_datetime(df["odsc_datetime_4_Corrected Form Resent to Approvals Team"])
    except:
        print("error")

    try:
        df["odsc_datetime_4_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_4_Rejected by Approvals Team"])
    except:
        print("error")

    try:
        df["odsc_datetime_4_Insurance Company Needs Info from Branch"] = pd.to_datetime(df["odsc_datetime_4_Insurance Company Needs Info from Branch"])
    except:
        print("error")




    
    try:
        df["odsc_datetime_5_Corrected Form Resent to Approvals Team"] = pd.to_datetime(df["odsc_datetime_5_Corrected Form Resent to Approvals Team"])
    except:
        print("error")

    try:
        df["odsc_datetime_5_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_5_Rejected by Approvals Team"])
    except:
        print("error")

    try:
        df["odsc_datetime_5_Insurance Company Needs Info from Branch"] = pd.to_datetime(df["odsc_datetime_5_Insurance Company Needs Info from Branch"])
    except:
        print("error")

    
    try:
        df["odsc_datetime_6_Corrected Form Resent to Approvals Team"] = pd.to_datetime(df["odsc_datetime_6_Corrected Form Resent to Approvals Team"])
    except:
        print("error")

    try:
        df["odsc_datetime_6_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_6_Rejected by Approvals Team"])
    except:
        print("error")



    try:
        df["odsc_datetime_7_Corrected Form Resent to Approvals Team"] = pd.to_datetime(df["odsc_datetime_7_Corrected Form Resent to Approvals Team"])
    except:
        print("error")

    try:
        df["odsc_datetime_7_Rejected by Approvals Team"] = pd.to_datetime(df["odsc_datetime_7_Rejected by Approvals Team"])
    except:
        print("error")
    
    print("Identified Dates")

    #df["odsc_datetime_2_Upload Attachment"] = pd.to_datetime(df["odsc_datetime_2_Upload Attachment"])
    #df["odsc_datetime_2_SMART Forwarded to Approvals Team"] = pd.to_datetime(df["odsc_datetime_2_SMART Forwarded to Approvals Team"])
    #df["odsc_datetime_2_Customer Confirmed Order"] = pd.to_datetime(df["odsc_datetime_2_Customer Confirmed Order"])
    
    # normal working hours
    workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(18),
        working_days=[0,1, 2, 3, 4])

    saturday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(16),
        working_days=[5])

    insurance_workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(8),
        end_time=datetime.time(17),
        working_days=[0,1, 2, 3, 4])
    
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    cal = Kenya()
    #hl = cal.holidays(2021)
    hl = data.values.tolist()
    #print(hl)
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    # normal working hours
    businesshrs = businesstimedelta.Rules([workday, saturday, holidays])

    # insurance working hours
    insbusinesshrs = businesstimedelta.Rules([insurance_workday, holidays])

    # normal working hours function
    def BusHrs(start, end):
        if end>=start:
            return float(businesshrs.difference(start,end).hours)*float(60)+float(businesshrs.difference(start,end).seconds)/float(60)
        else:
            return ""

    # insurance working hours function
    def InsHrs(start, end):
        if end>=start:
            return float(insbusinesshrs.difference(start,end).hours)*float(60)+float(insbusinesshrs.difference(start,end).seconds)/float(60)
        else:
            return ""
    
    # approvals confirmed
    try:
        df['upld1_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_Upload Attachment"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")

    try:
        df['upld2_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_2_Upload Attachment"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")

    try:
        df['upld3_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_3_Upload Attachment"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")

    try:
        df['upld4_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_4_Upload Attachment"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")

    try:
        df['smart1_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_SMART Forwarded to Approvals Team"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")

    try:
        df['smart2_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_2_SMART Forwarded to Approvals Team"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")

    try:
        df['smart3_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_3_SMART Forwarded to Approvals Team"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")

    try:
        df['cust1_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_Customer Confirmed Order"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")

    try:
        df['cust2_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_2_Customer Confirmed Order"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")

    try:
        df['cust3_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_3_Customer Confirmed Order"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")

    try:
        df['corrected1_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_Corrected Form Resent to Approvals Team"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")

    try:
        df['corrected2_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_2_Corrected Form Resent to Approvals Team"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")
    
    try:
        df['corrected3_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_3_Corrected Form Resent to Approvals Team"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")

    try:
        df['corrected4_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_4_Corrected Form Resent to Approvals Team"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")

    try:
        df['corrected5_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_5_Corrected Form Resent to Approvals Team"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")

    try:
        df['corrected6_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_6_Corrected Form Resent to Approvals Team"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")

    try:
        df['corrected7_so_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_7_Corrected Form Resent to Approvals Team"], row["odsc_datetime_1_Sales Order Created"]), axis=1)
    except:
        print("error")
    
    
    # approvals rejection 1
    try:
        df['upld1_rej_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_Upload Attachment"], row["odsc_datetime_1_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")

    try:
        df['smart1_rej_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_SMART Forwarded to Approvals Team"], row["odsc_datetime_1_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")

    try:
        df['cust1_rej_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_Customer Confirmed Order"], row["odsc_datetime_1_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")
    
    
    # approvals rejection 2
    try:
        df['upld2_rej2_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_2_Upload Attachment"], row["odsc_datetime_2_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")

    try:
        df['smart2_rej2_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_2_SMART Forwarded to Approvals Team"], row["odsc_datetime_2_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")

    try:
        df['cust2_rej2_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_2_Customer Confirmed Order"], row["odsc_datetime_2_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")

    try:
        df['corrected1_rej2_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_Corrected Form Resent to Approvals Team"], row["odsc_datetime_2_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")


    # approvals rejection 3
    try:
        df['upld3_rej3_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_3_Upload Attachment"], row["odsc_datetime_3_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")

    try:
        df['smart3_rej3_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_3_SMART Forwarded to Approvals Team"], row["odsc_datetime_3_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")

    try:
        df['cust3_rej3_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_3_Customer Confirmed Order"], row["odsc_datetime_3_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")

    try:
        df['corrected2_rej3_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_2_Corrected Form Resent to Approvals Team"], row["odsc_datetime_3_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")


    # approvals rejection 4
    try:
        df['upld4_rej4_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_4_Upload Attachment"], row["odsc_datetime_4_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")

    try:
        df['corrected3_rej4_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_3_Corrected Form Resent to Approvals Team"], row["odsc_datetime_4_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")

    try:
        df['corrected4_rej5_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_4_Corrected Form Resent to Approvals Team"], row["odsc_datetime_5_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")

    try:
        df['corrected5_rej6_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_5_Corrected Form Resent to Approvals Team"], row["odsc_datetime_6_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")

    try:
        df['corrected6_rej7_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_6_Corrected Form Resent to Approvals Team"], row["odsc_datetime_7_Rejected by Approvals Team"]), axis=1)
    except:
        print("error")
    
    
    # insurance desk
    try:
        df['upld1_sentpreauth_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_Upload Attachment"], row["odsc_datetime_1_Sent Pre-Auth to Insurance Company"]), axis=1)
    except:
        print("error")

    try:
        df['upld1_ins_rej_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_Upload Attachment"], row["odsc_datetime_1_Rejected by Optica Insurance"]), axis=1)
    except:
        print("error")

    try:
        df['upld1_ins_info_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_Upload Attachment"], row["odsc_datetime_1_Insurance Company Needs Info from Branch"]), axis=1)
    except:
        print("error")



    try:
        df['upld2_sentpreauth_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_2_Upload Attachment"], row["odsc_datetime_2_Sent Pre-Auth to Insurance Company"]), axis=1)
    except:
        print("error")

    try:
        df['upld2_ins_rej_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_2_Upload Attachment"], row["odsc_datetime_2_Rejected by Optica Insurance"]), axis=1)
    except:
        print("error")

    try:
        df['upld2_ins_info_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_2_Upload Attachment"], row["odsc_datetime_2_Insurance Company Needs Info from Branch"]), axis=1)
    except:
        print("error")

    try:
        df['upld1_old_apprv_h_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_Upload Attachment"], row["odsc_datetime_1_Used Old Approval for New Order Value Higher th"]), axis=1)
    except:
        print("error")
    
    try:
        df['upld1_old_apprv_l_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_1_Upload Attachment"], row["odsc_datetime_1_Used Old Approval for New Order Value Lower or "]), axis=1)
    except:
        print("error")
    

    # insurance companies
    try:
        df['preauth_f_apprvl']=df.apply(lambda row: InsHrs(row["odsc_datetime_1_Sent Pre-Auth to Insurance Company"], row["odsc_datetime_1_Insurance Fully Approved"]), axis=1)
    except:
        print("error")

    try:
        df['preauth_p_apprvl']=df.apply(lambda row: InsHrs(row["odsc_datetime_1_Sent Pre-Auth to Insurance Company"], row["odsc_datetime_1_Insurance Partially Approved"]), axis=1)
    except:
        print("error")


    print ("Calculated All Work Hours")


    # approvals confirmed
    try:
        df['upld1_so_diff'] = pd.to_numeric(df['upld1_so_diff'])
    except:
        print("error")

    try:
        df['upld2_so_diff'] = pd.to_numeric(df['upld2_so_diff'])
    except:
        print("error")

    try:
        df['upld3_so_diff'] = pd.to_numeric(df['upld3_so_diff'])
    except:
        print("error")

    try:
        df['upld4_so_diff'] = pd.to_numeric(df['upld4_so_diff'])
    except:
        print("error")

    try:
        df['smart1_so_diff'] = pd.to_numeric(df['smart1_so_diff'])
    except:
        print("error")

    try:
        df['smart2_so_diff'] = pd.to_numeric(df['smart2_so_diff'])
    except:
        print("error")

    try:
        df['smart3_so_diff'] = pd.to_numeric(df['smart3_so_diff'])
    except:
        print("error")

    try:
        df['cust1_so_diff'] = pd.to_numeric(df['cust1_so_diff'])
    except:
        print("error")

    try:
        df['cust2_so_diff'] = pd.to_numeric(df['cust2_so_diff'])
    except:
        print("error")

    try:
        df['cust3_so_diff'] = pd.to_numeric(df['cust3_so_diff'])
    except:
        print("error")

    try:
        df['corrected1_so_diff'] = pd.to_numeric(df['corrected1_so_diff'])
    except:
        print("error")

    try:
        df['corrected2_so_diff'] = pd.to_numeric(df['corrected2_so_diff'])
    except:
        print("error")

    try:
        df['corrected3_so_diff'] = pd.to_numeric(df['corrected3_so_diff'])
    except:
        print("error")

    try:
        df['corrected4_so_diff'] = pd.to_numeric(df['corrected4_so_diff'])
    except:
        print("error")

    try:
        df['corrected5_so_diff'] = pd.to_numeric(df['corrected5_so_diff'])
    except:
        print("error")

    try:
        df['corrected6_so_diff'] = pd.to_numeric(df['corrected6_so_diff'])
    except:
        print("error")

    try:
        df['corrected7_so_diff'] = pd.to_numeric(df['corrected7_so_diff'])
    except:
        print("error")
    
    
    

    # approvals rejections 1
    try:
        df['upld1_rej_diff'] = pd.to_numeric(df['upld1_rej_diff'])
    except:
        print("error")

    try:
        df['smart1_rej_diff'] = pd.to_numeric(df['smart1_rej_diff'])
    except:
        print("error")

    try:
        df['cust1_rej_diff'] = pd.to_numeric(df['cust1_rej_diff'])
    except:
        print("error")
    

    # approvals rejections 2
    try:
        df['upld2_rej2_diff'] = pd.to_numeric(df['upld2_rej2_diff'])
    except:
        print("error")

    try:
        df['smart2_rej2_diff'] = pd.to_numeric(df['smart2_rej2_diff'])
    except:
        print("error")

    try:
        df['cust2_rej2_diff'] = pd.to_numeric(df['cust2_rej2_diff'])
    except:
        print("error")

    try:
        df['corrected1_rej2_diff'] = pd.to_numeric(df['corrected1_rej2_diff'])
    except:
        print("error")


    # approvals rejections 3
    try:
        df['upld3_rej3_diff'] = pd.to_numeric(df['upld3_rej3_diff'])
    except:
        print("error")

    try:
        df['smart3_rej3_diff'] = pd.to_numeric(df['smart3_rej3_diff'])
    except:
        print("error")

    try:
        df['cust3_rej3_diff'] = pd.to_numeric(df['cust3_rej3_diff'])
    except:
        print("error")

    try:
        df['corrected2_rej3_diff'] = pd.to_numeric(df['corrected2_rej3_diff'])
    except:
        print("error")


    # approvals rejections 4
    try:
        df['upld4_rej4_diff'] = pd.to_numeric(df['upld4_rej4_diff'])
    except:
        print("error")

    try:
        df['corrected3_rej4_diff'] = pd.to_numeric(df['corrected3_rej4_diff'])
    except:
        print("error")

    try:
        df['corrected4_rej5_diff'] = pd.to_numeric(df['corrected4_rej5_diff'])
    except:
        print("error")

    try:
        df['corrected5_rej6_diff'] = pd.to_numeric(df['corrected5_rej6_diff'])
    except:
        print("error")

    try:
        df['corrected6_rej7_diff'] = pd.to_numeric(df['corrected6_rej7_diff'])
    except:
        print("error")
    

    # insurance desk
    try:
        df['upld1_sentpreauth_diff'] = pd.to_numeric(df['upld1_sentpreauth_diff'])
    except:
        print("error")

    try:
        df['upld1_ins_rej_diff'] = pd.to_numeric(df['upld1_ins_rej_diff'])
    except:
        print("error")

    try:
        df['upld1_ins_info_diff'] = pd.to_numeric(df['upld1_ins_info_diff'])
    except:
        print("error")

    try:
        df['upld2_sentpreauth_diff'] = pd.to_numeric(df['upld2_sentpreauth_diff'])
    except:
        print("error")

    try:
        df['upld2_ins_rej_diff'] = pd.to_numeric(df['upld2_ins_rej_diff'])
    except:
        print("error")

    try:
        df['upld2_ins_info_diff'] = pd.to_numeric(df['upld2_ins_info_diff'])
    except:
        print("error")

    try:
        df['upld1_old_apprv_h_diff'] = pd.to_numeric(df['upld1_old_apprv_h_diff'])
    except:
        print("error")

    try:
        df['upld1_old_apprv_l_diff'] = pd.to_numeric(df['upld1_old_apprv_l_diff'])
    except:
        print("error")
 

    print("Converted Numerics")

    #query = """drop table mabawa_dw.fact_orderscreenc1_approvals;"""
    #query = pg_execute(query)
    
    #df.to_sql('fact_orderscreenc1_approvals', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    
    print(df.columns)
    print(df['doc_entry'])

    df = df.set_index('doc_entry')

    print("Indexed DF")

    upsert(engine=engine,
       df=df,
       schema='mabawa_dw',
       table_name='fact_orderscreenc1_approvals',
       if_row_exists='update',
       create_table=False)
    
    print("Completed Upsert")
    
    return "something"

def v_all_approval_orders():

    add_key = """
    truncate mabawa_staging.all_approval_orders;
    insert into mabawa_staging.all_approval_orders
    SELECT doc_entry, odsc_doc_no
    FROM mabawa_staging.v_all_approval_orders;
    """
    add_key = pg_execute(add_key)

    return "something"

def order_rejection_count():

    query = """
    truncate mabawa_mviews.order_rejection_count;
    insert into mabawa_mviews.order_rejection_count
    SELECT distinct 
        a.doc_entry,
        (case when b.rejections is null then 0
        else b.rejections end) as rejections
    FROM mabawa_staging.source_orderscreenc1_approvals a 
    left join 
    (SELECT distinct doc_entry, max(rownum) as rejections
    FROM mabawa_staging.source_orderscreenc1_approvals
    where odsc_usr_dept = 'Approvals'
    and odsc_new_status = 'Rejected by Approvals Team'
    group by doc_entry) as b on a.doc_entry = b.doc_entry 
    where odsc_usr_dept = 'Approvals'
    """
    query = pg_execute(query)

    return "something"

def rejected_orders():

    query = """
    truncate mabawa_mviews.approvals_rejected_orders;
    insert into mabawa_mviews.approvals_rejected_orders
    SELECT doc_entry
    FROM mabawa_mviews.order_rejection_count
    where rejections >= 1
    """
    query = pg_execute(query)

    return "something"

def approvals_not_rejected():

    query = """
    truncate mabawa_mviews.approvals_not_rejected;
    insert into mabawa_mviews.approvals_not_rejected
    SELECT doc_entry
    FROM mabawa_mviews.order_rejection_count
    where rejections = 0
    """
    query = pg_execute(query)

    return "something"

def approvals_rejected_once():

    query = """
    truncate  mabawa_mviews.approvals_rejected_once;
    insert into mabawa_mviews.approvals_rejected_once
    SELECT doc_entry
    FROM mabawa_mviews.order_rejection_count
    where rejections = 1
    """
    query = pg_execute(query)

    return "something"

def approvals_rejected_twice():

    query = """
    truncate mabawa_mviews.approvals_rejected_twice;
    insert into mabawa_mviews.approvals_rejected_twice
    SELECT doc_entry
    FROM mabawa_mviews.order_rejection_count
    where rejections = 2
    """
    query = pg_execute(query)

    return "something"

def index_source_orderscreenc1_approvals():

    #add_key = """alter table mabawa_staging.source_orderscreenc1_approvals_trans add PRIMARY KEY (doc_entry)"""
    #add_key = pg_execute(add_key)
    testquery = """select 4"""
    testquery = pg_execute(testquery)

    return "Successfully Indexed"