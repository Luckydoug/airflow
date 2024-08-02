import sys
sys.path.append(".")
import requests
import datetime
import pandas as pd
import businesstimedelta
import holidays as pyholidays
from airflow.models import Variable
from datetime import date, timedelta
from workalendar.africa import Kenya
from pangres import upsert
from sub_tasks.data.connect import (pg_execute, engine) 
# from sub_tasks.api_login.api_login import(login)
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate

def fetch_sap_itr_logs ():
    SessionId = return_session_id(country = "Kenya")
   #SessionId = login()

    pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetITRLOGDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    pagecount_payload={}
    pagecount_headers = {}

    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    
    
    payload={}
    headers = {}
    pages = data['result']['body']['recs']['PagesCount']
    print("Retrived No of Pages")

    itr_log = pd.DataFrame()
    for i in range(1, pages+1):
        page = i
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetITRLOGDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()
        try:
            response = response['result']['body']['recs']['Results']
            response = pd.DataFrame.from_dict(response)
            response = response.T
            itr_log = itr_log.append(response, ignore_index=True)
        except:
            print('Error Unnesting Data')

    print("Created ITR Log DF")

    print("Renaming Columns")

    itr_log.rename (columns = {
            'Code':'code',
            'Date':'post_date',
            'Time':'post_time',
            'ITRLineNo':'itr_lineno',
            'ItemCode':'item_code',
            'Status':'status',
            'CreatedUser':'created_user',
            'Remarks':'remarks',
            'ITRNo':'itr_no'}
        ,inplace=True)
    
    print("Completed Renaming Columns")

    print("Setting Indexes")
    itr_log = itr_log.set_index('code')

    print("Initiating Upsert")
    upsert(engine=engine,
        df=itr_log,
        schema='mabawa_staging',
        table_name='source_itr_log',
        if_row_exists='update',
        create_table=False)

    print("Upsert Complete")
    
    #itr_log.to_sql('source_itr_log', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    return "Fetched ITR Logs"

# fetch_sap_itr_logs()

def create_mviews_source_itr_log ():

    query = """
    truncate mabawa_mviews.source_itr_log;
    insert into mabawa_mviews.source_itr_log
    SELECT itr_no, itr_lineno, item_code, post_date, post_time, status, created_user, remarks
    FROM mabawa_mviews.v_source_itr_log;
    """
    query = pg_execute(query)
    return "Complete"

def create_mviews_branchstock_itrlogs ():

    query = """
    truncate mabawa_mviews.branchstock_itrlogs;
    insert into mabawa_mviews.branchstock_itrlogs
    SELECT doc_no, itr_no, post_date, post_time_int, post_time, itr_lineno, 
    item_code, status, created_user, created_dept, remarks, dropped_status
    FROM mabawa_mviews.v_branchstock_itrlogs;
    """
    query = pg_execute(query)
    return "Complete"

def update_drop_status ():

    query = """
    update mabawa_mviews.branchstock_itrlogs itr 
    set dropped_status = 1 
    from mabawa_dw.dim_itr_issues its 
    where itr.doc_no::text = its.itr_no::text 
    and itr.created_dept ~~ any(array[its.issue_dept,its.issue_dept_2,its.issue_dept_3,its.issue_dept_4])
    """
    query = pg_execute(query)

    query2 = """
    update mabawa_mviews.branchstock_itrlogs itr 
    set dropped_status = 1 
    from mabawa_dw.dim_itr_time_issues its 
    where itr.post_time between its.issue_start_time and its.issue_end_time 
    and upper(itr.created_dept) = upper(its.issue_dept)
    """
    query2 = pg_execute(query2)

    query3 = """
    update mabawa_mviews.branchstock_itrlogs itr 
    set dropped_status = 1 
    from mabawa_dw.dim_itr_time_issues its 
    where itr.post_time between its.issue_start_time and its.issue_end_time 
    and upper(its.issue_dept) = 'ALL'
    """
    query3 = pg_execute(query3)
    return "Complete"

def create_mviews_salesorders_with_item_whse():

    query = """
    truncate mabawa_mviews.salesorders_with_item_whse;
    insert into mabawa_mviews.salesorders_with_item_whse
    SELECT sales_orderno, internal_number, order_canceled, document_status, warehouse_status, 
    posting_date, order_branch, id, item_no, warehouse_code
    FROM mabawa_mviews.v_salesorders_with_item_whse;
    """
    query = pg_execute(query)

    return "Complete"

def create_mviews_itr_whse_details():

    query = """
    truncate mabawa_mviews.itr_whse_details;
    insert into mabawa_mviews.itr_whse_details
    SELECT doc_internal_id, sales_orderno, sales_order_branch, item_no, warehouse_code
    FROM mabawa_mviews.v_itr_whse_details;
    """
    query = pg_execute(query)

    return "Complete"

def create_mviews_itr_with_details ():

    query = """
    truncate mabawa_mviews.itr_with_details;
    insert into mabawa_mviews.itr_with_details
    SELECT internal_no, doc_no, item_no, warehouse_code, picklist_created_datetime, remarks, dropped_status
    FROM mabawa_mviews.v_itr_with_details;
    """
    query = pg_execute(query)

    return "Fact Created"

def create_mviews_fact_branchstock_rep_itrlogs ():

    query = """
    truncate mabawa_mviews.fact_branchstock_rep_itrlogs;
    insert into mabawa_mviews.fact_branchstock_rep_itrlogs
    SELECT internal_no, doc_no, item_no, picklist_created_datetime, itr_remarks, 
    itr_no, post_date, post_time_int, post_time, post_datetime, itr_lineno, item_code, 
    warehouse_code, status, status_rownum, created_user, created_dept, log_remarks, dropped_status
    FROM mabawa_mviews.v_branchstock_rep_itrlogs;
    """
    query = pg_execute(query)

    return "Fact Created"

"""Creating Stores pipeline"""
def create_mviews_stores_branchstockrep_logs ():

    query = """
    truncate mabawa_mviews.stores_branchstockrep_logs;
    insert into mabawa_mviews.stores_branchstockrep_logs
    SELECT internal_no, doc_no, itr_no, picklist_created_datetime, 
    itr_remarks, post_date, post_datetime, itr_lineno, item_code, 
    warehouse_code, status, 
    status_rownum, created_user, created_dept, log_remarks, null as dropped_status
    FROM mabawa_mviews.v_stores_branchstockrep_logs;
    """
    query = pg_execute(query)

    return "Fact Created"

def update_stores_dropstatus ():

    query = """
    update mabawa_mviews.stores_branchstockrep_logs l 
    set dropped_status = 1 
    from mabawa_dw.dim_itr_issues its 
    where l.doc_no::text = its.itr_no::text
    and upper(its.issue_dept) like '%%STORE%%'
    """
    query = pg_execute(query)

    query1 = """
    update mabawa_mviews.stores_branchstockrep_logs l
    set dropped_status = 1 
    from mabawa_dw.dim_itr_time_issues itts 
    where l.picklist_created_datetime::date = itts.issue_date
    and l.picklist_created_datetime::time between itts.issue_start_time::time and itts.issue_end_time 
    and upper(itts.issue_dept) in ('ALL', 'DESIGNER STORE', 'LENS STORE', 'MAIN STORE')
    """
    query1 = pg_execute(query1)

    return "Fact Created"

def transpose_mviews_stores_branchstockrep_logs ():

    print ("Starting")

    df = pd.read_sql("""
    SELECT 
        internal_no, doc_no, itr_no, picklist_created_datetime, itr_remarks, 
        post_date, post_datetime, itr_lineno, item_code, warehouse_code, status, status_rownum, 
        created_user, created_dept, log_remarks, dropped_status
    FROM mabawa_mviews.stores_branchstockrep_logs
    """, con=engine)

    print("Fetched ITR Log Data")

    print("Pivoting Data")
    df = df.pivot_table(index=['internal_no', 'doc_no', 'itr_no', 'picklist_created_datetime', 'itr_lineno', 'item_code', 'warehouse_code'], columns=['status'], values=['post_datetime','created_user', 'created_dept', 'dropped_status', 'log_remarks'], aggfunc='min')
    print("Completed Pivoting Data")

    #df = df.pivot_table(index=['internal_no', 'doc_no', 'itr_no', 'picklist_created_datetime', 'itr_remarks', 'itr_lineno', 'item_code', 'warehouse_code'], columns=['status'], values=['post_datetime','created_user', 'created_dept', 'log_remarks', 'dropped_status'])
    
    def rename(col):
        if isinstance(col, tuple):
            col = '_'.join(str(c) for c in col)
        return col

    df.columns = map(rename, df.columns)
    #print(df)
    df=df.reset_index()
    #print(df)

    query = """drop table mabawa_mviews.stores_branchstockrep_logs_trans;"""
    query = pg_execute(query)

    df.to_sql('stores_branchstockrep_logs_trans', con = engine, schema='mabawa_mviews', if_exists = 'append', index=False)
    
    return "Fact Created"

def create_mviews_fact_stores_branchstockrep_logs ():

    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)
    
    print("Fetched Holidays")

    df = pd.read_sql("""
    SELECT 
        internal_no, doc_no, itr_no, picklist_created_datetime, null as itr_remarks, itr_lineno, item_code, warehouse_code, 
        "created_dept_Pick List Printed", "created_dept_Rep Sent to Control Room", "created_user_Pick List Printed", 
        "created_user_Rep Sent to Control Room", "dropped_status_Pick List Printed", 
        "dropped_status_Rep Sent to Control Room", "log_remarks_Pick List Printed", 
        "log_remarks_Rep Sent to Control Room", "post_datetime_Pick List Printed", 
        "post_datetime_Rep Sent to Control Room"
    FROM mabawa_mviews.stores_branchstockrep_logs_trans;
    """, con=engine)
    print("Fetched Stores ITR Log Data")

    df['picklist_created_datetime'] = pd.to_datetime(df['picklist_created_datetime'])
    df["post_datetime_Pick List Printed"] = pd.to_datetime(df["post_datetime_Pick List Printed"])
    df["post_datetime_Rep Sent to Control Room"] = pd.to_datetime(df["post_datetime_Rep Sent to Control Room"])

    print("Converted Dates")

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

    df['plc_plp_diff']=df.apply(lambda row: BusHrs(row["picklist_created_datetime"], row["post_datetime_Pick List Printed"]), axis=1)
    df['plp_stc_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Pick List Printed"], row["post_datetime_Rep Sent to Control Room"]), axis=1)
    df['plc_stc_diff']=df.apply(lambda row: BusHrs(row["picklist_created_datetime"], row["post_datetime_Rep Sent to Control Room"]), axis=1)
    
    df['plc_plp_diff'] = pd.to_numeric(df['plc_plp_diff'])
    df['plp_stc_diff'] = pd.to_numeric(df['plp_stc_diff'])
    df['plc_stc_diff'] = pd.to_numeric(df['plc_stc_diff'])

    query = """truncate mabawa_mviews.fact_stores_branchstockrep_logs;"""
    query = pg_execute(query)

    df.to_sql('fact_stores_branchstockrep_logs', con = engine, schema='mabawa_mviews', if_exists = 'append', index=False)
    return "Fact Created"

def create_fact_stores_branchstockrep_logs ():

    query = """
    truncate mabawa_dw.fact_stores_branchstockrep_logs;
    insert into mabawa_dw.fact_stores_branchstockrep_logs
    SELECT internal_no, doc_no, itr_no, picklist_created_datetime, itr_remarks, itr_lineno, item_code, warehouse_code, 
    "created_dept_Pick List Printed", "created_dept_Rep Sent to Control Room", "created_user_Pick List Printed", 
    "created_user_Rep Sent to Control Room", "dropped_status_Pick List Printed", "dropped_status_Rep Sent to Control Room", 
    "log_remarks_Pick List Printed", "log_remarks_Rep Sent to Control Room", "post_datetime_Pick List Printed", 
    "post_datetime_Rep Sent to Control Room", plc_plp_diff, plp_stc_diff, plc_stc_diff, main_store_first_cut, 
    main_store_second_cut, mainstore_cutoff_step, mainstore_cutoff_status, designer_store_first_cut, 
    designer_store_second_cut, designerstore_cutoff_step, designerstore_cutoff_status, lens_store_first_cut, 
    lens_store_second_cut, lenstore_cutoff_step, lensstore_cutoff_status
    FROM mabawa_mviews.v_fact_stores_branchstockrep_logs_3;
    """
    query = pg_execute(query)
    return "Complete"

"""Completed Stores pipeline"""

"""Creating Control pipeline"""
def create_mviews_control_branchstockrep_logs ():

    query = """
    truncate mabawa_mviews.control_branchstockrep_logs;
    insert into mabawa_mviews.control_branchstockrep_logs
    SELECT internal_no, doc_no, itr_no, picklist_created_datetime, itr_remarks, post_date, post_datetime, 
    itr_lineno, item_code, warehouse_code, status, status_rownum, created_user, created_dept, log_remarks, 
    null as dropped_status
    FROM mabawa_mviews.v_control_branchstockrep_logs;
    """
    query = pg_execute(query)

    return "Fact Created"

def update_control_dropstatus ():

    query = """
    update mabawa_mviews.control_branchstockrep_logs l 
    set dropped_status = 1 
    from mabawa_dw.dim_itr_issues its 
    where l.doc_no::text = its.itr_no::text
    and upper(its.issue_dept) = 'CONTROL ROOM'
    """
    query = pg_execute(query)

    query1 = """
    update mabawa_mviews.control_branchstockrep_logs l 
    set dropped_status = 1 
    from mabawa_dw.dim_itr_issues its 
    where l.doc_no::text = its.itr_no::text
    and upper(its.issue_dept_2) = 'CONTROL ROOM'
    """
    query1 = pg_execute(query1)

    query2 = """
    update mabawa_mviews.control_branchstockrep_logs l
    set dropped_status = 1 
    from mabawa_dw.dim_itr_time_issues itts 
    where l.post_datetime::date = itts.issue_date
    and l.status = 'Rep Sent to Control Room'
    and l.post_datetime::time  between itts.issue_start_time::time and itts.issue_end_time 
    and upper(itts.issue_dept) in ('ALL', 'CONTROL ROOM')
    """
    query2 = pg_execute(query2)

    return "Fact Created"

def transpose_mviews_control_branchstockrep_logs ():

    print ("Starting")

    df = pd.read_sql("""
    SELECT 
        internal_no, doc_no, itr_no, picklist_created_datetime, itr_remarks, 
        post_date, post_datetime, itr_lineno, item_code, warehouse_code, status, 
        status_rownum, created_user, created_dept, log_remarks, dropped_status
    FROM mabawa_mviews.control_branchstockrep_logs;
    """, con=engine)

    print("Fetched ITR Log Data")

    df = df.pivot_table(index=['internal_no', 'doc_no', 'itr_no', 'picklist_created_datetime', 'itr_lineno', 'item_code', 'warehouse_code'], columns=['status'], values=['post_datetime','created_user', 'created_dept', 'log_remarks', 'dropped_status'], aggfunc='min')
    
    def rename(col):
        if isinstance(col, tuple):
            col = '_'.join(str(c) for c in col)
        return col

    df.columns = map(rename, df.columns)
    #print(df)
    df=df.reset_index()
    #print(df)

    query = """drop table mabawa_mviews.control_branchstockrep_logs_trans;"""
    query = pg_execute(query)

    df.to_sql('control_branchstockrep_logs_trans', con = engine, schema='mabawa_mviews', if_exists = 'append', index=False)
    
    return "Fact Created"

def create_mviews_fact_control_branchstockrep_logs ():

    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)
    
    print("Fetched Holidays")

    df = pd.read_sql("""
    SELECT 
        internal_no, doc_no, itr_no, picklist_created_datetime, null as itr_remarks, itr_lineno, 
        item_code, warehouse_code, "created_dept_Rep Out of Stock", "created_dept_Rep Rejected by Control Room", 
        "created_dept_Rep Sent to Control Room", "created_dept_Rep Sent to Packaging", "created_user_Rep Out of Stock", 
        "created_user_Rep Rejected by Control Room", "created_user_Rep Sent to Control Room", 
        "created_user_Rep Sent to Packaging", "dropped_status_Rep Out of Stock", 
        "dropped_status_Rep Rejected by Control Room", "dropped_status_Rep Sent to Control Room", 
        "dropped_status_Rep Sent to Packaging", "log_remarks_Rep Out of Stock", 
        "log_remarks_Rep Rejected by Control Room", "log_remarks_Rep Sent to Control Room", 
        "log_remarks_Rep Sent to Packaging", "post_datetime_Rep Out of Stock", 
        "post_datetime_Rep Rejected by Control Room", "post_datetime_Rep Sent to Control Room", 
        "post_datetime_Rep Sent to Packaging"
    FROM mabawa_mviews.control_branchstockrep_logs_trans;
    """, con=engine)

    print("Fetched Stores ITR Log Data")

    df['picklist_created_datetime'] = pd.to_datetime(df['picklist_created_datetime'])
    df["post_datetime_Rep Sent to Control Room"] = pd.to_datetime(df["post_datetime_Rep Sent to Control Room"])
    df["post_datetime_Rep Sent to Packaging"] = pd.to_datetime(df["post_datetime_Rep Sent to Packaging"])

    print("Converted Dates")

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

    df['stc_stp_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Rep Sent to Control Room"], row["post_datetime_Rep Sent to Packaging"]), axis=1)
    
    df['stc_stp_diff'] = pd.to_numeric(df['stc_stp_diff'])

    query = """truncate mabawa_mviews.fact_control_branchstockrep_logs;"""
    query = pg_execute(query)

    df.to_sql('fact_control_branchstockrep_logs', con = engine, schema='mabawa_mviews', if_exists = 'append', index=False)
    return "Fact Created"

def create_fact_control_branchstockrep_logs ():

    query = """
    truncate mabawa_dw.fact_control_branchstockrep_logs;
    insert into mabawa_dw.fact_control_branchstockrep_logs
    SELECT internal_no, doc_no, itr_no, picklist_created_datetime, itr_remarks, itr_lineno, item_code, warehouse_code, 
    "created_dept_Rep Out of Stock", "created_dept_Rep Rejected by Control Room", "created_dept_Rep Sent to Control Room",
    "created_dept_Rep Sent to Packaging", "created_user_Rep Out of Stock", "created_user_Rep Rejected by Control Room", 
    "created_user_Rep Sent to Control Room", "created_user_Rep Sent to Packaging", "dropped_status_Rep Out of Stock", 
    "dropped_status_Rep Rejected by Control Room", "dropped_status_Rep Sent to Control Room", 
    "dropped_status_Rep Sent to Packaging", "log_remarks_Rep Out of Stock", "log_remarks_Rep Rejected by Control Room", 
    "log_remarks_Rep Sent to Control Room", "log_remarks_Rep Sent to Packaging", "post_datetime_Rep Out of Stock", 
    "post_datetime_Rep Rejected by Control Room", "post_datetime_Rep Sent to Control Room", 
    "post_datetime_Rep Sent to Packaging", stc_stp_diff, control_first_cut, control_second_cut, control_cutoff_step, 
    control_cutoff_status
    FROM mabawa_mviews.v_fact_control_branchstockrep_logs_3;
    """
    query = pg_execute(query)
    return "Complete"

"""Completed Control pipeline"""

"""Creating Packaging pipeline"""
def create_mviews_packaging_branchstockrep_logs ():

    query = """
    truncate mabawa_mviews.packaging_branchstockrep_logs;
    insert into mabawa_mviews.packaging_branchstockrep_logs
    SELECT internal_no, doc_no, itr_no, picklist_created_datetime, itr_remarks, post_date, post_datetime, 
    itr_lineno, item_code, warehouse_code, status, status_rownum, created_user, created_dept, log_remarks, 
    null as dropped_status
    FROM mabawa_mviews.v_packaging_branchstockrep_logs;
    """
    query = pg_execute(query)

    return "Fact Created"

def update_packaging_dropstatus ():

    query = """
    update mabawa_mviews.packaging_branchstockrep_logs l 
    set dropped_status = 1 
    from mabawa_dw.dim_itr_issues its 
    where l.doc_no::text = its.itr_no::text
    and upper(its.issue_dept) = 'PACKAGING'
    """
    query = pg_execute(query)

    query1 = """
    update mabawa_mviews.packaging_branchstockrep_logs l
    set dropped_status = 1 
    from mabawa_dw.dim_itr_time_issues itts 
    where l.post_datetime::date = itts.issue_date
    and l.status = 'Rep Sent to Branch'
    and l.post_datetime::time  between itts.issue_start_time::time and itts.issue_end_time 
    and upper(itts.issue_dept) in ('ALL', 'PACKAGING')
    """
    query1 = pg_execute(query1)

    return "Fact Created"

def transpose_mviews_packaging_branchstockrep_logs ():

    print ("Starting")

    df = pd.read_sql("""
    SELECT 
        internal_no, doc_no, itr_no, picklist_created_datetime, itr_remarks, post_date, post_datetime, itr_lineno, 
        item_code, warehouse_code, status, status_rownum, created_user, created_dept, log_remarks, dropped_status
    FROM mabawa_mviews.packaging_branchstockrep_logs;
    """, con=engine)

    print("Fetched ITR Log Data")

    df = df.pivot_table(index=['internal_no', 'doc_no', 'itr_no', 'picklist_created_datetime', 'itr_lineno', 'item_code', 'warehouse_code'], columns=['status'], values=['post_datetime','created_user', 'created_dept', 'log_remarks', 'dropped_status'], aggfunc='min')
    
    def rename(col):
        if isinstance(col, tuple):
            col = '_'.join(str(c) for c in col)
        return col

    df.columns = map(rename, df.columns)
    #print(df)
    df=df.reset_index()
    #print(df)

    query = """drop table mabawa_mviews.packaging_branchstockrep_logs_trans;"""
    query = pg_execute(query)

    df.to_sql('packaging_branchstockrep_logs_trans', con = engine, schema='mabawa_mviews', if_exists = 'append', index=False)
    
    return "Fact Created"

def create_mviews_fact_packaging_branchstockrep_logs ():

    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)
    
    print("Fetched Holidays")

    df = pd.read_sql("""
    SELECT 
        internal_no, doc_no, itr_no, picklist_created_datetime, null as itr_remarks, itr_lineno, item_code, warehouse_code, 
        "created_dept_Rep Sent to Branch", "created_dept_Rep Sent to Packaging", "created_user_Rep Sent to Branch", 
        "created_user_Rep Sent to Packaging", "dropped_status_Rep Sent to Branch", "dropped_status_Rep Sent to Packaging",
        "log_remarks_Rep Sent to Branch", "log_remarks_Rep Sent to Packaging", "post_datetime_Rep Sent to Branch", 
        "post_datetime_Rep Sent to Packaging"
    FROM mabawa_mviews.packaging_branchstockrep_logs_trans;
    """, con=engine)

    print("Fetched Stores ITR Log Data")
    
    df["post_datetime_Rep Sent to Packaging"] = pd.to_datetime(df["post_datetime_Rep Sent to Packaging"])
    df["post_datetime_Rep Sent to Branch"] = pd.to_datetime(df["post_datetime_Rep Sent to Branch"])
    

    print("Converted Dates")

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

    df['stp_stb_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Rep Sent to Packaging"], row["post_datetime_Rep Sent to Branch"]), axis=1)
    
    df['stp_stb_diff'] = pd.to_numeric(df['stp_stb_diff'])

    query = """truncate mabawa_mviews.fact_packaging_branchstockrep_logs;"""
    query = pg_execute(query)

    df.to_sql('fact_packaging_branchstockrep_logs', con = engine, schema='mabawa_mviews', if_exists = 'append', index=False)
    return "Fact Created"

def create_fact_packaging_branchstockrep_logs ():

    query = """
    truncate mabawa_dw.fact_packaging_branchstockrep_logs;
    insert into mabawa_dw.fact_packaging_branchstockrep_logs
    SELECT internal_no, doc_no, itr_no, picklist_created_datetime, itr_remarks, itr_lineno, item_code, warehouse_code, 
    "created_dept_Rep Sent to Branch", "created_dept_Rep Sent to Packaging", "created_user_Rep Sent to Branch", 
    "created_user_Rep Sent to Packaging", "dropped_status_Rep Sent to Branch", "dropped_status_Rep Sent to Packaging", 
    "log_remarks_Rep Sent to Branch", "log_remarks_Rep Sent to Packaging", "post_datetime_Rep Sent to Branch", 
    "post_datetime_Rep Sent to Packaging", stp_stb_diff, packaging_first_cut, packaging_second_cut, 
    packaging_cutoff_step, packaging_cutoff_status
    FROM mabawa_mviews.v_fact_packaging_branchstockrep_logs_3;
    """
    query = pg_execute(query)
    return "Complete"

"""Creating all dept pipeline"""
def create_mviews_all_itr_logs ():

    query = """
    truncate mabawa_mviews.all_itr_logs;
    insert into mabawa_mviews.all_itr_logs
    SELECT 
        itr_no, itr_lineno, item_code, post_date, post_time_int, post_time, status, created_user, created_dept, 
        created_branch_code, remarks
    FROM mabawa_mviews.v_all_itr_logs;
    """
    query = pg_execute(query)
    return "Something"

def update_branch_codes ():

    query = """
    update mabawa_mviews.all_itr_logs l 
    set created_branch_code = upper(left(created_user::text,3))
    where created_dept is null
    """
    query = pg_execute(query)
    return "Something"

def create_mviews_all_itrs ():

    query = """
    truncate mabawa_mviews.all_itrs;
    insert into mabawa_mviews.all_itrs
    SELECT doc_internal_id, doc_no
    FROM mabawa_mviews.v_all_itrs;
    """
    query = pg_execute(query)
    return "Something"

def create_mviews_all_itrs_createdate ():

    query = """
    truncate mabawa_mviews.all_itrs_createdate;
    insert into mabawa_mviews.all_itrs_createdate
    SELECT doc_internal_id, doc_no, create_date, creation_datetime, create_datetime
    FROM mabawa_mviews.v_all_itrs_createdate;
    """
    query = pg_execute(query)
    return "Something"

def create_mviews_all_itr_logs_2 ():

    query = """
    truncate mabawa_mviews.all_itr_logs_2;
    insert into mabawa_mviews.all_itr_logs_2
    SELECT 
        distinct
        itr_no, itr.doc_no, itr_lineno, item_code, post_date, post_time_int, post_time, 
        (post_date::date||' '||post_time::time)::timestamp as post_datetime,
        status, created_user, created_dept, created_branch_code, remarks,
        null as dropped
    FROM mabawa_mviews.all_itr_logs itl
    left join mabawa_mviews.all_itrs itr on itl.itr_no::text = itr.doc_internal_id::text
    """
    query = pg_execute(query)
    return "Something"

def create_mviews_itr_sourcestore ():

    query = """
    truncate mabawa_mviews.itrs_source_store;
    insert into mabawa_mviews.itrs_source_store
    SELECT doc_no, itr_no, created_dept
    FROM mabawa_mviews.v_itrs_source_store;
    """
    query = pg_execute(query)

    return "Something"

def update_all_dropstatus ():

    query = """
    update mabawa_mviews.all_itr_logs_2 l 
    set dropped = '1' 
    from mabawa_dw.dim_itr_issues its 
    where l.doc_no::text = its.itr_no::text
    and upper(its.issue_dept) = upper(l.created_dept) 
    """
    query = pg_execute(query)

    query1 = """
    update mabawa_mviews.all_itr_logs_2 l 
    set dropped = '1' 
    from mabawa_dw.dim_itr_issues its 
    where l.doc_no::text = its.itr_no::text
    and upper(its.issue_dept_2) = upper(l.created_dept)
    """
    query1 = pg_execute(query1)

    query2 = """
    update mabawa_mviews.all_itr_logs_2 l 
    set dropped = '1' 
    from mabawa_dw.dim_itr_issues its 
    where l.doc_no::text = its.itr_no::text
    and upper(its.issue_dept_3) = upper(l.created_dept) 
    """
    query2 = pg_execute(query2)

    query3 = """
    update mabawa_mviews.all_itr_logs_2 l
    set dropped  = 1 
    from mabawa_dw.dim_itr_time_issues itts 
    where l.post_datetime::date = itts.issue_date::date
    and l.post_datetime::time  between itts.issue_start_time::time and itts.issue_end_time 
    and upper(itts.issue_dept) = upper(l.created_dept)
    """
    query3 = pg_execute(query3)

    query4 = """
    update mabawa_mviews.all_itr_logs_2 l
    set dropped  = 1 
    from mabawa_dw.dim_itr_time_issues itts 
    where l.post_datetime::date = itts.issue_date::date
    and l.post_datetime::time  between itts.issue_start_time::time and itts.issue_end_time 
    and upper(itts.issue_dept) = ('ALL')
    """
    query4 = pg_execute(query4)

    return "Fact Created"

def transpose_mviews_all_itr_logs ():

    print ("Starting")

    df = pd.read_sql("""
    SELECT 
        itr_no, doc_no, itr_lineno, item_code, post_date, post_time_int, 
        post_time, post_datetime, status, created_user, created_dept, 
        created_branch_code, remarks, dropped
    FROM mabawa_mviews.all_itr_logs_2
    where post_date::date >= (current_date-interval'2 day')::date
    --where post_date::date >= '2022-01-01'
    --and post_date::date <= '2022-01-31'
    """, con=engine)

    print("Fetched ITR Log Data")

    print("Initializing Pivoting")

    df = df.pivot_table(index=['itr_no', 'doc_no', 'itr_lineno', 'item_code'], columns=['status'], values=['post_datetime', 'created_user', 'created_dept', 'created_branch_code', 'remarks', 'dropped'], aggfunc='min')
    
    print("Pivoting Complete")

    print("Renaming Columns")
    def rename(col):
        if isinstance(col, tuple):
            col = '_'.join(str(c) for c in col)
        return col

    df.columns = map(rename, df.columns)
    df=df.reset_index()

    # query = """drop table mabawa_mviews.all_itr_logs_trans;"""
    # query = pg_execute(query)

    # print("Inserting Data")
    # df.to_sql('all_itr_logs_trans', con = engine, schema='mabawa_mviews', if_exists = 'append', index=False)

    df = df.set_index(['itr_no','itr_lineno', 'item_code'])

    upsert(engine=engine,
       df=df,
       schema='mabawa_mviews',
       table_name='all_itr_logs_trans',
       if_row_exists='update',
       create_table=False,
       add_new_columns=True)
    
    return "Fact Created"
    
def update_missing_dates ():

    query = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Return List Printed" = "post_datetime_Return Sent to HQ" 
    where "post_datetime_Return List Printed" is null
    and "post_datetime_Return Sent to HQ" is not null
    """
    query = pg_execute(query)

    query1 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Return Sent to HQ" = "post_datetime_Return List Printed"  
    where "post_datetime_Return Sent to HQ"  is null
    and "post_datetime_Return List Printed" is not null
    """
    query1 = pg_execute(query1)

    query2 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Return Received at HQ"  = "post_datetime_Return Sent to Control Room"  
    where "post_datetime_Return Received at HQ"  is null
    and "post_datetime_Return Sent to Control Room" is not null
    """
    query2 = pg_execute(query2)

    query3 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Return Sent to Control Room"  = "post_datetime_Return Received at HQ"   
    where "post_datetime_Return Sent to Control Room" is null
    and "post_datetime_Return Received at HQ" is not null
    """
    query3 = pg_execute(query3)

    query4 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Exchange List Printed"  = "post_datetime_Exchange Sent to HQ"   
    where "post_datetime_Exchange List Printed" is null
    and "post_datetime_Exchange Sent to HQ" is not null
    """
    query4 = pg_execute(query4)

    query5 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Exchange Sent to HQ" = "post_datetime_Exchange List Printed"  
    where "post_datetime_Exchange Sent to HQ" is null
    and "post_datetime_Exchange List Printed" is not null
    """
    query5 = pg_execute(query5)

    query6 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Exchange Received at HQ" = "post_datetime_Exchange Sent to Control Room"  
    where "post_datetime_Exchange Received at HQ"  is null
    and "post_datetime_Exchange Sent to Control Room" is not null
    """
    query6 = pg_execute(query6)

    query7 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Exchange Sent to Control Room" = "post_datetime_Exchange Received at HQ"  
    where "post_datetime_Exchange Sent to Control Room" is null
    and "post_datetime_Exchange Received at HQ" is not null
    """
    query7 = pg_execute(query7)

    query8 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Exchange Received at Control Room" = "post_datetime_Exchange Sent to Default Warehouse"  
    where "post_datetime_Exchange Received at Control Room" is null
    and "post_datetime_Exchange Sent to Default Warehouse" is not null
    """
    query8 = pg_execute(query8)

    query9 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Exchange Sent to Default Warehouse" = "post_datetime_Exchange Received at Control Room"  
    where "post_datetime_Exchange Sent to Default Warehouse" is null
    and "post_datetime_Exchange Received at Control Room" is not null
    """
    query9 = pg_execute(query9)

    query10 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Warranty Return List Printed"  = "post_datetime_Warranty Return Sent to HQ"  
    where "post_datetime_Warranty Return List Printed" is null
    and "post_datetime_Warranty Return Sent to HQ" is not null
    """
    query10 = pg_execute(query10)

    query11 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Warranty Return Sent to HQ" = "post_datetime_Warranty Return List Printed"  
    where "post_datetime_Warranty Return Sent to HQ" is null
    and "post_datetime_Warranty Return List Printed" is not null
    """
    query11 = pg_execute(query11)

    query12 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Warranty Return Received at HQ"  = "post_datetime_Warranty Return Sent to Control Room"  
    where "post_datetime_Warranty Return Received at HQ" is null
    and "post_datetime_Warranty Return Sent to Control Room" is not null
    """
    query12 = pg_execute(query12)

    query13 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Warranty Return Sent to Control Room" = "post_datetime_Warranty Return Received at HQ"  
    where "post_datetime_Warranty Return Sent to Control Room" is null
    and "post_datetime_Warranty Return Received at HQ" is not null
    """
    query13 = pg_execute(query13)

    query14 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Damage Received at HQ" = "post_datetime_Damage Sent to Control Rooom"  
    where "post_datetime_Damage Received at HQ" is null
    and "post_datetime_Damage Sent to Control Rooom" is not null
    """
    query14 = pg_execute(query14)

    query15 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Damage Sent to Control Rooom" = "post_datetime_Damage Received at HQ"  
    where "post_datetime_Damage Sent to Control Rooom" is null
    and "post_datetime_Damage Received at HQ" is not null
    """
    query15 = pg_execute(query15)

    query16 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Damage Received at Control Room" = "post_datetime_Damage Sent to Damage Store"  
    where "post_datetime_Damage Received at Control Room" is null
    and "post_datetime_Damage Sent to Damage Store" is not null
    """
    query16 = pg_execute(query16)

    query17 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Damage Sent to Damage Store" = "post_datetime_Damage Received at Control Room"   
    where "post_datetime_Damage Sent to Damage Store" is null
    and "post_datetime_Damage Received at Control Room" is not null
    """
    query17 = pg_execute(query17)

    query18 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Identifier Printed" = "post_datetime_Borrow Sent to HQ"  
    where "post_datetime_Borrow Identifier Printed" is null
    and "post_datetime_Borrow Sent to HQ" is not null
    """
    query18 = pg_execute(query18)

    query19 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Sent to HQ" = "post_datetime_Borrow Identifier Printed"  
    where "post_datetime_Borrow Sent to HQ" is null
    and "post_datetime_Borrow Identifier Printed" is not null
    """
    query19 = pg_execute(query19)

    query20 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Received at HQ" = "post_datetime_Borrow Sent to HQ Control Room By Receiver"  
    where "post_datetime_Borrow Received at HQ" is null
    and "post_datetime_Borrow Sent to HQ Control Room By Receiver" is not null
    """
    query20 = pg_execute(query20)

    query21 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Received at HQ Control Room From Receiver" = "post_datetime_Borrow Sent to Packaging" 
    where "post_datetime_Borrow Received at HQ Control Room From Receiver" is null
    and "post_datetime_Borrow Sent to Packaging" is not null
    """
    query21 = pg_execute(query21)

    query20 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Sent to Packaging" = "post_datetime_Borrow Received at HQ Control Room From Receiver" 
    where "post_datetime_Borrow Sent to Packaging" is null
    and "post_datetime_Borrow Received at HQ Control Room From Receiver" is not null
    """
    query20 = pg_execute(query20)

    query21 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Received at Branch" = "post_datetime_Borrow OK"  
    where "post_datetime_Borrow Received at Branch" is null
    and "post_datetime_Borrow OK" is not null
    """
    query21 = pg_execute(query21)

    query22 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow OK" = "post_datetime_Borrow Received at Branch"  
    where "post_datetime_Borrow OK" is null
    and "post_datetime_Borrow Received at Branch" is not null
    """
    query22 = pg_execute(query22)

    query23 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Identifier Printed" = "post_datetime_Borrow Sent to HQ Control Room"  
    where "post_datetime_Borrow Identifier Printed" is null
    and "post_datetime_Borrow Sent to HQ Control Room" is not null
    """
    query23 = pg_execute(query23)

    query24 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Sent to HQ Control Room" = "post_datetime_Borrow Identifier Printed"  
    where "post_datetime_Borrow Sent to HQ Control Room" is null
    and "post_datetime_Borrow Identifier Printed" is not null
    """
    query24 = pg_execute(query24)

    query25 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Received at HQ Control Room" = "post_datetime_Borrow Sent to Packaging at HQ"  
    where "post_datetime_Borrow Received at HQ Control Room" is null
    and "post_datetime_Borrow Sent to Packaging at HQ" is not null
    """
    query25 = pg_execute(query25)

    query26 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Sent to Packaging at HQ" = "post_datetime_Borrow Received at HQ Control Room"  
    where "post_datetime_Borrow Sent to Packaging at HQ" is null
    and "post_datetime_Borrow Received at HQ Control Room" is not null
    """
    query26 = pg_execute(query26)

    query27 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Received at Packaging at HQ"  = "post_datetime_Borrow Sent to Branch From HQ"  
    where "post_datetime_Borrow Received at Packaging at HQ" is null
    and "post_datetime_Borrow Sent to Branch From HQ" is not null
    """
    query27 = pg_execute(query27)

    query28 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Sent to Branch From HQ" = "post_datetime_Borrow Received at Packaging at HQ"  
    where "post_datetime_Borrow Sent to Branch From HQ" is null
    and "post_datetime_Borrow Received at Packaging at HQ" is not null
    """
    query28 = pg_execute(query28)

    query29 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Received at Branch" = "post_datetime_Borrow OK"  
    where "post_datetime_Borrow Received at Branch" is null
    and "post_datetime_Borrow OK" is not null
    """
    query29 = pg_execute(query29)

    query30 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow OK" = "post_datetime_Borrow Received at Branch"  
    where "post_datetime_Borrow OK" is null
    and "post_datetime_Borrow Received at Branch" is not null
    """
    query30 = pg_execute(query30)

    query31 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Return Received at Control Room" = "post_datetime_Return Sent to Default Warehouse"  
    where "post_datetime_Return Received at Control Room" is null
    and "post_datetime_Return Sent to Default Warehouse" is not null
    """
    query31 = pg_execute(query31)

    query32 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Return Sent to Default Warehouse" = "post_datetime_Return Received at Control Room"  
    where "post_datetime_Return Sent to Default Warehouse" is null
    and "post_datetime_Return Received at Control Room" is not null
    """
    query32 = pg_execute(query32)

    query33 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Warranty Return Received at Control Room" = "post_datetime_Warranty Return Not Correct Item OK"  
    where "post_datetime_Warranty Return Received at Control Room" is null
    and "post_datetime_Warranty Return Not Correct Item OK" is not null
    """
    query33 = pg_execute(query33)

    query34 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Warranty Return Not Correct Item OK" = "post_datetime_Warranty Return Received at Control Room"  
    where "post_datetime_Warranty Return Not Correct Item OK" is null
    and "post_datetime_Warranty Return Received at Control Room" is not null
    """
    query34 = pg_execute(query34)

    query35 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Warranty Return Received at Control Room"  = "post_datetime_Warranty Return Sent to Damage Store"  
    where "post_datetime_Warranty Return Received at Control Room" is null
    and "post_datetime_Warranty Return Sent to Damage Store" is not null
    """
    query35 = pg_execute(query35)

    query36 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Warranty Return Sent to Damage Store" = "post_datetime_Warranty Return Received at Control Room"  
    where "post_datetime_Warranty Return Sent to Damage Store" is null
    and "post_datetime_Warranty Return Received at Control Room" is not null
    """
    query36 = pg_execute(query36)

    query37 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Damage List Printed"  = "post_datetime_Damage Sent to HQ"  
    where "post_datetime_Damage List Printed" is null
    and "post_datetime_Damage Sent to HQ" is not null
    """
    query37 = pg_execute(query37)

    query38 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Damage Sent to HQ" = "post_datetime_Damage List Printed"  
    where "post_datetime_Damage Sent to HQ" is null
    and "post_datetime_Damage List Printed" is not null
    """
    query38 = pg_execute(query38)

    query39 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Sent to Branch at HQ" = "post_datetime_Borrow Received at Branch" 
    where "post_datetime_Borrow Sent to Branch at HQ" is null
    and "post_datetime_Borrow Received at Branch" is not null
    """
    query39 = pg_execute(query39)

    query40 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Sent to Packaging" = "post_datetime_Borrow Received at Packaging"  
    where "post_datetime_Borrow Sent to Packaging" is null
    and "post_datetime_Borrow Received at Packaging" is not null
    """
    query40 = pg_execute(query40)

    query41 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Sent to HQ Control Room By Receiver" = "post_datetime_Borrow Received at HQ"  
    where "post_datetime_Borrow Sent to HQ Control Room By Receiver" is null
    and "post_datetime_Borrow Received at HQ" is not null
    """
    query41 = pg_execute(query41)

    query42 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Borrow Sent to HQ Control Room By Receiver" = "post_datetime_Borrow Received at HQ Control Room From Receiver"  
    where "post_datetime_Borrow Sent to HQ Control Room By Receiver" is null
    and "post_datetime_Borrow Received at HQ Control Room From Receiver" is not null
    """
    query42 = pg_execute(query42)

    query43 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "post_datetime_Exchange Sent to Default Warehouse" = '2022-06-21 14:52:00.000',
    "post_datetime_Exchange Received at Default Warehouse" = '2022-06-21 16:34:00.000'
    where doc_no = 227000054
    """
    query43 = pg_execute(query43)

    return "Something"

def update_missing_dates_dept ():

    query1 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Return Received at HQ" = "created_dept_Return Sent to Control Room"  
    where "created_dept_Return Received at HQ"  is null
    and "created_dept_Return Sent to Control Room" is not null
    """
    query1 = pg_execute(query1)

    query2 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Return Sent to Control Room" = "created_dept_Return Received at HQ"   
    where "created_dept_Return Sent to Control Room" is null
    and "created_dept_Return Received at HQ" is not null
    """
    query2 = pg_execute(query2)

    query3 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Exchange Received at HQ" = "created_dept_Exchange Sent to Control Room" 
    where "created_dept_Exchange Received at HQ"  is null
    and "created_dept_Exchange Sent to Control Room" is not null
    """
    query3 = pg_execute(query3)

    query4 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Exchange Sent to Control Room" = "created_dept_Exchange Received at HQ" 
    where "created_dept_Exchange Sent to Control Room" is null
    and "created_dept_Exchange Received at HQ" is not null
    """
    query4 = pg_execute(query4)

    query5 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Exchange Received at Control Room" = "created_dept_Exchange Sent to Default Warehouse" 
    where "created_dept_Exchange Received at Control Room" is null
    and "created_dept_Exchange Sent to Default Warehouse" is not null
    """
    query5 = pg_execute(query5)

    query6 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Exchange Sent to Default Warehouse" = "created_dept_Exchange Received at Control Room" 
    where "created_dept_Exchange Sent to Default Warehouse" is null
    and "created_dept_Exchange Received at Control Room" is not null
    """
    query6 = pg_execute(query6)

    query7 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Warranty Return Received at HQ" = "created_dept_Warranty Return Sent to Control Room" 
    where "created_dept_Warranty Return Received at HQ" is null
    and "created_dept_Warranty Return Sent to Control Room" is not null
    """
    query7 = pg_execute(query7)

    query8 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Warranty Return Sent to Control Room" = "created_dept_Warranty Return Received at HQ" 
    where "created_dept_Warranty Return Sent to Control Room" is null
    and "created_dept_Warranty Return Received at HQ" is not null
    """
    query8 = pg_execute(query8)

    query9 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Damage Received at HQ" = "created_dept_Damage Sent to Control Rooom"  
    where "created_dept_Damage Received at HQ" is null
    and "created_dept_Damage Sent to Control Rooom" is not null
    """
    query9 = pg_execute(query9)

    query10 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Damage Sent to Control Rooom" = "created_dept_Damage Received at HQ"  
    where "created_dept_Damage Sent to Control Rooom" is null
    and "created_dept_Damage Received at HQ" is not null
    """
    query10 = pg_execute(query10)

    query11 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Damage Received at Control Room"  = "created_dept_Damage Sent to Damage Store"  
    where "created_dept_Damage Received at Control Room" is null
    and "created_dept_Damage Sent to Damage Store" is not null
    """
    query11 = pg_execute(query11)

    query12 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Damage Sent to Damage Store" = "created_dept_Damage Received at Control Room"  
    where "created_dept_Damage Sent to Damage Store" is null
    and "created_dept_Damage Received at Control Room" is not null
    """
    query12 = pg_execute(query12)

    query13 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Borrow Received at HQ"  = "created_dept_Borrow Sent to HQ Control Room By Receiver"  
    where "created_dept_Borrow Received at HQ" is null
    and "created_dept_Borrow Sent to HQ Control Room By Receiver" is not null
    """
    query13 = pg_execute(query13)

    query14 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Borrow Sent to HQ Control Room By Receiver" = "created_dept_Borrow Received at HQ"  
    where "created_dept_Borrow Sent to HQ Control Room By Receiver" is null
    and "created_dept_Borrow Received at HQ" is not null
    """
    query14 = pg_execute(query14)

    query15 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Borrow Sent to Packaging" = "created_dept_Borrow Received at HQ Control Room From Receiver" 
    where "created_dept_Borrow Sent to Packaging" is null
    and "created_dept_Borrow Received at HQ Control Room From Receiver" is not null
    """
    query15 = pg_execute(query15)

    query16 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Borrow Received at HQ Control Room From Receiver" = "created_dept_Borrow Sent to Packaging" 
    where "created_dept_Borrow Received at HQ Control Room From Receiver" is null
    and "created_dept_Borrow Sent to Packaging" is not null
    """
    query16 = pg_execute(query16)

    query17 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Borrow Received at HQ Control Room"  = "created_dept_Borrow Sent to Packaging at HQ"  
    where "created_dept_Borrow Received at HQ Control Room" is null
    and "created_dept_Borrow Sent to Packaging at HQ" is not null
    """
    query17 = pg_execute(query17)

    query18 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Borrow Sent to Packaging at HQ" = "created_dept_Borrow Received at HQ Control Room"  
    where "created_dept_Borrow Sent to Packaging at HQ" is null
    and "created_dept_Borrow Received at HQ Control Room" is not null
    """
    query18 = pg_execute(query18)

    query19 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Borrow Received at Packaging at HQ" = "created_dept_Borrow Sent to Branch From HQ"  
    where "created_dept_Borrow Received at Packaging at HQ" is null
    and "created_dept_Borrow Sent to Branch From HQ" is not null
    """
    query19 = pg_execute(query19)

    query20 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Borrow Sent to Branch From HQ" = "created_dept_Borrow Received at Packaging at HQ"  
    where "created_dept_Borrow Sent to Branch From HQ" is null
    and "created_dept_Borrow Received at Packaging at HQ" is not null
    """
    query20 = pg_execute(query20)

    query21 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Return Received at Control Room"  = "created_dept_Return Sent to Default Warehouse"  
    where "created_dept_Return Received at Control Room" is null
    and "created_dept_Return Sent to Default Warehouse" is not null
    """
    query21 = pg_execute(query21)

    query22 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Return Sent to Default Warehouse" = "created_dept_Return Received at Control Room"  
    where "created_dept_Return Sent to Default Warehouse" is null
    and "created_dept_Return Received at Control Room" is not null
    """
    query22 = pg_execute(query22)

    query23 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Warranty Return Received at Control Room" = "created_dept_Warranty Return Not Correct Item OK" 
    where "created_dept_Warranty Return Received at Control Room" is null
    and "created_dept_Warranty Return Not Correct Item OK" is not null
    """
    query23 = pg_execute(query23)

    query24 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Warranty Return Not Correct Item OK" = "created_dept_Warranty Return Received at Control Room" 
    where "created_dept_Warranty Return Not Correct Item OK" is null
    and "created_dept_Warranty Return Received at Control Room" is not null
    """
    query24 = pg_execute(query24)

    query25 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Warranty Return Received at Control Room" = "created_dept_Warranty Return Sent to Damage Store"  
    where "created_dept_Warranty Return Received at Control Room" is null
    and "created_dept_Warranty Return Sent to Damage Store" is not null
    """
    query25 = pg_execute(query25)

    query26 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Warranty Return Sent to Damage Store" = "created_dept_Warranty Return Received at Control Room"  
    where "created_dept_Warranty Return Sent to Damage Store" is null
    and "created_dept_Warranty Return Received at Control Room" is not null
    """
    query26 = pg_execute(query26)

    query27 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Borrow Sent to Branch at HQ"  = 'PACKAGING'
    where "post_datetime_Borrow Sent to Branch at HQ" is not null
    and "created_dept_Borrow Sent to Branch at HQ" is null
    """
    query27 = pg_execute(query27)

    query28 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Borrow Sent to Packaging"  = "created_dept_Borrow Received at Packaging" 
    where "created_dept_Borrow Sent to Packaging" is null
    and "created_dept_Borrow Received at Packaging" is not null
    """
    query28 = pg_execute(query28)

    query29 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Borrow Sent to HQ Control Room By Receiver"  = 'CONTROL ROOM' 
    where "post_datetime_Borrow Sent to HQ Control Room By Receiver" is not null
    and "created_dept_Borrow Sent to HQ Control Room By Receiver" is null
    """
    query29 = pg_execute(query29)

    query30 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Borrow Sent to HQ Control Room By Receiver" = "created_dept_Borrow Received at HQ Control Room From Receiver"  
    where "created_dept_Borrow Sent to HQ Control Room By Receiver" is null
    and "created_dept_Borrow Received at HQ Control Room From Receiver" is not null
    """
    query30 = pg_execute(query30)

    query31 = """
    update mabawa_mviews.all_itr_logs_trans itr
    set "created_dept_Exchange Received at Default Warehouse" = 'MAIN STORE'
    where doc_no = 227000054
    """
    query31 = pg_execute(query31)

    return "Something"

def create_mviews_fact_all_itr_logs ():

    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)
    
    print("Fetched Holidays")

    df = pd.read_sql("""
    SELECT 
        itc.create_datetime, itr.*
    FROM mabawa_mviews.all_itr_logs_trans itr
    join mabawa_mviews.all_itrs_createdate itc on itr.itr_no::text = itc.doc_internal_id::text
    and itr.doc_no::text = itc.doc_no::text
    where itc.create_datetime::date >= (current_date-interval'5 day')::date
    --where itr.doc_no = 227000054
    """, con=engine)

    print(df)

    print("Fetched All ITR Log Data")

    print("Initializing Date Conversion")
    
    # Create Date
    df["create_datetime"] = pd.to_datetime(df["create_datetime"])
    
    # Additional Stock
    df["post_datetime_Additional Stock List Printed"] = pd.to_datetime(df["post_datetime_Additional Stock List Printed"])
    df["post_datetime_Additional Stock Sent to Control Room"] = pd.to_datetime(df["post_datetime_Additional Stock Sent to Control Room"])
    df["post_datetime_Additional Stock Sent to Packaging"] = pd.to_datetime(df["post_datetime_Additional Stock Sent to Packaging"])
    df["post_datetime_Additional Stock Sent to Branch"] = pd.to_datetime(df["post_datetime_Additional Stock Sent to Branch"])

    #Stock Return
    df["post_datetime_Return Sent to HQ"] = pd.to_datetime(df["post_datetime_Return Sent to HQ"])
    df["post_datetime_Return Received at HQ"] = pd.to_datetime(df["post_datetime_Return Received at HQ"])
    df["post_datetime_Return Sent to Control Room"] = pd.to_datetime(df["post_datetime_Return Sent to Control Room"])
    df["post_datetime_Return Received at Control Room"] = pd.to_datetime(df["post_datetime_Return Received at Control Room"])
    df["post_datetime_Return Sent to Default Warehouse"] = pd.to_datetime(df["post_datetime_Return Sent to Default Warehouse"])
    df["post_datetime_Return Received at Default Warehouse"] = pd.to_datetime(df["post_datetime_Return Received at Default Warehouse"])


    #Stock Exchange
    df["post_datetime_Exchange Sent to HQ"] = pd.to_datetime(df["post_datetime_Exchange Sent to HQ"])
    df["post_datetime_Exchange Received at HQ"] = pd.to_datetime(df["post_datetime_Exchange Received at HQ"])
    df["post_datetime_Exchange Sent to Control Room"] = pd.to_datetime(df["post_datetime_Exchange Sent to Control Room"])
    df["post_datetime_Exchange Received at Control Room"] = pd.to_datetime(df["post_datetime_Exchange Received at Control Room"])
    df["post_datetime_Exchange Sent to Default Warehouse"] = pd.to_datetime(df["post_datetime_Exchange Sent to Default Warehouse"])
    df["post_datetime_Exchange Received at Default Warehouse"] = pd.to_datetime(df["post_datetime_Exchange Received at Default Warehouse"])

    #Warranty Return
    df["post_datetime_Warranty Return Sent to HQ"] = pd.to_datetime(df["post_datetime_Warranty Return Sent to HQ"])
    df["post_datetime_Warranty Return Received at HQ"] = pd.to_datetime(df["post_datetime_Warranty Return Received at HQ"])
    df["post_datetime_Warranty Return Sent to Control Room"] = pd.to_datetime(df["post_datetime_Warranty Return Sent to Control Room"])
    df["post_datetime_Warranty Return Received at Control Room"] = pd.to_datetime(df["post_datetime_Warranty Return Received at Control Room"])
    df["post_datetime_Warranty Return Not Correct Item OK"] = pd.to_datetime(df["post_datetime_Warranty Return Not Correct Item OK"])
    df["post_datetime_Warranty Return Sent to Damage Store"] = pd.to_datetime(df["post_datetime_Warranty Return Sent to Damage Store"])
    
    #Damage Stock
    df["post_datetime_Damage Sent to HQ"] = pd.to_datetime(df["post_datetime_Damage Sent to HQ"])
    df["post_datetime_Damage Received at HQ"] = pd.to_datetime(df["post_datetime_Damage Received at HQ"])
    df["post_datetime_Damage Sent to Control Rooom"] = pd.to_datetime(df["post_datetime_Damage Sent to Control Rooom"])
    df["post_datetime_Damage Received at Control Room"] = pd.to_datetime(df["post_datetime_Damage Received at Control Room"])
    df["post_datetime_Damage Sent to Damage Store"] = pd.to_datetime(df["post_datetime_Damage Sent to Damage Store"])

    #Stock Borrow
    df["post_datetime_Borrow Identifier Printed"] = pd.to_datetime(df["post_datetime_Borrow Identifier Printed"])
    df["post_datetime_Borrow Sent to HQ Control Room"] = pd.to_datetime(df["post_datetime_Borrow Sent to HQ Control Room"])
    df["post_datetime_Borrow Received at HQ Control Room"] = pd.to_datetime(df["post_datetime_Borrow Received at HQ Control Room"])
    df["post_datetime_Borrow Sent to Packaging at HQ"] = pd.to_datetime(df["post_datetime_Borrow Sent to Packaging at HQ"])
    df["post_datetime_Borrow Received at Packaging at HQ"] = pd.to_datetime(df["post_datetime_Borrow Received at Packaging at HQ"])
    df["post_datetime_Borrow Sent to Branch From HQ"] = pd.to_datetime(df["post_datetime_Borrow Sent to Branch From HQ"])
    df["post_datetime_Borrow Sent to Branch"] = pd.to_datetime(df["post_datetime_Borrow Sent to Branch"])

    #Stock Borrow Branch
    df["post_datetime_Borrow Sent to HQ"] = pd.to_datetime(df["post_datetime_Borrow Sent to HQ"])
    df["post_datetime_Borrow Received at HQ"] = pd.to_datetime(df["post_datetime_Borrow Received at HQ"])
    df["post_datetime_Borrow Sent to HQ Control Room By Receiver"] = pd.to_datetime(df["post_datetime_Borrow Sent to HQ Control Room By Receiver"])
    df["post_datetime_Borrow Received at HQ Control Room From Receiver"] = pd.to_datetime(df["post_datetime_Borrow Received at HQ Control Room From Receiver"])
    df["post_datetime_Borrow Sent to Packaging"] = pd.to_datetime(df["post_datetime_Borrow Sent to Packaging"])
    df["post_datetime_Borrow Received at Packaging"] = pd.to_datetime(df["post_datetime_Borrow Received at Packaging"])
    df["post_datetime_Borrow Sent to Branch at HQ"] = pd.to_datetime(df["post_datetime_Borrow Sent to Branch at HQ"])

    print("Completed Date Conversion")

    print("Setting UP Business Hours Details")

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
    print("Business Hours Successfully Defined")

    print("Initializing Business Hours Calculation")
    #Additional Stock
    df['alc_alp_diff']=df.apply(lambda row: BusHrs(row["create_datetime"], row["post_datetime_Additional Stock List Printed"]), axis=1)
    df['alp_alstc_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Additional Stock List Printed"], row["post_datetime_Additional Stock Sent to Control Room"]), axis=1)
    df['alc_alstc_diff']=df.apply(lambda row: BusHrs(row["create_datetime"], row["post_datetime_Additional Stock Sent to Control Room"]), axis=1)
    df['alstc_alstp_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Additional Stock Sent to Control Room"], row["post_datetime_Additional Stock Sent to Packaging"]), axis=1)
    df['alstp_alstb_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Additional Stock Sent to Packaging"], row["post_datetime_Additional Stock Sent to Branch"]), axis=1)
    
    print("Calculated Business Hours for Additional Stock")

    #Stock Return
    df['rshq_rrhq_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Return Sent to HQ"], row["post_datetime_Return Received at HQ"]), axis=1)
    df['rrhq_rstc_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Return Received at HQ"], row["post_datetime_Return Sent to Control Room"]), axis=1)
    df['rshq_rstc_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Return Sent to HQ"], row["post_datetime_Return Sent to Control Room"]), axis=1)
    df['rstc_rrac_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Return Sent to Control Room"], row["post_datetime_Return Received at Control Room"]), axis=1)
    df['rrac_rsdw_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Return Received at Control Room"], row["post_datetime_Return Sent to Default Warehouse"]), axis=1)
    df['rstc_rsdw_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Return Sent to Control Room"], row["post_datetime_Return Sent to Default Warehouse"]), axis=1)
    df['rsdw_rrdw_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Return Sent to Default Warehouse"], row["post_datetime_Return Received at Default Warehouse"]), axis=1)
    
    print("Calculated Business Hours for Stock Return")

    #Stock Exchange
    df['eshq_erhq_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Exchange Sent to HQ"], row["post_datetime_Exchange Received at HQ"]), axis=1)
    df['erhq_estc_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Exchange Received at HQ"], row["post_datetime_Exchange Sent to Control Room"]), axis=1)
    df['eshq_estc_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Exchange Sent to HQ"], row["post_datetime_Exchange Sent to Control Room"]), axis=1)
    df['estc_erac_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Exchange Sent to Control Room"], row["post_datetime_Exchange Received at Control Room"]), axis=1)
    df['erac_esdw_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Exchange Received at Control Room"], row["post_datetime_Exchange Sent to Default Warehouse"]), axis=1)
    df['estc_esdw_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Exchange Sent to Control Room"], row["post_datetime_Exchange Sent to Default Warehouse"]), axis=1)
    df['esdw_erdw_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Exchange Sent to Default Warehouse"], row["post_datetime_Exchange Received at Default Warehouse"]), axis=1)
    
    print("Calculated Business Hours for Stock Exchange")

    #Warranty Return
    df['wshq_wrhq_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Warranty Return Sent to HQ"], row["post_datetime_Warranty Return Received at HQ"]), axis=1)
    df['wrhq_wstc_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Warranty Return Received at HQ"], row["post_datetime_Warranty Return Sent to Control Room"]), axis=1)
    df['wshq_wstc_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Warranty Return Sent to HQ"], row["post_datetime_Warranty Return Sent to Control Room"]), axis=1)
    df['wstc_wrac_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Warranty Return Sent to Control Room"], row["post_datetime_Warranty Return Received at Control Room"]), axis=1)
    df['wrac_wnc_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Warranty Return Received at Control Room"], row["post_datetime_Warranty Return Not Correct Item OK"]), axis=1)
    df['wrac_wsds_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Warranty Return Received at Control Room"], row["post_datetime_Warranty Return Sent to Damage Store"]), axis=1)
    df['wstc_wsds_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Warranty Return Sent to Control Room"], row["post_datetime_Warranty Return Sent to Damage Store"]), axis=1)
    df['wnc_wrac_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Warranty Return Not Correct Item OK"], row["post_datetime_Warranty Return Received at Control Room"]), axis=1)
    
    print("Calculated Business Hours for Warranty Return")

    #Damage Stock
    df['dshq_drhq_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Damage Sent to HQ"], row["post_datetime_Damage Received at HQ"]), axis=1)
    df['drhq_dstc_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Damage Received at HQ"], row["post_datetime_Damage Sent to Control Rooom"]), axis=1)
    df['dshq_dstc_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Damage Sent to HQ"], row["post_datetime_Damage Sent to Control Rooom"]), axis=1)
    df['dstc_drac_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Damage Sent to Control Rooom"], row["post_datetime_Damage Received at Control Room"]), axis=1)
    df['drac_dsds_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Damage Received at Control Room"], row["post_datetime_Damage Sent to Damage Store"]), axis=1)
    df['dstc_dsds_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Damage Sent to Control Rooom"], row["post_datetime_Damage Sent to Damage Store"]), axis=1)
    
    print("Calculated Business Hours for Damage Stock")

    #Stock Borrow
    df['blc_bip_diff']=df.apply(lambda row: BusHrs(row["create_datetime"], row["post_datetime_Borrow Identifier Printed"]), axis=1)
    df['bip_bshqc_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Identifier Printed"], row["post_datetime_Borrow Sent to HQ Control Room"]), axis=1)
    df['bic_bshqc_diff']=df.apply(lambda row: BusHrs(row["create_datetime"], row["post_datetime_Borrow Sent to HQ Control Room"]), axis=1)
    df['bshqc_brhqc_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Sent to HQ Control Room"], row["post_datetime_Borrow Received at HQ Control Room"]), axis=1)
    df['brhqc_bstphq_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Received at HQ Control Room"], row["post_datetime_Borrow Sent to Packaging at HQ"]), axis=1)
    df['bshqc_bstphq_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Sent to HQ Control Room"], row["post_datetime_Borrow Sent to Packaging at HQ"]), axis=1)
    df['bstphq_braphq_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Sent to Packaging at HQ"], row["post_datetime_Borrow Received at Packaging at HQ"]), axis=1)
    df['braphq_bstbfhq_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Received at Packaging at HQ"], row["post_datetime_Borrow Sent to Branch From HQ"]), axis=1)
    df['braphq_bstb_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Received at Packaging at HQ"], row["post_datetime_Borrow Sent to Branch"]), axis=1)
    df['bstphq_bstbfhq_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Sent to Packaging at HQ"], row["post_datetime_Borrow Sent to Branch From HQ"]), axis=1)
    
    print("Calculated Business Hours for Stock Borrow")

    #Stock Borrow Branch
    df['bshq_brhq_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Sent to HQ"], row["post_datetime_Borrow Received at HQ"]), axis=1)
    df['brhq_bshqcbr_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Received at HQ"], row["post_datetime_Borrow Sent to HQ Control Room By Receiver"]), axis=1)
    df['bshq_bshqcbr_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Sent to HQ"], row["post_datetime_Borrow Sent to HQ Control Room By Receiver"]), axis=1)
    df['bshqcbr_brahqcfr_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Sent to HQ Control Room By Receiver"], row["post_datetime_Borrow Received at HQ Control Room From Receiver"]), axis=1)
    df['brahqcfr_bstp_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Received at HQ Control Room From Receiver"], row["post_datetime_Borrow Sent to Packaging"]), axis=1)
    df['bshqcbr_bstp_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Sent to HQ Control Room By Receiver"], row["post_datetime_Borrow Sent to Packaging"]), axis=1)
    df['bstp_brap_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Sent to Packaging"], row["post_datetime_Borrow Received at Packaging"]), axis=1)
    df['brap_bstbahq_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Received at Packaging"], row["post_datetime_Borrow Sent to Branch at HQ"]), axis=1)
    df['brap_bstb_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Received at Packaging"], row["post_datetime_Borrow Sent to Branch"]), axis=1)
    df['bstp_bstbahq_diff']=df.apply(lambda row: BusHrs(row["post_datetime_Borrow Sent to Packaging"], row["post_datetime_Borrow Sent to Branch at HQ"]), axis=1)
    
    print("Calculated Business Hours for Stock Borrow Branch")
    print("Completed Business Hours Calculation")

    print("Initializing Conversion of New Columns into Numeric")

    #Additional Stock
    df['alc_alp_diff'] = pd.to_numeric(df['alc_alp_diff'])
    df['alp_alstc_diff'] = pd.to_numeric(df['alp_alstc_diff'])
    df['alc_alstc_diff'] = pd.to_numeric(df['alc_alstc_diff'])
    df['alstc_alstp_diff'] = pd.to_numeric(df['alstc_alstp_diff'])
    df['alstp_alstb_diff'] = pd.to_numeric(df['alstp_alstb_diff'])

    print("Converted New Columns into Numeric for Additional Stock")

    #Stock Return
    df['rshq_rrhq_diff'] = pd.to_numeric(df['rshq_rrhq_diff'])
    df['rrhq_rstc_diff'] = pd.to_numeric(df['rrhq_rstc_diff'])
    df['rshq_rstc_diff'] = pd.to_numeric(df['rshq_rstc_diff'])
    df['rstc_rrac_diff'] = pd.to_numeric(df['rstc_rrac_diff'])
    df['rrac_rsdw_diff'] = pd.to_numeric(df['rrac_rsdw_diff'])
    df['rstc_rsdw_diff'] = pd.to_numeric(df['rstc_rsdw_diff'])
    df['rsdw_rrdw_diff'] = pd.to_numeric(df['rsdw_rrdw_diff'])

    print("Converted New Columns into Numeric for Stock Return")

    #Stock Exchange
    df['eshq_erhq_diff'] = pd.to_numeric(df['eshq_erhq_diff'])
    df['erhq_estc_diff'] = pd.to_numeric(df['erhq_estc_diff'])
    df['eshq_estc_diff'] = pd.to_numeric(df['eshq_estc_diff'])
    df['estc_erac_diff'] = pd.to_numeric(df['estc_erac_diff'])
    df['erac_esdw_diff'] = pd.to_numeric(df['erac_esdw_diff'])
    df['estc_esdw_diff'] = pd.to_numeric(df['estc_esdw_diff'])
    df['esdw_erdw_diff'] = pd.to_numeric(df['esdw_erdw_diff'])

    print("Converted New Columns into Numeric for Stock Exchange")

    #Warranty Return
    df['wshq_wrhq_diff'] = pd.to_numeric(df['wshq_wrhq_diff'])
    df['wrhq_wstc_diff'] = pd.to_numeric(df['wrhq_wstc_diff'])
    df['wshq_wstc_diff'] = pd.to_numeric(df['wshq_wstc_diff'])
    df['wstc_wrac_diff'] = pd.to_numeric(df['wstc_wrac_diff'])
    df['wrac_wnc_diff'] = pd.to_numeric(df['wrac_wnc_diff'])
    df['wrac_wsds_diff'] = pd.to_numeric(df['wrac_wsds_diff'])
    df['wstc_wsds_diff'] = pd.to_numeric(df['wstc_wsds_diff'])
    df['wnc_wrac_diff'] = pd.to_numeric(df['wnc_wrac_diff'])

    print("Converted New Columns into Numeric for Warranty Return")

    #Damage Stock
    df['dshq_drhq_diff'] = pd.to_numeric(df['dshq_drhq_diff'])
    df['drhq_dstc_diff'] = pd.to_numeric(df['drhq_dstc_diff'])
    df['dshq_dstc_diff'] = pd.to_numeric(df['dshq_dstc_diff'])
    df['dstc_drac_diff'] = pd.to_numeric(df['dstc_drac_diff'])
    df['drac_dsds_diff'] = pd.to_numeric(df['drac_dsds_diff'])
    df['dstc_dsds_diff'] = pd.to_numeric(df['dstc_dsds_diff'])

    print("Converted New Columns into Numeric for Damage Stock")

    #Stock Borrow
    df['blc_bip_diff'] = pd.to_numeric(df['blc_bip_diff'])
    df['bip_bshqc_diff'] = pd.to_numeric(df['bip_bshqc_diff'])
    df['bic_bshqc_diff'] = pd.to_numeric(df['bic_bshqc_diff'])
    df['bshqc_brhqc_diff'] = pd.to_numeric(df['bshqc_brhqc_diff'])
    df['brhqc_bstphq_diff'] = pd.to_numeric(df['brhqc_bstphq_diff'])
    df['bshqc_bstphq_diff'] = pd.to_numeric(df['bshqc_bstphq_diff'])
    df['bstphq_braphq_diff'] = pd.to_numeric(df['bstphq_braphq_diff'])
    df['braphq_bstbfhq_diff'] = pd.to_numeric(df['braphq_bstbfhq_diff'])
    df['braphq_bstb_diff'] = pd.to_numeric(df['braphq_bstb_diff'])
    df['bstphq_bstbfhq_diff'] = pd.to_numeric(df['bstphq_bstbfhq_diff'])

    print("Converted New Columns into Numeric for Stock Borrow")

    #Stock Borrow Branch
    df['bshq_brhq_diff'] = pd.to_numeric(df['bshq_brhq_diff'])
    df['brhq_bshqcbr_diff'] = pd.to_numeric(df['brhq_bshqcbr_diff'])
    df['bshq_bshqcbr_diff'] = pd.to_numeric(df['bshq_bshqcbr_diff'])
    df['bshqcbr_brahqcfr_diff'] = pd.to_numeric(df['bshqcbr_brahqcfr_diff'])
    df['brahqcfr_bstp_diff'] = pd.to_numeric(df['brahqcfr_bstp_diff'])
    df['bshqcbr_bstp_diff'] = pd.to_numeric(df['bshqcbr_bstp_diff'])
    df['bstp_brap_diff'] = pd.to_numeric(df['bstp_brap_diff'])
    df['brap_bstbahq_diff'] = pd.to_numeric(df['brap_bstbahq_diff'])
    df['brap_bstb_diff'] = pd.to_numeric(df['brap_bstb_diff'])
    df['bstp_bstbahq_diff'] = pd.to_numeric(df['bstp_bstbahq_diff'])


    print("Converted New Columns into Numeric for Stock Borow Branch")
    print("Completed Conversion of New Columns into Numeric")

    #query = """drop table mabawa_mviews.fact_all_itr_logs;"""
    #query = pg_execute(query)

    print("Inserting Data")

    # df.to_sql('fact_all_itr_logs', con = engine, schema='mabawa_mviews', if_exists = 'append', index=False)

    df = df.set_index(['itr_no','itr_lineno', 'item_code'])

    upsert(engine=engine,
       df=df,
       schema='mabawa_mviews',
       table_name='fact_all_itr_logs',
       if_row_exists='update',
       create_table=False,
       add_new_columns=True)

    return "Fact Created"

def create_fact_all_itr_logs ():

    query = """
    drop table mabawa_dw.fact_all_itr_logs;
    create table mabawa_dw.fact_all_itr_logs as
    SELECT 
        *
    FROM mabawa_mviews.fact_all_itr_logs;
    """
    query=pg_execute(query)
    
    return "Created"

def update_fact_all_itr_logs_dropped ():

    # dropping items
    query = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Return Sent to Default Warehouse" = '1',
    "dropped_Return Received at Default Warehouse" = '1',
    "created_dept_Return Sent to Default Warehouse" = 'MAIN STORE',
    "created_dept_Return Received at Default Warehouse" = 'MAIN STORE' 
    where doc_no = 22202368   
    """
    query=pg_execute(query)

    query1 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Return Sent to Default Warehouse" = '1',
    "dropped_Return Received at Default Warehouse" = '1',
    "created_dept_Return Sent to Default Warehouse" = 'DESIGNER STORE',
    "created_dept_Return Received at Default Warehouse" = 'DESIGNER STORE' 
    where doc_no = 22101289
    """
    query1=pg_execute(query1)

    query2 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Additional Stock Sent to Control Room" = '1',
    "dropped_Additional Stock Sent to Packaging" = '1',
    "created_dept_Additional Stock Sent to Packaging" = 'CONTROL ROOM'
    where doc_no = 226000471
    """
    query2=pg_execute(query2)

    query3 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Additional Stock Sent to Control Room" = '1',
    "dropped_Additional Stock Sent to Packaging" = '1',
    "created_dept_Additional Stock Sent to Packaging" = 'CONTROL ROOM'
    where doc_no = 223800407
    """
    query3=pg_execute(query3)

    query4 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Additional Stock Sent to Control Room" = '1',
    "dropped_Additional Stock Sent to Packaging" = '1',
    "created_dept_Additional Stock Sent to Packaging" = 'CONTROL ROOM'
    where doc_no = 224600640
    """
    query4=pg_execute(query4)

    query5 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Additional Stock Sent to Control Room" = '1',
    "dropped_Additional Stock Sent to Packaging" = '1',
    "created_dept_Additional Stock Sent to Packaging" = 'CONTROL ROOM'
    where doc_no = 227200068
    """
    query5=pg_execute(query5)

    query6 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Additional Stock Sent to Packaging" = '1',
    "dropped_Additional Stock Sent to Branch" = '1',
    "created_dept_Additional Stock Sent to Packaging" = 'PACKAGING',
    "created_dept_Additional Stock Sent to Branch" = 'PACKAGING'
    where doc_no = 225100364
    """
    query6=pg_execute(query6)

    query7 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Additional Stock Sent to Packaging" = '1',
    "dropped_Additional Stock Sent to Branch" = '1',
    "created_dept_Additional Stock Sent to Packaging" = 'PACKAGING',
    "created_dept_Additional Stock Sent to Branch" = 'PACKAGING'
    where doc_no = 22101307
    """
    query7=pg_execute(query7)

    query8 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Additional Stock Sent to Packaging" = '1',
    "dropped_Additional Stock Sent to Branch" = '1',
    "created_dept_Additional Stock Sent to Packaging" = 'PACKAGING',
    "created_dept_Additional Stock Sent to Branch" = 'PACKAGING'
    where doc_no = 224500891
    """
    query8=pg_execute(query8)

    query9 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Additional Stock Sent to Packaging" = '1',
    "dropped_Additional Stock Sent to Branch" = '1',
    "created_dept_Additional Stock Sent to Packaging" = 'PACKAGING',
    "created_dept_Additional Stock Sent to Branch" = 'PACKAGING'
    where doc_no = 22900826
    """
    query9=pg_execute(query9)

    query10 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Return Sent to HQ" = '1',
    "dropped_Return Sent to Control Room" = 1,
    "created_dept_Return Sent to Control Room" = 'RECEIVING'
    where doc_no = 223600311
    """
    query10=pg_execute(query10)

    query11 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Return Sent to HQ" = '1',
    "dropped_Return Sent to Control Room" = 1,
    "created_dept_Return Sent to Control Room" = 'RECEIVING'
    where doc_no = 225100344
    """
    query11=pg_execute(query11)

    query12 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Return Sent to HQ" = '1',
    "dropped_Return Sent to Control Room" = 1,
    "created_dept_Return Sent to Control Room" = 'RECEIVING'
    where doc_no = 22101198
    """
    query12=pg_execute(query12)

    query13 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Return Sent to HQ" = '1',
    "dropped_Return Sent to Control Room" = 1,
    "created_dept_Return Sent to Control Room" = 'RECEIVING'
    where doc_no = 225100340
    """
    query13=pg_execute(query13)

    query14 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Return Sent to HQ" = '1',
    "dropped_Return Sent to Control Room" = 1,
    "created_dept_Return Sent to Control Room" = 'RECEIVING'
    where doc_no = 22202876
    """
    query14=pg_execute(query14)

    query15 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Borrow Sent to HQ Control Room By Receiver" = '1'
    where doc_no = 224000296
    """
    query15=pg_execute(query15)

    query16 = """
    update mabawa_dw.fact_all_itr_logs
    set "dropped_Warranty Return Sent to Control Room" = '1'
    where doc_no = 225100352
    """
    query16=pg_execute(query16)
    
    return "Created"

def update_fact_all_itr_logs_dept ():

    query = """
    update mabawa_dw.fact_all_itr_logs 
    set "created_dept_Borrow Sent to HQ Control Room" = 'MAIN STORE'
    where doc_no = 22202744
    """
    query=pg_execute(query)

    query1 = """
    update mabawa_dw.fact_all_itr_logs
    set "created_dept_Borrow Sent to HQ Control Room" = 'DESIGNER STORE'
    where doc_no = 22202632
    """
    query1=pg_execute(query1)

    query2 = """
    update mabawa_dw.fact_all_itr_logs
    set "created_dept_Borrow Sent to HQ Control Room" = 'DESIGNER STORE'
    where doc_no = 22202852
    """
    query2=pg_execute(query2)

    query3 = """
    update mabawa_dw.fact_all_itr_logs
    set "created_dept_Return Sent to Default Warehouse" = 'CONTROL ROOM'
    where doc_no = 22202974
    """
    query3=pg_execute(query3)
    return "Created"

"""Creating Branch pipeline"""
def create_all_branch_itr_logs ():

    query = """
    truncate mabawa_mviews.all_branch_itr_logs;
    insert into mabawa_mviews.all_branch_itr_logs
    SELECT 
        itr_no, doc_no, itr_lineno, item_code, post_date, post_time_int, 
        post_time, post_datetime, status, created_user, created_dept, 
        created_branch_code, remarks, dropped
    FROM mabawa_mviews.all_itr_logs_2
    where created_branch_code is not null
    """
    query=pg_execute(query)
    
    return "Created"

def update_branch_itr_dropstatus ():

    query = """
    update mabawa_mviews.all_itr_logs_2 l 
    set dropped = '1' 
    from mabawa_dw.dim_itr_issues its 
    where l.doc_no::text = its.itr_no::text
    and upper(its.issue_dept) = upper(l.created_dept) 
    """
    query = pg_execute(query)

    return "Something"

def transpose_mviews_all_branch_itr_logs ():

    print ("Starting")

    df = pd.read_sql("""
    SELECT 
        itr_no, doc_no, itr_lineno, item_code, post_date, post_time_int, post_time, post_datetime, status, 
        created_user, created_dept, created_branch_code, remarks, dropped
    FROM mabawa_mviews.all_branch_itr_logs
    where post_date >= '2022-07-18'
    """, con=engine)

    print("Fetched ITR Log Data")

    print("Initializing Pivoting")

    df = df.pivot_table(index=['itr_no', 'doc_no', 'itr_lineno', 'item_code'], columns=['status'], values=['post_datetime', 'created_user', 'created_branch_code', 'remarks', 'dropped'], aggfunc='min')
    
    print("Pivoting Complete")

    print("Renaming Columns")
    def rename(col):
        if isinstance(col, tuple):
            col = '_'.join(str(c) for c in col)
        return col

    df.columns = map(rename, df.columns)
    df=df.reset_index()

    # query = """drop table mabawa_mviews.all_itr_logs_trans;"""
    # query = pg_execute(query)

    # print("Inserting Data")
    # df.to_sql('all_branch_itr_logs_trans', con = engine, schema='mabawa_mviews', if_exists = 'append', index=False)

    df = df.set_index(['itr_no','itr_lineno', 'item_code'])

    upsert(engine=engine,
       df=df,
       schema='mabawa_mviews',
       table_name='all_branch_itr_logs_trans',
       if_row_exists='update',
       create_table=False,
       add_new_columns=True)
    
    return "Fact Created"

def create_mviews_fact_category1_logs ():

    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)
    
    print("Fetched Holidays")

    df = pd.read_sql("""
    select 
	    a.*
    from
    (SELECT 
        itc.create_datetime, b.created_branch_code, bc.category, itr.*
    FROM mabawa_mviews.all_branch_itr_logs_trans itr
    join mabawa_mviews.all_itrs_createdate itc on itr.itr_no::text = itc.doc_internal_id::text
    and itr.doc_no::text = itc.doc_no::text
    join mabawa_mviews.itrs_branch_codes b on itr.itr_no = b.itr_no
    join mabawa_mviews.branch_hour_categories bc on b.created_branch_code = bc.whse_code) as a
    """, con=engine)

    print(df)

    print("Fetched All ITR Log Data")

    print("Initializing Date Conversion")
    
    # Create Date
    df["create_datetime"] = pd.to_datetime(df["create_datetime"])
    
    # Additional Stock
    df["post_datetime_Additional Stock Received at Branch"] = pd.to_datetime(df["post_datetime_Additional Stock Received at Branch"])
    df["post_datetime_Additional Stock OK"] = pd.to_datetime(df["post_datetime_Additional Stock OK"])
    
    # Stock Return
    df["post_datetime_Return List Printed"] = pd.to_datetime(df["post_datetime_Return List Printed"])
    df["post_datetime_Return Sent to HQ"] = pd.to_datetime(df["post_datetime_Return Sent to HQ"])
    
    # Stock Exchange
    df["post_datetime_Exchange List Printed"] = pd.to_datetime(df["post_datetime_Exchange List Printed"])
    df["post_datetime_Exchange Sent to HQ"] = pd.to_datetime(df["post_datetime_Exchange Sent to HQ"])
    
    #Warranty Return
    df["post_datetime_Warranty Return List Printed"] = pd.to_datetime(df["post_datetime_Warranty Return List Printed"])
    df["post_datetime_Warranty Return Sent to HQ"] = pd.to_datetime(df["post_datetime_Warranty Return Sent to HQ"])
    
    #STock Borrow
    df["post_datetime_Borrow Sent to Branch"] = pd.to_datetime(df["post_datetime_Borrow Sent to Branch"])
    df["post_datetime_Borrow Received at Branch"] = pd.to_datetime(df["post_datetime_Borrow Received at Branch"])
    df["post_datetime_Borrow OK"] = pd.to_datetime(df["post_datetime_Borrow OK"])
    df["post_datetime_Borrow Identifier Printed"] = pd.to_datetime(df["post_datetime_Borrow Identifier Printed"])
    df["post_datetime_Borrow Sent to HQ"] = pd.to_datetime(df["post_datetime_Borrow Sent to HQ"])
    df["post_datetime_Borrow Sent to Branch at HQ"] = pd.to_datetime(df["post_datetime_Borrow Sent to Branch at HQ"])
    df["post_datetime_Borrow Missing"] = pd.to_datetime(df["post_datetime_Borrow Missing"])
    
    print("Completed Date Conversion")

    print("Setting UP Business Hours Details")

    cat1_workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(10),
        end_time=datetime.time(19),
        working_days=[0,1, 2, 3, 4])

    cat1_saturday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(10),
        end_time=datetime.time(18),
        working_days=[5])

    cat1_sunday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(11),
        end_time=datetime.time(17),
        working_days=[6])
    
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    cal = Kenya()
    #hl = cal.holidays(2021)
    hl = data.values.tolist()
    #print(hl)
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    cat1businesshrs = businesstimedelta.Rules([cat1_workday, cat1_saturday, cat1_sunday])

    def Cat1BusHrs(start, end):
        if end>=start:
            return float(cat1businesshrs.difference(start,end).hours)*float(60)+float(cat1businesshrs.difference(start,end).seconds)/float(60)
        else:
            return ""

    print("Business Hours Successfully Defined")

    print("Initializing Business Hours Calculation")
    #Additional Stock
    df['asrb_asok_diff']=df.apply(lambda row: Cat1BusHrs(row["post_datetime_Additional Stock Received at Branch"], row["post_datetime_Additional Stock OK"]), axis=1)
    
    #Stock Return
    df['rlc_rlp_diff']=df.apply(lambda row: Cat1BusHrs(row["create_datetime"], row["post_datetime_Return List Printed"]), axis=1)
    df['rlp_rshq_diff']=df.apply(lambda row: Cat1BusHrs(row["post_datetime_Return List Printed"], row["post_datetime_Return Sent to HQ"]), axis=1)
    
    # STock Exchnage
    df['elc_elp_diff']=df.apply(lambda row: Cat1BusHrs(row["create_datetime"], row["post_datetime_Exchange List Printed"]), axis=1)
    df['elc_eshq_diff']=df.apply(lambda row: Cat1BusHrs(row["post_datetime_Exchange List Printed"], row["post_datetime_Exchange Sent to HQ"]), axis=1)
    
    # Warranty Return
    df['wrc_wrp_diff']=df.apply(lambda row: Cat1BusHrs(row["create_datetime"], row["post_datetime_Warranty Return List Printed"]), axis=1)
    df['wrp_wrshq_diff']=df.apply(lambda row: Cat1BusHrs(row["post_datetime_Warranty Return List Printed"], row["post_datetime_Warranty Return Sent to HQ"]), axis=1)
    
    # STock Borrow
    df['bsb_brb_diff']=df.apply(lambda row: Cat1BusHrs(row["post_datetime_Borrow Sent to Branch"], row["post_datetime_Borrow Received at Branch"]), axis=1)
    df['brb_bok_diff']=df.apply(lambda row: Cat1BusHrs(row["post_datetime_Borrow Received at Branch"], row["post_datetime_Borrow OK"]), axis=1)
    df['brb_bm_diff']=df.apply(lambda row: Cat1BusHrs(row["post_datetime_Borrow Received at Branch"], row["post_datetime_Borrow Missing"]), axis=1)
    
    print("Completed Business Hours Calculation")

    print("Initializing Conversion of New Columns into Numeric")

    #Additional Stock
    df['asrb_asok_diff'] = pd.to_numeric(df['asrb_asok_diff'])

    #Stock Return
    df['rlc_rlp_diff'] = pd.to_numeric(df['rlc_rlp_diff'])
    df['rlp_rshq_diff'] = pd.to_numeric(df['rlp_rshq_diff'])

    #Stock Exchange
    df['elc_elp_diff'] = pd.to_numeric(df['elc_elp_diff'])
    df['elc_eshq_diff'] = pd.to_numeric(df['elc_eshq_diff'])

    #Warranty Return
    df['wrc_wrp_diff'] = pd.to_numeric(df['wrc_wrp_diff'])
    df['wrp_wrshq_diff'] = pd.to_numeric(df['wrp_wrshq_diff'])

    #Stock Borrow
    df['bsb_brb_diff'] = pd.to_numeric(df['bsb_brb_diff'])
    df['brb_bok_diff'] = pd.to_numeric(df['brb_bok_diff'])
    df['brb_bm_diff'] = pd.to_numeric(df['brb_bm_diff'])
    
    print("Completed Conversion of New Columns into Numeric")

    print("Inserting Data")

    # query = """drop table mabawa_mviews.fact_all_branch_itr_logs"""
    # query = pg_execute(query)

    # df.to_sql('fact_all_branch_itr_logs', con = engine, schema='mabawa_mviews', if_exists = 'append', index=False)

    df = df.set_index(['itr_no','itr_lineno', 'item_code','created_branch_code'])

    upsert(engine=engine,
       df=df,
       schema='mabawa_mviews',
       table_name='fact_all_branch_itr_logs',
       if_row_exists='update',
       create_table=False,
       add_new_columns=True)

    return "Fact Created"