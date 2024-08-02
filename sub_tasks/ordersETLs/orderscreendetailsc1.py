import sys
sys.path.append(".")
import psycopg2
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
import datetime
import businesstimedelta
import pandas as pd
import holidays as pyholidays
from workalendar.africa import Kenya
from sub_tasks.data.connect import (pg_execute, engine) 
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate

# FromDate = '2024/02/05'
# ToDate = '2024/02/05'


def fetch_sap_orderscreendetailsc1 ():
    
    # SessionId = login()
    SessionId = return_session_id(country = "Kenya")
    pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderDetailsC1&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    pagecount_payload={}
    pagecount_headers = {}
    
    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    
    orderscreenc1 = pd.DataFrame()
    payload={}
    headers = {}
    pages = data['result']['body']['recs']['PagesCount']
    for i in range(1, pages+1):
        page = i
        print(page)
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderDetailsC1&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()
        response = response['result']['body']['recs']['Results']
        response = pd.DataFrame.from_dict(response)
        response = response.T
        orderscreenc1 = orderscreenc1.append(response, ignore_index=True)
    

    orderscreenc1.rename (columns = {'DocEntry':'doc_entry', 
                       'LineId':'odsc_lineid', 
                       'Date':'odsc_date', 
                       'Time':'odsc_time', 
                       'Status':'odsc_status', 
                       'DocumentNo':'odsc_doc_no', 
                       'CreatedUser':'odsc_createdby', 
                       'Document':'odsc_document', 
                       'Remarks':'odsc_remarks'}
            ,inplace=True)

    query = """truncate mabawa_staging.landing_orderscreenc1;"""
    query = pg_execute(query)
    orderscreenc1.to_sql('landing_orderscreenc1', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    
    print('landing_orderscreenc1')

# fetch_sap_orderscreendetailsc1()

def update_to_source_orderscreenc1():

    data = pd.read_sql("""
    SELECT distinct
    doc_entry, odsc_lineid, odsc_date, odsc_time, odsc_status, odsc_doc_no, odsc_createdby, odsc_document, odsc_remarks
    FROM mabawa_staging.landing_orderscreenc1;
    """, con=engine)

    data = data.set_index(['doc_entry','odsc_lineid'])

    upsert(engine=engine,
       df=data,
       schema='mabawa_staging',
       table_name='source_orderscreenc1',
       if_row_exists='update',
       create_table=False)
    
    print('source_orderscreenc1')

# update_to_source_orderscreenc1()

def create_source_orderscreenc1_staging():

    query = """
    truncate mabawa_staging.source_orderscreenc1_staging;
    insert into mabawa_staging.source_orderscreenc1_staging
    SELECT distinct doc_entry, odsc_lineid, odsc_date, odsc_time_int, odsc_time, odsc_status, odsc_doc_no, odsc_createdby, odsc_usr_dept, odsc_document, odsc_remarks
    FROM mabawa_staging.v_source_orderscreenc1_staging;
    """

    query = pg_execute(query)

    print('create_source_orderscreenc1_staging')

# create_source_orderscreenc1_staging()  

def create_source_orderscreenc1_staging2():

    query = """
    truncate mabawa_staging.source_orderscreenc1_staging2;
    insert into mabawa_staging.source_orderscreenc1_staging2
    SELECT doc_entry, odsc_lineid, odsc_date, odsc_time_int, 
    odsc_time, odsc_datetime, odsc_status, odsc_new_status, 
    odsc_doc_no, odsc_createdby, odsc_usr_dept, odsc_document, 
    odsc_remarks
    FROM mabawa_staging.v_source_orderscreenc1_staging2;
    """

    query = pg_execute(query)

    print('create_source_orderscreenc1_staging2')

# create_source_orderscreenc1_staging2()

def create_source_orderscreenc1_staging3():

    query = """
    truncate mabawa_staging.source_orderscreenc1_staging3;
    insert into mabawa_staging.source_orderscreenc1_staging3
    SELECT doc_entry, odsc_lineid, odsc_date, odsc_time_int, 
    odsc_time, odsc_datetime, odsc_status, odsc_new_status, 
    odsc_doc_no, odsc_createdby, odsc_usr_dept
    FROM mabawa_staging.v_source_orderscreenc1_staging3;
    """

    query = pg_execute(query)
    
    print('create_source_orderscreenc1_staging3')

# create_source_orderscreenc1_staging3()

def create_source_orderscreenc1_staging4():

    query = """
    truncate mabawa_staging.source_orderscreenc1_staging4;
    insert into mabawa_staging.source_orderscreenc1_staging4
    SELECT doc_entry, odsc_lineid, odsc_date, odsc_time_int, 
    odsc_time, odsc_datetime, odsc_status, odsc_new_status, 
    rownum, odsc_doc_no, odsc_createdby, odsc_usr_dept,
    odsc_remarks, null as is_dropped
    FROM mabawa_staging.v_source_orderscreenc1_staging4;
    """

    query = pg_execute(query)

    print('create_source_orderscreenc1_staging4')

# create_source_orderscreenc1_staging4()

def update_orderscreenc1_staging4():

    query = """
    update mabawa_staging.source_orderscreenc1_staging4 c
    set odsc_new_status = 'Sales Order Created Old'
    from mabawa_staging.source_orderscreen so 
    where c.doc_entry::text = so.doc_entry::text
    and so.ods_ordercriteriastatus = 'PL and HQ Frame'
    and c.odsc_status = 'Sales Order Created'
    """

    query = pg_execute(query)

    query1 = """
    update mabawa_staging.source_orderscreenc1_staging4 c
    set odsc_new_status = 'Sales Order Created Not Used'
    from mabawa_staging.source_orderscreen so 
    where c.doc_entry = so.doc_entry
    and so.ods_ordercriteriastatus = 'PL and HQ Frame'
    and c.odsc_status = 'Confirmed by Approvals Team'
    """

    query1 = pg_execute(query1)

    query2 = """
    update mabawa_staging.source_orderscreenc1_staging4 c
    set odsc_new_status = 'Sales Order Created'
    from mabawa_staging.source_orderscreen so 
    where c.doc_entry = so.doc_entry
    and so.ods_ordercriteriastatus = 'PL and HQ Frame'
    and c.odsc_status = 'PL Sent to Lens Store'
    """

    query2 = pg_execute(query2)

    query3 = """
    update mabawa_staging.source_orderscreenc1_staging4 c 
    set is_dropped = 1
    from mabawa_dw.dim_orders_with_issues o 
    where o.doc_entry::text = c.doc_entry::text 
    and upper(c.odsc_usr_dept) = upper(o.issue_dept)
    """

    query3 = pg_execute(query3)

    query4 = """
    update mabawa_staging.source_orderscreenc1_staging4 c 
    set is_dropped = 1
    from (select doc_entry 
    from mabawa_staging.source_orderscreenc1_staging2
    where odsc_status = 'Lenses coming from Branch Lab') a
    where c.doc_entry:: text = a.doc_entry::text
    and odsc_usr_dept in ('Lens Store')
    """

    query4 = pg_execute(query4)

    query5 = """
    update mabawa_staging.source_orderscreenc1_staging4 c 
    set is_dropped = 1
    from (select doc_entry 
    from mabawa_staging.source_orderscreenc1_staging2
    where odsc_status = 'HQ Item Out of Stock Getting It') as a
    where c.doc_entry = a.doc_entry
    and odsc_usr_dept in ('Main Store', 'Designer')
    """

    query5 = pg_execute(query5)

    query6 = """
    update mabawa_staging.source_orderscreenc1_staging4 c 
    set is_dropped = 1
    from mabawa_dw.dim_time_with_issues t 
    where c.odsc_date::date = t.issue_date::date
    and c.odsc_time::time between t.issue_start_time::time and t.issue_end_time::time 
    and t.issue_dept = 'ALL'
    """

    query6 = pg_execute(query6)

    query7 = """
    update mabawa_staging.source_orderscreenc1_staging4 c 
    set is_dropped = 1
    from mabawa_dw.dim_time_with_issues t 
    where c.odsc_date::date = t.issue_date::date
    and c.odsc_time::time between t.issue_start_time::time and t.issue_end_time::time 
    and upper(c.odsc_usr_dept) = upper(t.issue_dept)
    """

    query7 = pg_execute(query7)

    query8 = """
    update mabawa_staging.source_orderscreenc1_staging4 c 
    set is_dropped = 1
    from (select doc_entry 
    from mabawa_staging.source_orderscreenc1_staging2
    where odsc_status = 'Repair Order Printed') a
    where c.doc_entry = a.doc_entry
    and odsc_usr_dept in ('Lens Store')
    """

    query8 = pg_execute(query8)

    query9 = """
    update mabawa_staging.source_orderscreenc1_staging4 c 
    set is_dropped = 1
    from (select 
        t.doc_entry
    from
    (select doc_entry 
    from mabawa_staging.source_orderscreenc1_staging4
    where odsc_new_status = 'Rejected Order Received At Control Room'
    and odsc_usr_dept = 'Control'
    union all 
    select doc_entry 
    from mabawa_staging.source_orderscreenc1_staging4
    where odsc_new_status = 'Rejected Lenses sent to Lens Store'
    and odsc_usr_dept = 'Control'
    union all 
    select doc_entry 
    from mabawa_staging.source_orderscreenc1_staging4
    where odsc_new_status = 'Rejected Frame sent to Frame Store'
    and odsc_usr_dept = 'Control'
    union all
    SELECT doc_entry 
    FROM mabawa_staging.source_orderscreenc1_staging4
    where odsc_new_status like '%Reject Received at Control Room%'
    and odsc_usr_dept = 'Control'
    union all 
    SELECT doc_entry 
    FROM mabawa_staging.source_orderscreenc1_staging4
    where odsc_new_status = 'Control Room Rejected'
    and odsc_usr_dept = 'Control'
    union all 
    SELECT doc_entry 
	FROM mabawa_staging.source_orderscreenc1_staging4
	where odsc_new_status = 'Blanks Rejected at Control Room'
	and odsc_usr_dept = 'Control') as t) as a
    where c.doc_entry = a.doc_entry
    and odsc_usr_dept in ('Control')
    """

    query9 = pg_execute(query9)

    query10 = """
    update mabawa_staging.source_orderscreenc1_staging4 c 
    set is_dropped = 1
    from 
    (select 
        distinct t.doc_entry
    from
    (select doc_entry 
	from mabawa_staging.source_orderscreenc1_staging4
	where odsc_status = 'Surfacing Damaged By Technician'
	and odsc_usr_dept = 'Surfacing'
    union all 
    select doc_entry 
    from mabawa_staging.source_orderscreenc1_staging4
    where odsc_status = 'Surfacing Damage/Reject Sent to Control Room'
    and odsc_usr_dept = 'Surfacing'
    union all 
    select doc_entry 
    from mabawa_staging.source_orderscreenc1_staging4
    where odsc_status = 'Surfacing Rejected'
    and odsc_usr_dept = 'Surfacing') as t) as a
    where c.doc_entry = a.doc_entry
    and odsc_usr_dept in ('Surfacing')
    """

    query10 = pg_execute(query10)

    query11 = """
    update mabawa_staging.source_orderscreenc1_staging4 c 
    set is_dropped = 1
    from 
    (select distinct
        t.doc_entry
    from
    (select 
        doc_entry 
    FROM mabawa_staging.source_orderscreenc1_staging4
        where odsc_new_status = 'Requested Courier for Home Delivery'
    union all 
    select 
        doc_entry 
    FROM mabawa_staging.source_orderscreenc1_staging4
        where odsc_new_status = 'Sent For Home Delivery'
    union all 	
    select 
        doc_entry 
    FROM mabawa_staging.source_orderscreenc1_staging4
        where odsc_new_status = 'Home Delivery Collected'
    union all 	
    select 
        doc_entry 
    FROM mabawa_staging.source_orderscreenc1_staging4
        where odsc_new_status = 'Damaged PF/PL Sent to Branch') as t) as a
        where c.doc_entry = a.doc_entry
        and odsc_usr_dept in ('Packaging')
    """

    query11 = pg_execute(query11)

    query12 = """
    update mabawa_staging.source_orderscreenc1_staging4 c 
    set is_dropped = 1
    from 
    (select 
    doc_entry 
    FROM mabawa_staging.source_orderscreenc1_staging4
    where odsc_new_status = 'Rejected Order Sent To Control Room'
    and odsc_usr_dept = 'PreQuality') as a
    where c.doc_entry = a.doc_entry
    and odsc_usr_dept in ('PreQuality')
    """

    query12 = pg_execute(query12)

    query13 = """
    update mabawa_staging.source_orderscreenc1_staging4 c 
    set is_dropped = 1
    from 
    (select 
    doc_entry 
    FROM mabawa_staging.source_orderscreenc1_staging4
    where odsc_new_status = 'Rejected Order Sent To Control Room'
    and odsc_usr_dept = 'FinalQC') as a
    where c.doc_entry = a.doc_entry
    and odsc_usr_dept in ('FinalQC')
    """

    query13 = pg_execute(query13)

    query14 = """
    update mabawa_staging.source_orderscreenc1_staging4 c 
    set is_dropped = 1
    from 
    (select 
    doc_entry 
    FROM mabawa_staging.source_orderscreenc1_staging4
    where odsc_new_status = 'Damage Sent to Control Rooom'
    and odsc_usr_dept = 'FinalQC') as a
    where c.doc_entry = a.doc_entry
    and odsc_usr_dept in ('FinalQC')
    """

    query14 = pg_execute(query14)

    print('update_orderscreenc1_staging4')

# update_orderscreenc1_staging4()


def get_collected_orders():

    data = pd.read_sql("""
    SELECT distinct doc_entry 
    FROM mabawa_staging.source_orderscreenc1
    where odsc_status = 'Collected'
    and odsc_date::date < (current_date-interval '5 day')::date
    """, con=engine)

    print("Query Data")

    query = """drop table mabawa_staging.source_collected_orders;"""
    pg_execute(query)

    print("Dropped Table")
    
    data.to_sql('source_collected_orders', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    print("Inserted Data")

    print('get_collected_orders')

# get_collected_orders()


def transpose_orderscreenc1():

    print('lets transpose')

    df = pd.read_sql("""
    SELECT 
        doc_entry, odsc_date, odsc_time_int, odsc_time, 
        odsc_datetime, odsc_status, odsc_new_status, 
        odsc_doc_no, odsc_createdby, odsc_usr_dept, is_dropped
    FROM mabawa_staging.source_orderscreenc1_staging4
    where odsc_date::date >= '2024-05-01'
    """, con=engine)

    print('df created')

    df2 = pd.read_sql("""
    SELECT doc_entry
    FROM mabawa_staging.source_collected_orders
    """, con=engine)
    
    data = pd.merge(df, df2, how='left', indicator=True)

    # data =data[data['_merge'] == 'left_only']
    
    trans_data = data.pivot_table(index='doc_entry', columns=['odsc_new_status'], values=['odsc_datetime','odsc_createdby', 'odsc_usr_dept', 'is_dropped'], aggfunc='min')
    
    # rename columns
    def rename(col):
        if isinstance(col, tuple):
            col = '_'.join(str(c) for c in col)
        return col

    trans_data.columns = map(rename, trans_data.columns)
    print(list(trans_data.columns))
    # trans_data.reset_index(inplace=True)
    trans_data['doc_entry'] = trans_data.index

    drop_table = """drop table mabawa_staging.source_orderscreenc1_staging_trans;"""
    drop_table = pg_execute(drop_table)

    trans_data.to_sql('source_orderscreenc1_staging_trans', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)

    print('source_orderscreenc1_staging_trans created')

# transpose_orderscreenc1()


def index_trans():

    add_key = """alter table mabawa_staging.source_orderscreenc1_staging_trans add PRIMARY KEY (doc_entry)"""
    add_key = pg_execute(add_key)

    add_indexes = """CREATE INDEX ON mabawa_staging.source_orderscreenc1_staging_trans USING btree (doc_entry);
                    CREATE INDEX ON mabawa_staging.source_orderscreenc1_staging_trans USING btree ("odsc_datetime_Order Printed");
                    CREATE INDEX ON mabawa_staging.source_orderscreenc1_staging_trans USING btree ("odsc_usr_dept_Order Printed");"""
    add_indexes = pg_execute(add_indexes)
    
    print('index_trans')

# index_trans()

def create_fact_orderscreenc1_new():

    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)
    
    print("Fetched Holidays")

    df = pd.read_sql("""
    SELECT *
    FROM mabawa_staging.source_orderscreenc1_staging_trans;
    """, con=engine)

    print("Fetched Order Data")

    df["odsc_datetime_Sales Order Created"] = pd.to_datetime(df["odsc_datetime_Sales Order Created"])
    df["odsc_datetime_Order Printed"] = pd.to_datetime(df["odsc_datetime_Order Printed"])
    df["odsc_datetime_Frame Sent to Lens Store"] = pd.to_datetime(df["odsc_datetime_Frame Sent to Lens Store"])
    df["odsc_datetime_Frame Sent to Overseas Desk"] = pd.to_datetime(df["odsc_datetime_Frame Sent to Overseas Desk"])
    df["odsc_datetime_Sent to Control Room"] = pd.to_datetime(df["odsc_datetime_Sent to Control Room"])
    df["odsc_datetime_Blanks Sent to Control Room"] = pd.to_datetime(df["odsc_datetime_Blanks Sent to Control Room"])
    df["odsc_datetime_Issue blanks"] = pd.to_datetime(df["odsc_datetime_Issue blanks"])
    df["odsc_datetime_Issue Finished Lenses for Both Eyes"] = pd.to_datetime(df["odsc_datetime_Issue Finished Lenses for Both Eyes"])
    df["odsc_datetime_PF & PL Sent to Lens Store"] = pd.to_datetime(df["odsc_datetime_PF & PL Sent to Lens Store"])
    df["odsc_datetime_PF & PL Received at Lens Store"] = pd.to_datetime(df["odsc_datetime_PF & PL Received at Lens Store"])
    df["odsc_datetime_Repair Order Printed"] = pd.to_datetime(df["odsc_datetime_Repair Order Printed"])
    df["odsc_datetime_Sent to Surfacing"] = pd.to_datetime(df["odsc_datetime_Sent to Surfacing"])
    df["odsc_datetime_Sent to Pre Quality"] = pd.to_datetime(df["odsc_datetime_Sent to Pre Quality"])
    df["odsc_datetime_Surfacing Assigned to Technician"] = pd.to_datetime(df["odsc_datetime_Surfacing Assigned to Technician"])
    df["odsc_datetime_Assigned to Technician"] = pd.to_datetime(df["odsc_datetime_Assigned to Technician"])
    df["odsc_datetime_Sent to Packaging"] = pd.to_datetime(df["odsc_datetime_Sent to Packaging"])
    df["odsc_datetime_Branch Glazing Sent to Branch"] = pd.to_datetime(df["odsc_datetime_Branch Glazing Sent to Branch"])
    df["odsc_datetime_Sent to Branch"] = pd.to_datetime(df["odsc_datetime_Sent to Branch"])

    print("Converted Dates")

    # All departments working hours 
    workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(19),
        working_days=[0,1, 2, 3, 4])

    saturday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(18),
        working_days=[5])    
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    cal = Kenya()
    hl = data.values.tolist()
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    businesshrs = businesstimedelta.Rules([workday, saturday, holidays])

    def BusHrs(start, end):
        if end>=start:
            return float(businesshrs.difference(start,end).hours)*float(60)+float(businesshrs.difference(start,end).seconds)/float(60)
        else:
            return ""

    # Designer store 
    workday1 = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(18),
        working_days=[0,1, 2, 3, 4])

    saturday1 = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(16),
        working_days=[5])    
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    cal = Kenya()
    hl = data.values.tolist()
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    businesshrs1 = businesstimedelta.Rules([workday1, saturday1, holidays])

    def BusHrs1(start, end):
        if end>=start:
            return float(businesshrs1.difference(start,end).hours)*float(60)+float(businesshrs1.difference(start,end).seconds)/float(60)
        else:
            return ""
    
    filtercolumns = ["odsc_createdby_Order Printed","odsc_createdby_Frame Sent to Lens Store","odsc_createdby_Frame Sent to Overseas Desk",
                     "odsc_createdby_Sent to Control Room"]

    mainstoredf = df[df[filtercolumns].apply(lambda x: x.isin(['main1', 'main2', 'main3']).any(), axis=1)]
    designerdf = df[df[filtercolumns].apply(lambda x: x.isin(['designer1', 'designer2', 'designer3']).any(), axis=1)]

    #designer store    
    designerdf['so_op_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Order Printed"]), axis=1)
    designerdf['op_sl_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    designerdf['op_sc_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Order Printed"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    designerdf['op_ov_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    designerdf['so_sl_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    designerdf['so_sc_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    designerdf['so_ov_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)


    #mainstore
    mainstoredf['so_op_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Order Printed"]), axis=1)
    mainstoredf['op_sl_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    mainstoredf['op_sc_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    mainstoredf['op_ov_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    mainstoredf['so_sl_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    mainstoredf['so_sc_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    mainstoredf['so_ov_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    maindesigner = pd.concat([mainstoredf,designerdf])
    maindesigner = maindesigner[['doc_entry','so_op_diff','op_sl_diff','op_sc_diff','op_ov_diff','so_sl_diff','so_sc_diff','so_ov_diff']]

    print("Completed Main & Designer Store Working Hours")


    #len store
    df['sl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    df['sl_isb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Issue blanks"]), axis=1)
    df['sl_iflb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Issue Finished Lenses for Both Eyes"]), axis=1)
    df['sl_ov_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    df['sent_bfsl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Branch Frame Received from Receiver"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    df['sent_pfsl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF Received from Receiver"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    df['op_isb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Issue blanks"]), axis=1)
    df['op_iflb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Issue Finished Lenses for Both Eyes"]), axis=1)
    
    print("Completed Lens Store Working Hours")
    
    #len store repair orders senttolenstore
    df['sent_sl_rop_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Sent to Lens Store"], row["odsc_datetime_Repair Order Printed"]), axis=1)
    df['rop_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Repair Order Printed"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    df['rop_isb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Repair Order Printed"], row["odsc_datetime_Issue blanks"]), axis=1)
    df['rop_iflb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Repair Order Printed"], row["odsc_datetime_Issue Finished Lenses for Both Eyes"]), axis=1)
    

    #len store repair orders receivedatlenstore
    df['received_sl_rop_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Received at Lens Store"], row["odsc_datetime_Repair Order Printed"]), axis=1)
    df['sent_sl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Sent to Lens Store"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    df['received_sl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Received at Lens Store"], row["odsc_datetime_Sent to Control Room"]), axis=1)

    print("Completed Lens Store Repair Orders")

    #control room
    df['sc__ssf_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Control Room"], row["odsc_datetime_Sent to Surfacing"]), axis=1)
    df['sc_sp_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Control Room"], row["odsc_datetime_Sent to Packaging"]), axis=1)
    df['sc_spqc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Control Room"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)
    df['bsc_ssf_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Blanks Sent to Control Room"], row["odsc_datetime_Sent to Surfacing"]), axis=1)
    df['bsc_sp_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Blanks Sent to Control Room"], row["odsc_datetime_Sent to Packaging"]), axis=1)
    df['bsc_spqc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Blanks Sent to Control Room"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)
    
    print("Completed Control Room Working Hours")

    #surfacing
    df['ssf__astch_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Surfacing"], row["odsc_datetime_Surfacing Assigned to Technician"]), axis=1)
    df['astch_spqc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Surfacing Assigned to Technician"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)
    df['ssf_spqc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Surfacing"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)

    print("Completed Surfacing Working Hours")

    #prequality
    df['spqc__astch_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Pre Quality"], row["odsc_datetime_Assigned to Technician"]), axis=1)
    df['spqc__stp_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Pre Quality"], row["odsc_datetime_Sent to Packaging"]), axis=1)

    print("Completed PreQC Working Hours")

    #finalqc tech
    df['astch_stp_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Assigned to Technician"], row["odsc_datetime_Sent to Packaging"]), axis=1)

    print("Completed FinalQC Working Hours")

    #packaging
    df['sp_stb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Packaging"], row["odsc_datetime_Sent to Branch"]), axis=1)
    df['sp_gbstb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Packaging"], row["odsc_datetime_Branch Glazing Sent to Branch"]), axis=1)

    print("Calculated All Working Hours")

    df = pd.merge(df,maindesigner,on ='doc_entry',how = 'outer')

    print("Data Merged")  
    print(df['doc_entry'].duplicated().sum())  
    df = df.drop_duplicates(subset=['doc_entry'],keep = 'first')
    print('Existing Duplicates Dropped ')


    #main and designer stored
    df['so_op_diff'] = pd.to_numeric(df['so_op_diff'])
    df['op_sl_diff'] = pd.to_numeric(df['op_sl_diff'])
    df['op_sc_diff'] = pd.to_numeric(df['op_sc_diff'])
    df['op_ov_diff'] = pd.to_numeric(df['op_ov_diff'])
    df['so_sl_diff'] = pd.to_numeric(df['so_sl_diff'])
    df['so_sc_diff'] = pd.to_numeric(df['so_sc_diff'])
    df['so_ov_diff'] = pd.to_numeric(df['so_ov_diff'])

   #lensstore
    df['sl_sc_diff'] = pd.to_numeric(df['sl_sc_diff'])
    df['sl_isb_diff'] = pd.to_numeric(df['sl_isb_diff'])
    df['sl_iflb_diff'] = pd.to_numeric(df['sl_iflb_diff'])
    df['sl_ov_diff'] = pd.to_numeric(df['sl_ov_diff'])
    df['op_isb_diff'] = pd.to_numeric(df['op_isb_diff'])
    df['op_iflb_diff'] = pd.to_numeric(df['op_iflb_diff'])
    df['sent_bfsl_sc_diff']=pd.to_numeric(df['sent_bfsl_sc_diff'])
    df['sent_pfsl_sc_diff']=pd.to_numeric(df['sent_pfsl_sc_diff'])

    #lensstorerepairs
    df['sent_sl_rop_diff'] = pd.to_numeric(df['sent_sl_rop_diff'])
    df['rop_sc_diff'] = pd.to_numeric(df['rop_sc_diff'])
    df['rop_isb_diff'] = pd.to_numeric(df['rop_isb_diff'])
    df['rop_iflb_diff'] = pd.to_numeric(df['rop_iflb_diff'])
    df['received_sl_rop_diff'] = pd.to_numeric(df['received_sl_rop_diff'])
    df['sent_sl_sc_diff'] = pd.to_numeric(df['sent_sl_sc_diff'])
    df['received_sl_sc_diff'] = pd.to_numeric(df['received_sl_sc_diff'])

    #controlroom
    df['sc__ssf_diff'] = pd.to_numeric(df['sc__ssf_diff'])
    df['sc_sp_diff'] = pd.to_numeric(df['sc_sp_diff'])
    df['sc_spqc_diff'] = pd.to_numeric(df['sc_spqc_diff'])
    df['bsc_ssf_diff'] = pd.to_numeric(df['bsc_ssf_diff'])
    df['bsc_sp_diff'] = pd.to_numeric(df['bsc_sp_diff'])
    df['bsc_spqc_diff'] = pd.to_numeric(df['bsc_spqc_diff'])

    #surfacing
    df['ssf__astch_diff'] = pd.to_numeric(df['ssf__astch_diff'])
    df['astch_spqc_diff'] = pd.to_numeric(df['astch_spqc_diff'])
    df['ssf_spqc_diff'] = pd.to_numeric(df['ssf_spqc_diff'])

    #prequality
    df['spqc__astch_diff'] = pd.to_numeric(df['spqc__astch_diff'])
    df['spqc__stp_diff'] = pd.to_numeric(df['spqc__stp_diff'])

    #finalqc
    df['astch_stp_diff'] = pd.to_numeric(df['astch_stp_diff'])

    #packaging
    df['sp_stb_diff'] = pd.to_numeric(df['sp_stb_diff'])
    df['sp_gbstb_diff'] = pd.to_numeric(df['sp_gbstb_diff'])

    print("Converted Numerics")

    #print("Dropped Table")
    df = df.set_index('doc_entry')
    print("Data Indexed")

    #query = """drop table mabawa_dw.fact_orderscreenc1_dev"""
    #query = pg_execute(query)

    #df.to_sql('fact_orderscreenc1_dev', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    
    upsert(engine=engine,
       df=df,
       schema='mabawa_dw',
       table_name='fact_orderscreenc1_test',
       if_row_exists='update',
       create_table=False,
       add_new_columns=True)
    
    print("Data Updated")
    
    print("create_fact_orderscreenc1_new")

# create_fact_orderscreenc1_new()


def reindex_db():

    query = """
    REINDEX DATABASE mabawa;
    """
    query = pg_execute(query)

    print('reindex_db')

# reindex_db()

def get_techs():

    preqc = pd.read_sql("""
    SELECT
    doc_entry, odsc_remarks,
    split_part(odsc_remarks, ',', 1) as first_tech,
    split_part(odsc_remarks, ',', 2) as second_tech,
    split_part(odsc_remarks, ',', 3) as third_tech
    FROM mabawa_staging.source_orderscreenc1
    where odsc_createdby like 'prequality%%'
    and odsc_status = 'Assigned to Technician'
    """, con=engine)

    print("Query Complete")

    query = """truncate mabawa_dw.preqc_techs;"""
    query = pg_execute(query)
    print("Truncated PreQC Table")
    
    preqc.to_sql('preqc_techs', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    print("PreQC Table Created")


    finalqc = pd.read_sql("""
    SELECT
    doc_entry, odsc_remarks,
    split_part(odsc_remarks, ',', 1) as first_tech,
    split_part(odsc_remarks, ',', 2) as second_tech,
    split_part(odsc_remarks, ',', 3) as third_tech
    FROM mabawa_staging.source_orderscreenc1
    where odsc_createdby like 'final%%'
    """, con=engine)

    print("Query Complete")

    truncate = """truncate mabawa_dw.finalqc_techs;"""
    truncate = pg_execute(truncate)
    print("Truncated FinalQC Table")
    
    finalqc.to_sql('finalqc_techs', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    print("FinalQC Techs Table Created")

    print('get_techs')

# get_techs()

def create_source_orderscreenc1_staging5():

    df = pd.read_sql("""
    SELECT 
        doc_entry, odsc_date, odsc_time_int, odsc_time, 
        odsc_datetime, odsc_status, odsc_new_status, 
        odsc_doc_no, odsc_createdby, odsc_usr_dept, is_dropped
    FROM mabawa_staging.source_orderscreenc1_staging4
    where odsc_date::date >= '2021-11-01'
    """, con=engine)

    df = df.pivot_table(index='doc_entry', columns=['odsc_new_status'], values=['odsc_datetime','odsc_createdby', 'odsc_usr_dept', 'is_dropped'], aggfunc='min')
    print("Transposed")

    # rename columns
    def rename(col):
        if isinstance(col, tuple):
            col = '_'.join(str(c) for c in col)
        return col
    
    df.columns = map(rename, df.columns)
    df['doc_entry'] = df.index
    print("Indexed")

    drop_table = """drop table mabawa_staging.source_orderscreenc1_staging5;"""
    drop_table = pg_execute(drop_table)
    print("Dropped")

    df.to_sql('source_orderscreenc1_staging5', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    
    print('create_source_orderscreenc1_staging5')

# create_source_orderscreenc1_staging5() 

def index_staging5():

    add_key = """alter table mabawa_staging.source_orderscreenc1_staging5 add PRIMARY KEY (doc_entry)"""
    add_key = pg_execute(add_key)

    add_indexes = """CREATE INDEX ON mabawa_staging.source_orderscreenc1_staging5 USING btree (doc_entry);
                    CREATE INDEX ON mabawa_staging.source_orderscreenc1_staging5 USING btree ("odsc_datetime_Order Printed");
                    CREATE INDEX ON mabawa_staging.source_orderscreenc1_staging5 USING btree ("odsc_usr_dept_Order Printed");"""
    add_indexes = pg_execute(add_indexes)

    print('index_staging5')

# index_staging5()

def create_fact_orderscreenc1_staging():

    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)
    
    df = pd.read_sql("""
    select *
    FROM mabawa_staging.source_orderscreenc1_staging_trans;
    """, con=engine)

    df["odsc_datetime_Sales Order Created"] = pd.to_datetime(df["odsc_datetime_Sales Order Created"])
    df["odsc_datetime_Order Printed"] = pd.to_datetime(df["odsc_datetime_Order Printed"])
    df["odsc_datetime_Frame Sent to Lens Store"] = pd.to_datetime(df["odsc_datetime_Frame Sent to Lens Store"])
    df["odsc_datetime_Frame Sent to Overseas Desk"] = pd.to_datetime(df["odsc_datetime_Frame Sent to Overseas Desk"])
    df["odsc_datetime_Sent to Control Room"] = pd.to_datetime(df["odsc_datetime_Sent to Control Room"])
    df["odsc_datetime_Blanks Sent to Control Room"] = pd.to_datetime(df["odsc_datetime_Blanks Sent to Control Room"])
    df["odsc_datetime_Issue blanks"] = pd.to_datetime(df["odsc_datetime_Issue blanks"])
    df["odsc_datetime_Issue Finished Lenses for Both Eyes"] = pd.to_datetime(df["odsc_datetime_Issue Finished Lenses for Both Eyes"])
    df["odsc_datetime_PF & PL Sent to Lens Store"] = pd.to_datetime(df["odsc_datetime_PF & PL Sent to Lens Store"])
    df["odsc_datetime_PF & PL Received at Lens Store"] = pd.to_datetime(df["odsc_datetime_PF & PL Received at Lens Store"])
    df["odsc_datetime_Repair Order Printed"] = pd.to_datetime(df["odsc_datetime_Repair Order Printed"])
    df["odsc_datetime_Sent to Surfacing"] = pd.to_datetime(df["odsc_datetime_Sent to Surfacing"])
    df["odsc_datetime_Sent to Pre Quality"] = pd.to_datetime(df["odsc_datetime_Sent to Pre Quality"])
    df["odsc_datetime_Surfacing Assigned to Technician"] = pd.to_datetime(df["odsc_datetime_Surfacing Assigned to Technician"])
    df["odsc_datetime_Assigned to Technician"] = pd.to_datetime(df["odsc_datetime_Assigned to Technician"])
    df["odsc_datetime_Sent to Packaging"] = pd.to_datetime(df["odsc_datetime_Sent to Packaging"])
    df["odsc_datetime_Branch Glazing Sent to Branch"] = pd.to_datetime(df["odsc_datetime_Branch Glazing Sent to Branch"])
    df["odsc_datetime_Sent to Branch"] = pd.to_datetime(df["odsc_datetime_Sent to Branch"])


    # Designer store 
    workday1 = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(18),
        working_days=[0,1, 2, 3, 4])

    saturday1 = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(16),
        working_days=[5]) 
       
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    cal = Kenya()
    hl = data.values.tolist()
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    businesshrs1 = businesstimedelta.Rules([workday1, saturday1, holidays])

    def BusHrs1(start, end):
        if end>=start:
            return float(businesshrs1.difference(start,end).hours)*float(60)+float(businesshrs1.difference(start,end).seconds)/float(60)
        else:
            return ""
        
    # All departments working hours 
    workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(19),
        working_days=[0,1, 2, 3, 4])

    saturday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(18),
        working_days=[5])    
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    cal = Kenya()
    hl = data.values.tolist()
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    businesshrs = businesstimedelta.Rules([workday, saturday, holidays])

    def BusHrs(start, end):
        if end>=start:
            return float(businesshrs.difference(start,end).hours)*float(60)+float(businesshrs.difference(start,end).seconds)/float(60)
        else:
            return ""

    
    filtercolumns = ["odsc_createdby_Order Printed","odsc_createdby_Frame Sent to Lens Store","odsc_createdby_Frame Sent to Overseas Desk",
                     "odsc_createdby_Sent to Control Room"]

    mainstoredf = df[df[filtercolumns].apply(lambda x: x.isin(['main1', 'main2', 'main3']).any(), axis=1)]
    designerdf = df[df[filtercolumns].apply(lambda x: x.isin(['designer1', 'designer2', 'designer3']).any(), axis=1)]


    #designer store    
    designerdf['so_op_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Order Printed"]), axis=1)
    designerdf['op_sl_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    designerdf['op_sc_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Order Printed"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    designerdf['op_ov_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    designerdf['so_sl_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    designerdf['so_sc_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    designerdf['so_ov_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)


    #mainstore
    mainstoredf['so_op_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Order Printed"]), axis=1)
    mainstoredf['op_sl_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    mainstoredf['op_sc_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    mainstoredf['op_ov_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    mainstoredf['so_sl_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    mainstoredf['so_sc_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    mainstoredf['so_ov_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    maindesigner = pd.concat([mainstoredf,designerdf])
    maindesigner = maindesigner[['doc_entry','so_op_diff','op_sl_diff','op_sc_diff','op_ov_diff','so_sl_diff','so_sc_diff','so_ov_diff']]
    
    #len store
    df['sl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    df['sl_isb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Issue blanks"]), axis=1)
    df['sl_iflb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Issue Finished Lenses for Both Eyes"]), axis=1)
    df['sl_ov_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    df['sent_bfsl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Branch Frame Received from Receiver"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    df['sent_pfsl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF Received from Receiver"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    df['op_isb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Issue blanks"]), axis=1)
    df['op_iflb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Issue Finished Lenses for Both Eyes"]), axis=1)
    
    
    #len store repair orders senttolenstore
    df['sent_sl_rop_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Sent to Lens Store"], row["odsc_datetime_Repair Order Printed"]), axis=1)
    df['rop_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Repair Order Printed"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    df['rop_isb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Repair Order Printed"], row["odsc_datetime_Issue blanks"]), axis=1)
    df['rop_iflb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Repair Order Printed"], row["odsc_datetime_Issue Finished Lenses for Both Eyes"]), axis=1)
    
    #len store repair orders receivedatlenstore
    df['received_sl_rop_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Received at Lens Store"], row["odsc_datetime_Repair Order Printed"]), axis=1)
    df['sent_sl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Sent to Lens Store"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    df['received_sl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Received at Lens Store"], row["odsc_datetime_Sent to Control Room"]), axis=1)


    #control room
    df['sc__ssf_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Control Room"], row["odsc_datetime_Sent to Surfacing"]), axis=1)
    df['sc_sp_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Control Room"], row["odsc_datetime_Sent to Packaging"]), axis=1)
    df['sc_spqc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Control Room"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)
    df['bsc_ssf_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Blanks Sent to Control Room"], row["odsc_datetime_Sent to Surfacing"]), axis=1)
    df['bsc_sp_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Blanks Sent to Control Room"], row["odsc_datetime_Sent to Packaging"]), axis=1)
    df['bsc_spqc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Blanks Sent to Control Room"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)
    
    #surfacing
    df['ssf__astch_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Surfacing"], row["odsc_datetime_Surfacing Assigned to Technician"]), axis=1)
    df['astch_spqc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Surfacing Assigned to Technician"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)
    df['ssf_spqc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Surfacing"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)

    #prequality
    df['spqc__astch_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Pre Quality"], row["odsc_datetime_Assigned to Technician"]), axis=1)
    df['spqc__stp_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Pre Quality"], row["odsc_datetime_Sent to Packaging"]), axis=1)

    #finalqc tech
    df['astch_stp_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Assigned to Technician"], row["odsc_datetime_Sent to Packaging"]), axis=1)

    #packaging
    df['sp_stb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Packaging"], row["odsc_datetime_Sent to Branch"]), axis=1)
    df['sp_gbstb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Packaging"], row["odsc_datetime_Branch Glazing Sent to Branch"]), axis=1)

    df = pd.merge(df,maindesigner,on ='doc_entry',how = 'outer')

    print("Data Merged")  
    print(df['doc_entry'].duplicated().sum())  
    df = df.drop_duplicates(subset=['doc_entry'],keep = 'first')
    print('Existing Duplicates Dropped ')

    #main and designer stored
    df['so_op_diff'] = pd.to_numeric(df['so_op_diff'])
    df['op_sl_diff'] = pd.to_numeric(df['op_sl_diff'])
    df['op_sc_diff'] = pd.to_numeric(df['op_sc_diff'])
    df['op_ov_diff'] = pd.to_numeric(df['op_ov_diff'])
    df['so_sl_diff'] = pd.to_numeric(df['so_sl_diff'])
    df['so_sc_diff'] = pd.to_numeric(df['so_sc_diff'])
    df['so_ov_diff'] = pd.to_numeric(df['so_ov_diff'])

    #lensstore
    df['sl_sc_diff'] = pd.to_numeric(df['sl_sc_diff'])
    df['sl_isb_diff'] = pd.to_numeric(df['sl_isb_diff'])
    df['sl_iflb_diff'] = pd.to_numeric(df['sl_iflb_diff'])
    df['sl_ov_diff'] = pd.to_numeric(df['sl_ov_diff'])
    df['op_isb_diff'] = pd.to_numeric(df['op_isb_diff'])
    df['op_iflb_diff'] = pd.to_numeric(df['op_iflb_diff'])
    df['sent_bfsl_sc_diff']=pd.to_numeric(df['sent_bfsl_sc_diff'])
    df['sent_pfsl_sc_diff']=pd.to_numeric(df['sent_pfsl_sc_diff'])

    #lensstorerepairs
    df['sent_sl_rop_diff'] = pd.to_numeric(df['sent_sl_rop_diff'])
    df['rop_sc_diff'] = pd.to_numeric(df['rop_sc_diff'])
    df['rop_isb_diff'] = pd.to_numeric(df['rop_isb_diff'])
    df['rop_iflb_diff'] = pd.to_numeric(df['rop_iflb_diff'])
    df['received_sl_rop_diff'] = pd.to_numeric(df['received_sl_rop_diff'])
    df['sent_sl_sc_diff'] = pd.to_numeric(df['sent_sl_sc_diff'])
    df['received_sl_sc_diff'] = pd.to_numeric(df['received_sl_sc_diff'])

    #controlroom
    df['sc__ssf_diff'] = pd.to_numeric(df['sc__ssf_diff'])
    df['sc_sp_diff'] = pd.to_numeric(df['sc_sp_diff'])
    df['sc_spqc_diff'] = pd.to_numeric(df['sc_spqc_diff'])
    df['bsc_ssf_diff'] = pd.to_numeric(df['bsc_ssf_diff'])
    df['bsc_sp_diff'] = pd.to_numeric(df['bsc_sp_diff'])
    df['bsc_spqc_diff'] = pd.to_numeric(df['bsc_spqc_diff'])

    #surfacing
    df['ssf__astch_diff'] = pd.to_numeric(df['ssf__astch_diff'])
    df['astch_spqc_diff'] = pd.to_numeric(df['astch_spqc_diff'])
    df['ssf_spqc_diff'] = pd.to_numeric(df['ssf_spqc_diff'])

    #prequality
    df['spqc__astch_diff'] = pd.to_numeric(df['spqc__astch_diff'])
    df['spqc__stp_diff'] = pd.to_numeric(df['spqc__stp_diff'])

    #finalqc
    df['astch_stp_diff'] = pd.to_numeric(df['astch_stp_diff'])

    #packaging
    df['sp_stb_diff'] = pd.to_numeric(df['sp_stb_diff'])
    df['sp_gbstb_diff'] = pd.to_numeric(df['sp_gbstb_diff'])

    #query = """drop table mabawa_dw.fact_orderscreenc1_test;"""
    #query = pg_execute(query)
    
    df.to_sql('fact_orderscreenc1_staging', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    
    print('create_fact_orderscreenc1_staging')

# create_fact_orderscreenc1_staging()

def create_fact_orderscreenc1():

    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)
    
    df = pd.read_sql("""
    SELECT *
    FROM mabawa_staging.source_orderscreenc1_staging5;
    """, con=engine)

    print("Data Picked")

    df["odsc_datetime_Sales Order Created"] = pd.to_datetime(df["odsc_datetime_Sales Order Created"])
    df["odsc_datetime_Order Printed"] = pd.to_datetime(df["odsc_datetime_Order Printed"])
    df["odsc_datetime_Frame Sent to Lens Store"] = pd.to_datetime(df["odsc_datetime_Frame Sent to Lens Store"])
    df["odsc_datetime_Frame Sent to Overseas Desk"] = pd.to_datetime(df["odsc_datetime_Frame Sent to Overseas Desk"])
    df["odsc_datetime_Sent to Control Room"] = pd.to_datetime(df["odsc_datetime_Sent to Control Room"])
    df["odsc_datetime_Blanks Sent to Control Room"] = pd.to_datetime(df["odsc_datetime_Blanks Sent to Control Room"])
    df["odsc_datetime_Issue blanks"] = pd.to_datetime(df["odsc_datetime_Issue blanks"])
    df["odsc_datetime_Issue Finished Lenses for Both Eyes"] = pd.to_datetime(df["odsc_datetime_Issue Finished Lenses for Both Eyes"])
    df["odsc_datetime_PF & PL Sent to Lens Store"] = pd.to_datetime(df["odsc_datetime_PF & PL Sent to Lens Store"])
    df["odsc_datetime_PF & PL Received at Lens Store"] = pd.to_datetime(df["odsc_datetime_PF & PL Received at Lens Store"])
    df["odsc_datetime_Repair Order Printed"] = pd.to_datetime(df["odsc_datetime_Repair Order Printed"])
    df["odsc_datetime_Sent to Surfacing"] = pd.to_datetime(df["odsc_datetime_Sent to Surfacing"])
    df["odsc_datetime_Sent to Pre Quality"] = pd.to_datetime(df["odsc_datetime_Sent to Pre Quality"])
    df["odsc_datetime_Surfacing Assigned to Technician"] = pd.to_datetime(df["odsc_datetime_Surfacing Assigned to Technician"])
    df["odsc_datetime_Assigned to Technician"] = pd.to_datetime(df["odsc_datetime_Assigned to Technician"])
    df["odsc_datetime_Sent to Packaging"] = pd.to_datetime(df["odsc_datetime_Sent to Packaging"])
    df["odsc_datetime_Branch Glazing Sent to Branch"] = pd.to_datetime(df["odsc_datetime_Branch Glazing Sent to Branch"])
    df["odsc_datetime_Sent to Branch"] = pd.to_datetime(df["odsc_datetime_Sent to Branch"])

    print("Converted Dates")

    # Designer store 
    workday1 = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(18),
        working_days=[0,1, 2, 3, 4])

    saturday1 = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(16),
        working_days=[5])    
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    cal = Kenya()
    hl = data.values.tolist()
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    businesshrs1 = businesstimedelta.Rules([workday1, saturday1, holidays])

    def BusHrs1(start, end):
        if end>=start:
            return float(businesshrs1.difference(start,end).hours)*float(60)+float(businesshrs1.difference(start,end).seconds)/float(60)
        else:
            return ""
        
    # All departments working hours 
    workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(19),
        working_days=[0,1, 2, 3, 4])

    saturday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(18),
        working_days=[5])    
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    cal = Kenya()
    hl = data.values.tolist()
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    businesshrs = businesstimedelta.Rules([workday, saturday, holidays])

    def BusHrs(start, end):
        if end>=start:
            return float(businesshrs.difference(start,end).hours)*float(60)+float(businesshrs.difference(start,end).seconds)/float(60)
        else:
            return ""

    
    filtercolumns = ["odsc_createdby_Order Printed","odsc_createdby_Frame Sent to Lens Store","odsc_createdby_Frame Sent to Overseas Desk",
                     "odsc_createdby_Sent to Control Room"]

    mainstoredf = df[df[filtercolumns].apply(lambda x: x.isin(['main1', 'main2', 'main3']).any(), axis=1)]
    designerdf = df[df[filtercolumns].apply(lambda x: x.isin(['designer1', 'designer2', 'designer3']).any(), axis=1)]

    #designer store    
    designerdf['so_op_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Order Printed"]), axis=1)
    designerdf['op_sl_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    designerdf['op_sc_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Order Printed"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    designerdf['op_ov_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    designerdf['so_sl_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    designerdf['so_sc_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    designerdf['so_ov_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)


    #mainstore
    mainstoredf['so_op_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Order Printed"]), axis=1)
    mainstoredf['op_sl_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    mainstoredf['op_sc_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    mainstoredf['op_ov_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    mainstoredf['so_sl_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    mainstoredf['so_sc_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    mainstoredf['so_ov_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    maindesigner = pd.concat([mainstoredf,designerdf])
    maindesigner = maindesigner[['doc_entry','so_op_diff','op_sl_diff','op_sc_diff','op_ov_diff','so_sl_diff','so_sc_diff','so_ov_diff']]

    #len store
    df['sl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    df['sl_isb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Issue blanks"]), axis=1)
    df['sl_iflb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Issue Finished Lenses for Both Eyes"]), axis=1)
    df['sl_ov_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    df['sent_bfsl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    # df['sent_bfsl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Branch Frame Received from Receiver"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    # df['sent_pfsl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF Received from Receiver"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    df['op_isb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Issue blanks"]), axis=1)
    df['op_iflb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Issue Finished Lenses for Both Eyes"]), axis=1)
    
    
    #len store repair orders senttolenstore
    df['sent_sl_rop_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Sent to Lens Store"], row["odsc_datetime_Repair Order Printed"]), axis=1)
    df['rop_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Repair Order Printed"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    df['rop_isb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Repair Order Printed"], row["odsc_datetime_Issue blanks"]), axis=1)
    df['rop_iflb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Repair Order Printed"], row["odsc_datetime_Issue Finished Lenses for Both Eyes"]), axis=1)
    
    #len store repair orders receivedatlenstore
    df['received_sl_rop_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Received at Lens Store"], row["odsc_datetime_Repair Order Printed"]), axis=1)
    df['sent_sl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Sent to Lens Store"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    df['received_sl_sc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Received at Lens Store"], row["odsc_datetime_Sent to Control Room"]), axis=1)


    #control room
    df['sc__ssf_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Control Room"], row["odsc_datetime_Sent to Surfacing"]), axis=1)
    df['sc_sp_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Control Room"], row["odsc_datetime_Sent to Packaging"]), axis=1)
    df['sc_spqc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Control Room"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)
    df['bsc_ssf_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Blanks Sent to Control Room"], row["odsc_datetime_Sent to Surfacing"]), axis=1)
    df['bsc_sp_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Blanks Sent to Control Room"], row["odsc_datetime_Sent to Packaging"]), axis=1)
    df['bsc_spqc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Blanks Sent to Control Room"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)
    
    #surfacing
    df['ssf__astch_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Surfacing"], row["odsc_datetime_Surfacing Assigned to Technician"]), axis=1)
    df['astch_spqc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Surfacing Assigned to Technician"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)
    df['ssf_spqc_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Surfacing"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)

    #prequality
    df['spqc__astch_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Pre Quality"], row["odsc_datetime_Assigned to Technician"]), axis=1)
    df['spqc__stp_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Pre Quality"], row["odsc_datetime_Sent to Packaging"]), axis=1)

    #finalqc tech
    df['astch_stp_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Assigned to Technician"], row["odsc_datetime_Sent to Packaging"]), axis=1)

    #packaging
    df['sp_stb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Packaging"], row["odsc_datetime_Sent to Branch"]), axis=1)
    df['sp_gbstb_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Packaging"], row["odsc_datetime_Branch Glazing Sent to Branch"]), axis=1)

    df = pd.merge(df,maindesigner,on ='doc_entry',how = 'outer')
    print("Data Merged")  
    print(df['doc_entry'].duplicated().sum())  
    df = df.drop_duplicates(subset=['doc_entry'],keep = 'first')
    print('Existing Duplicates Dropped ')

    print("Calculated Working Minutes")

    #main and designer stored
    df['so_op_diff'] = pd.to_numeric(df['so_op_diff'])
    df['op_sl_diff'] = pd.to_numeric(df['op_sl_diff'])
    df['op_sc_diff'] = pd.to_numeric(df['op_sc_diff'])
    df['op_ov_diff'] = pd.to_numeric(df['op_ov_diff'])
    df['so_sl_diff'] = pd.to_numeric(df['so_sl_diff'])
    df['so_sc_diff'] = pd.to_numeric(df['so_sc_diff'])
    df['so_ov_diff'] = pd.to_numeric(df['so_ov_diff'])

    #lensstore
    df['sl_sc_diff'] = pd.to_numeric(df['sl_sc_diff'])
    df['sl_isb_diff'] = pd.to_numeric(df['sl_isb_diff'])
    df['sl_iflb_diff'] = pd.to_numeric(df['sl_iflb_diff'])
    df['sl_ov_diff'] = pd.to_numeric(df['sl_ov_diff'])
    df['op_isb_diff'] = pd.to_numeric(df['op_isb_diff'])
    df['op_iflb_diff'] = pd.to_numeric(df['op_iflb_diff'])
    df['sent_bfsl_sc_diff']=pd.to_numeric(df['sent_bfsl_sc_diff'])
    df['sent_pfsl_sc_diff']=pd.to_numeric(df['sent_pfsl_sc_diff'])

    #lensstorerepairs
    df['sent_sl_rop_diff'] = pd.to_numeric(df['sent_sl_rop_diff'])
    df['rop_sc_diff'] = pd.to_numeric(df['rop_sc_diff'])
    df['rop_isb_diff'] = pd.to_numeric(df['rop_isb_diff'])
    df['rop_iflb_diff'] = pd.to_numeric(df['rop_iflb_diff'])
    df['received_sl_rop_diff'] = pd.to_numeric(df['received_sl_rop_diff'])
    df['sent_sl_sc_diff'] = pd.to_numeric(df['sent_sl_sc_diff'])
    df['received_sl_sc_diff'] = pd.to_numeric(df['received_sl_sc_diff'])

    #controlroom
    df['sc__ssf_diff'] = pd.to_numeric(df['sc__ssf_diff'])
    df['sc_sp_diff'] = pd.to_numeric(df['sc_sp_diff'])
    df['sc_spqc_diff'] = pd.to_numeric(df['sc_spqc_diff'])
    df['bsc_ssf_diff'] = pd.to_numeric(df['bsc_ssf_diff'])
    df['bsc_sp_diff'] = pd.to_numeric(df['bsc_sp_diff'])
    df['bsc_spqc_diff'] = pd.to_numeric(df['bsc_spqc_diff'])

    #surfacing
    df['ssf__astch_diff'] = pd.to_numeric(df['ssf__astch_diff'])
    df['astch_spqc_diff'] = pd.to_numeric(df['astch_spqc_diff'])
    df['ssf_spqc_diff'] = pd.to_numeric(df['ssf_spqc_diff'])

    #prequality
    df['spqc__astch_diff'] = pd.to_numeric(df['spqc__astch_diff'])
    df['spqc__stp_diff'] = pd.to_numeric(df['spqc__stp_diff'])

    #finalqc
    df['astch_stp_diff'] = pd.to_numeric(df['astch_stp_diff'])

    #packaging
    df['sp_stb_diff'] = pd.to_numeric(df['sp_stb_diff'])
    df['sp_gbstb_diff'] = pd.to_numeric(df['sp_gbstb_diff'])

    print("Converted Numerics")

    query = """drop table mabawa_dw.fact_orderscreenc1_test;"""
    query = pg_execute(query)

    print("Dropped Table")
    
    df.to_sql('fact_orderscreenc1_test', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    
    print('create_fact_orderscreenc1')

# create_fact_orderscreenc1()

def index_fact_c1():

    add_key = """alter table mabawa_dw.fact_orderscreenc1_test add PRIMARY KEY (doc_entry)"""
    add_key = pg_execute(add_key)

    add_indexes = """
    CREATE INDEX fact_orderscreenc1_test_doc_entry_idx ON mabawa_dw.fact_orderscreenc1_test USING btree (doc_entry);
    CREATE INDEX "fact_orderscreenc1_test_is_dropped_Issue Finished Lenses fo_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("is_dropped_Issue Finished Lenses for Both Eyes");
    CREATE INDEX "fact_orderscreenc1_test_is_dropped_Order Printed_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("is_dropped_Order Printed");
    CREATE INDEX "fact_orderscreenc1_test_is_dropped_Sales Order Created_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("is_dropped_Sales Order Created");
    CREATE INDEX "fact_orderscreenc1_test_odsc_datetime_Branch Frame Sent to _idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_datetime_Branch Frame Sent to Lens Store");
    CREATE INDEX "fact_orderscreenc1_test_odsc_datetime_Frame Sent to Oversea_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_datetime_Frame Sent to Overseas Desk");
    CREATE INDEX "fact_orderscreenc1_test_odsc_datetime_Issue Finished Lenses_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_datetime_Issue Finished Lenses for Both Eyes");
    CREATE INDEX "fact_orderscreenc1_test_odsc_datetime_Issue blanks_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_datetime_Issue blanks");
    CREATE INDEX "fact_orderscreenc1_test_odsc_datetime_Order Printed_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_datetime_Order Printed");
    CREATE INDEX "fact_orderscreenc1_test_odsc_datetime_Sent to Control Room_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_datetime_Sent to Control Room");
    CREATE INDEX "fact_orderscreenc1_test_odsc_datetime_Sent to Packaging_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_datetime_Sent to Packaging");
    CREATE INDEX "fact_orderscreenc1_test_odsc_datetime_Sent to Pre Quality_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_datetime_Sent to Pre Quality");
    CREATE INDEX "fact_orderscreenc1_test_odsc_datetime_Sent to Surfacing_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_datetime_Sent to Surfacing");
    CREATE INDEX "fact_orderscreenc1_test_odsc_usr_dept_Frame Sent to Lens St_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_usr_dept_Frame Sent to Lens Store");
    CREATE INDEX "fact_orderscreenc1_test_odsc_usr_dept_Frame Sent to Oversea_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_usr_dept_Frame Sent to Overseas Desk");
    CREATE INDEX "fact_orderscreenc1_test_odsc_usr_dept_Issue Finished Lenses_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_usr_dept_Issue Finished Lenses for Both Eyes");
    CREATE INDEX "fact_orderscreenc1_test_odsc_usr_dept_Issue blanks_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_usr_dept_Issue blanks");
    CREATE INDEX "fact_orderscreenc1_test_odsc_usr_dept_Order Printed_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_usr_dept_Order Printed");
    CREATE INDEX "fact_orderscreenc1_test_odsc_usr_dept_Sent to Control Room_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_usr_dept_Sent to Control Room");
    CREATE INDEX "fact_orderscreenc1_test_odsc_usr_dept_Sent to Packaging_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_usr_dept_Sent to Packaging");
    CREATE INDEX "fact_orderscreenc1_test_odsc_usr_dept_Sent to Pre Quality_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_usr_dept_Sent to Pre Quality");
    CREATE INDEX "fact_orderscreenc1_test_odsc_usr_dept_Sent to Surfacing_idx" ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_usr_dept_Sent to Surfacing");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_datetime_Sent to Packaging");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_usr_dept_Sent to Packaging");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_datetime_Sent to Branch");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_usr_dept_Branch Glazing Sent to Branch");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_usr_dept_Sent to Branch");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_datetime_Branch Glazing Sent to Branch");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_datetime_Sent to Pre Quality");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_usr_dept_Sent to Packaging");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_test USING btree ("is_dropped_Sent to Packaging");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_test USING btree ("is_dropped_Surfacing Assigned to Technician");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_datetime_Surfacing Assigned to Technician");
    CREATE INDEX ON mabawa_dw.fact_orderscreenc1_test USING btree ("odsc_usr_dept_Surfacing Assigned to Technician");
    """
    add_indexes = pg_execute(add_indexes)

    print('index_fact_c1')

# index_fact_c1()

def get_fact_collected_orders():

    data = pd.read_sql("""
    SELECT "is_dropped_Assigned to Technician", "is_dropped_Assigned to Technician at Branch", 
    "is_dropped_Attached Collection Without Receipt Proof", "is_dropped_Awaiting for Collection", 
    "is_dropped_Blanks Rejected at Control Room", "is_dropped_Blanks Sent to Control Room", 
    "is_dropped_Blanks Sent to Packaging", "is_dropped_Branch Capture Off SMART", 
    "is_dropped_Branch Frame Received from Receiver", "is_dropped_Branch Frame Sent to HQ", 
    "is_dropped_Branch Frame Sent to Lens Store", "is_dropped_Branch Frame Sent to Overseas Desk",
    "is_dropped_Branch Glazing Sent to Branch", "is_dropped_Branch Received Approval Feedback",
    "is_dropped_Branch Received Different PF/PL", "is_dropped_Branch Reject Received at Receiver",
    "is_dropped_Branch Rejected Order Received At Control Room", "is_dropped_Branch Rejected Order Sent To Control Room",
    "is_dropped_Branch Rejected Order sent to HQ", "is_dropped_Branch confirmed Query from Insurance Company",
    "is_dropped_Branch confirmed rejected Reasons by Approvals",
    "is_dropped_Branch confirmed rejected Reasons by Insurance Team", "is_dropped_Cancel Order", 
    "is_dropped_Capture OFF SMART", "is_dropped_Collected", "is_dropped_Collection With Out Receipt",
     "is_dropped_Confirmed Order", "is_dropped_Control Room Rejected", "is_dropped_Corrected Form Resent to Approvals Team", "is_dropped_Corrected Form Resent to Optica Insurance", "is_dropped_Customer Bringing Different PF/PL", "is_dropped_Customer Called to Bring PF", "is_dropped_Customer Canceled Order", "is_dropped_Customer Changes to New Frame", "is_dropped_Customer Coming To Correct Forms From Approvals Team", "is_dropped_Customer Coming to do SMART", "is_dropped_Customer Confirmed Order", "is_dropped_Customer Issue During Collection", "is_dropped_Customer Will Change Order Contents", "is_dropped_Customer to Revert", "is_dropped_Damage Sent to Control Rooom", "is_dropped_Damaged PF/PL Received at Branch", "is_dropped_Damaged PF/PL Sent to Branch", "is_dropped_Damaged PF/PL Sent to Packaging", "is_dropped_Declined by Insurance Company", "is_dropped_Draft Order Created", "is_dropped_Draft Payments Posted", "is_dropped_Fitting Process Completed at Branch", "is_dropped_Forms Confirmed and Invoice Created", "is_dropped_Forms Received at Invoice Desk", "is_dropped_Frame Sent to Lens Store", "is_dropped_Frame Sent to Overseas Desk", "is_dropped_Generated GRPO", "is_dropped_Generated PO", "is_dropped_Generated Partial GRPO", "is_dropped_HQ Item Out of Stock Getting It", "is_dropped_Home Delivery Collected", "is_dropped_Insurance Company Needs Info from Branch", "is_dropped_Insurance Company Query for Optica Insurance", "is_dropped_Insurance Fully Approved", "is_dropped_Insurance Invoice Attached", "is_dropped_Insurance Invoice Created", "is_dropped_Insurance Order Initiated", "is_dropped_Insurance Partially Approved", "is_dropped_Issue Finished Lenses for Both Eyes", "is_dropped_Issue blanks", "is_dropped_Item Damaged", "is_dropped_Lens Received at Branch", "is_dropped_Lens Store Awaiting Branch feedback from Client", "is_dropped_Lenses coming from Branch Lab", "is_dropped_Order Printed", "is_dropped_Order Printed at Branch", "is_dropped_Overseas Rejected at Later Stage", "is_dropped_PF & PL Received at Lens Store", "is_dropped_PF & PL Sent to HQ", "is_dropped_PF & PL Sent to Lens Store", "is_dropped_PF Received at HQ", "is_dropped_PF Received from Receiver", "is_dropped_PF Sent to HQ", "is_dropped_PF Sent to Lens Store", "is_dropped_PF Sent to Overseas Desk", "is_dropped_PF to Follow Receive at Lens Store", "is_dropped_PF to Follow Sent to HQ", "is_dropped_PF to Follow Sent to Overseas Desk", "is_dropped_PL Sent to HQ", "is_dropped_Partial Confirmed All Good", "is_dropped_Pre-Auth Initiated For Optica Insurance", "is_dropped_Print Invoice Hard Copy", "is_dropped_Printed Frame Identifier", "is_dropped_Printed Frame and Lens Identifier", "is_dropped_Printed Lens Identifier", "is_dropped_Printed PF Identifier", "is_dropped_Printed PF to Follow Identifier", "is_dropped_Printed Repair Identifier", "is_dropped_Product Return Received at Overseas", "is_dropped_Quality Rejected at Branch", "is_dropped_ReIssued Lens for Order", "is_dropped_Received PF to Follow Request", "is_dropped_Received at Branch", "is_dropped_Received at Control Room", "is_dropped_Rejected Frame sent to Frame Store", "is_dropped_Rejected Lenses sent to Lens Store", "is_dropped_Rejected Order Received At Control Room", "is_dropped_Rejected Order Sent To Control Room", "is_dropped_Rejected by Approvals Team", "is_dropped_Rejected by Optica Insurance", "is_dropped_Repair Order Printed", "is_dropped_Requested Courier for Home Delivery", "is_dropped_Requesting PF to Follow from Branch", "is_dropped_Resent Pre-Auth to Insurance Company", "is_dropped_SMART Captured", "is_dropped_SMART Finalized", "is_dropped_SMART Forwarded to Approvals Team", "is_dropped_SMART to be captured", "is_dropped_Sales Order Created", "is_dropped_Sales Order Created Old", "is_dropped_Scanned Frame at Branch", "is_dropped_Scanned Lenses at Branch", "is_dropped_Sent Email to Supplier Coating PO", "is_dropped_Sent For Home Delivery", "is_dropped_Sent For Qc", "is_dropped_Sent Forms to Invoice Desk", "is_dropped_Sent Pre-Auth to Insurance Company", "is_dropped_Sent to Branch", "is_dropped_Sent to Control Room", "is_dropped_Sent to Packaging", "is_dropped_Sent to Pre Quality", "is_dropped_Sent to Surfacing", "is_dropped_Sent to Workshop for Damage/Reject Analysis", "is_dropped_Surfaced Lenses Sent to HQ", "is_dropped_Surfacing Assigned to Technician", "is_dropped_Surfacing Damage/Reject Received at Control Room", "is_dropped_Surfacing Damage/Reject Sent to Control Room", "is_dropped_Surfacing Damaged By Technician", "is_dropped_Surfacing Rejected", "is_dropped_To Be Paid By Customer", "is_dropped_Update Old Order Payments", "is_dropped_Update Smart", "is_dropped_Upload Attachment", "is_dropped_Use Available Amount on SMART", "is_dropped_Used Old Approval for New Order Value Higher than Ap", "is_dropped_Used Old Approval for New Order Value Lower or Equal", "is_dropped_VAT JE Created", "is_dropped_Wrong Item Supplied", "odsc_createdby_Assigned to Technician", "odsc_createdby_Assigned to Technician at Branch", "odsc_createdby_Attached Collection Without Receipt Proof", "odsc_createdby_Awaiting for Collection", "odsc_createdby_Blanks Rejected at Control Room", "odsc_createdby_Blanks Sent to Control Room", "odsc_createdby_Blanks Sent to Packaging", "odsc_createdby_Branch Capture Off SMART", "odsc_createdby_Branch Frame Received at HQ", "odsc_createdby_Branch Frame Received from Receiver", "odsc_createdby_Branch Frame Sent to HQ", "odsc_createdby_Branch Frame Sent to Lens Store", "odsc_createdby_Branch Frame Sent to Overseas Desk", "odsc_createdby_Branch Glazing Sent to Branch", "odsc_createdby_Branch Received Approval Feedback", "odsc_createdby_Branch Received Different PF/PL", "odsc_createdby_Branch Reject Received at Receiver", "odsc_createdby_Branch Rejected Order Received At Control Room", "odsc_createdby_Branch Rejected Order Sent To Control Room", "odsc_createdby_Branch Rejected Order sent to HQ", "odsc_createdby_Branch confirmed Query from Insurance Company", "odsc_createdby_Branch confirmed rejected Reasons", "odsc_createdby_Branch confirmed rejected Reasons by Approvals", "odsc_createdby_Branch confirmed rejected Reasons by Insurance T", "odsc_createdby_Cancel Order", "odsc_createdby_Canceled PO", "odsc_createdby_Cancelled Order Lenses Reused by Overseas Full", "odsc_createdby_Capture OFF SMART", "odsc_createdby_Collected", "odsc_createdby_Collection With Out Receipt", "odsc_createdby_Confirmed Order", "odsc_createdby_Control Room Rejected", "odsc_createdby_Corrected Form Resent to Approvals Team", "odsc_createdby_Corrected Form Resent to Optica Insurance", "odsc_createdby_Corrected Forms Resent to Invoice Desk", "odsc_createdby_Customer Bringing Different PF/PL", "odsc_createdby_Customer Called to Bring PF", "odsc_createdby_Customer Canceled Order", "odsc_createdby_Customer Changes to New Frame", "odsc_createdby_Customer Coming To Correct Forms From Approvals ", "odsc_createdby_Customer Coming To Correct Forms From Optica Ins", "odsc_createdby_Customer Coming to do SMART", "odsc_createdby_Customer Confirmed Order", "odsc_createdby_Customer Issue During Collection", "odsc_createdby_Customer Will Change Order Contents", "odsc_createdby_Customer to Revert", "odsc_createdby_Damage Sent to Control Rooom", "odsc_createdby_Damaged PF/PL Received at Branch", "odsc_createdby_Damaged PF/PL Sent to Branch", "odsc_createdby_Damaged PF/PL Sent to Packaging", "odsc_createdby_Damaged by Techinician at Branch", "odsc_createdby_Declined by Insurance Company", "odsc_createdby_Draft Order Created", "odsc_createdby_Draft Payments Posted", "odsc_createdby_Fitting Process Completed at Branch", "odsc_createdby_Forms Confirmed and Invoice Created", "odsc_createdby_Forms Corrected by Invoice Desk", "odsc_createdby_Forms Received at Invoice Desk", "odsc_createdby_Forms Sent Back to Branch for Correction", "odsc_createdby_Frame Sent to Lens Store", "odsc_createdby_Frame Sent to Overseas Desk", "odsc_createdby_Generated GRPO", "odsc_createdby_Generated PO", "odsc_createdby_Generated Partial GRPO", "odsc_createdby_HQ Item Out of Stock Getting It", "odsc_createdby_Home Delivery Collected", "odsc_createdby_Insurance Company Needs Info from Branch", "odsc_createdby_Insurance Company Query for Optica Insurance", "odsc_createdby_Insurance Fully Approved", "odsc_createdby_Insurance Invoice Attached", "odsc_createdby_Insurance Invoice Created", "odsc_createdby_Insurance Partially Approved", "odsc_createdby_Issue Finished Lenses for Both Eyes", "odsc_createdby_Issue blanks", "odsc_createdby_Item Damaged", "odsc_createdby_Lens Received at Branch", "odsc_createdby_Lens Store Awaiting Branch feedback from Client", "odsc_createdby_Lenses coming from Branch Lab", "odsc_createdby_Optica Mistake", "odsc_createdby_Order Printed", "odsc_createdby_Order Printed at Branch", "odsc_createdby_Order Printed at Overseas", "odsc_createdby_Overseas Awaiting Branch feedback from Client", "odsc_createdby_Overseas Damages Transferred To Damage Store", "odsc_createdby_Overseas Rejected at Later Stage", "odsc_createdby_PF & PL Received at HQ", "odsc_createdby_PF & PL Received at Lens Store", "odsc_createdby_PF & PL Sent to HQ", "odsc_createdby_PF & PL Sent to Lens Store", "odsc_createdby_PF Received at HQ", "odsc_createdby_PF Received from Receiver", "odsc_createdby_PF Sent to HQ", "odsc_createdby_PF Sent to Lens Store", "odsc_createdby_PF Sent to Overseas Desk", "odsc_createdby_PF to Follow Receive at Lens Store", "odsc_createdby_PF to Follow Sent to HQ", "odsc_createdby_PF to Follow Sent to Lens Store", "odsc_createdby_PF to Follow Sent to Overseas Desk", "odsc_createdby_PL Received at HQ", "odsc_createdby_PL Sent to HQ", "odsc_createdby_PL Sent to Lens Store", "odsc_createdby_Partial Confirmed All Good", "odsc_createdby_Print Invoice Hard Copy", "odsc_createdby_Printed Frame Identifier", "odsc_createdby_Printed Frame and Lens Identifier", "odsc_createdby_Printed Lens Identifier", "odsc_createdby_Printed PF Identifier", "odsc_createdby_Printed PF to Follow Identifier", "odsc_createdby_Printed Repair Identifier", "odsc_createdby_Product Return Received at Overseas", "odsc_createdby_Quality Rejected at Branch", "odsc_createdby_ReIssued Lens for Order", "odsc_createdby_Received PF to Follow Request", "odsc_createdby_Received at Branch", "odsc_createdby_Received at Control Room", "odsc_createdby_Received at Packaging", "odsc_createdby_Received by Optica Insurance", "odsc_createdby_Rejected Frame sent to Frame Store", "odsc_createdby_Rejected Lenses sent to Lens Store", "odsc_createdby_Rejected Order Received At Control Room", "odsc_createdby_Rejected Order Sent To Control Room", "odsc_createdby_Rejected by Approvals Team", "odsc_createdby_Rejected by Optica Insurance", "odsc_createdby_Repair Order Printed", "odsc_createdby_Requested Courier for Home Delivery", "odsc_createdby_Requesting PF to Follow from Branch", "odsc_createdby_Resent Pre-Auth to Insurance Company", "odsc_createdby_SMART Captured", "odsc_createdby_SMART Finalized", "odsc_createdby_SMART Forwarded to Approvals Team", "odsc_createdby_Sales Order Created", "odsc_createdby_Sales Order Created Not Used", "odsc_createdby_Sales Order Created Old", "odsc_createdby_Scanned Frame at Branch", "odsc_createdby_Scanned Lenses at Branch", "odsc_createdby_Sent Email to Supplier Coating PO", "odsc_createdby_Sent For Home Delivery", "odsc_createdby_Sent For Qc", "odsc_createdby_Sent Forms to Invoice Desk", "odsc_createdby_Sent Pre-Auth to Insurance Company", "odsc_createdby_Sent to Branch", "odsc_createdby_Sent to Control Room", "odsc_createdby_Sent to Packaging", "odsc_createdby_Sent to Pre Quality", "odsc_createdby_Sent to Surfacing", "odsc_createdby_Sent to Workshop for Damage/Reject Analysis", "odsc_createdby_Supplier Mistake", "odsc_createdby_Surfaced Lenses Received at HQ", "odsc_createdby_Surfaced Lenses Sent to HQ", "odsc_createdby_Surfaced Lenses Sent to Lens Store", "odsc_createdby_Surfacing Assigned to Technician", "odsc_createdby_Surfacing Damage/Reject Received at Control Room", "odsc_createdby_Surfacing Damage/Reject Sent to Control Room", "odsc_createdby_Surfacing Damaged By Technician", "odsc_createdby_Surfacing Rejected", "odsc_createdby_To Be Paid By Customer", "odsc_createdby_Update Old Order Payments", "odsc_createdby_Update Smart", "odsc_createdby_Upload Attachment", "odsc_createdby_Use Available Amount on SMART", "odsc_createdby_Used Old Approval for New Order Value Higher tha", "odsc_createdby_Used Old Approval for New Order Value Lower or E", "odsc_createdby_VAT JE Created", "odsc_createdby_Wrong Item Supplied", "odsc_datetime_Assigned to Technician", "odsc_datetime_Assigned to Technician at Branch", "odsc_datetime_Attached Collection Without Receipt Proof", "odsc_datetime_Awaiting for Collection", "odsc_datetime_Blanks Rejected at Control Room", "odsc_datetime_Blanks Sent to Control Room", "odsc_datetime_Blanks Sent to Packaging", "odsc_datetime_Branch Capture Off SMART", "odsc_datetime_Branch Frame Received at HQ", "odsc_datetime_Branch Frame Received from Receiver", "odsc_datetime_Branch Frame Sent to HQ", "odsc_datetime_Branch Frame Sent to Lens Store", "odsc_datetime_Branch Frame Sent to Overseas Desk", "odsc_datetime_Branch Glazing Sent to Branch", "odsc_datetime_Branch Received Approval Feedback", "odsc_datetime_Branch Received Different PF/PL", "odsc_datetime_Branch Reject Received at Receiver", "odsc_datetime_Branch Rejected Order Received At Control Room", "odsc_datetime_Branch Rejected Order Sent To Control Room", "odsc_datetime_Branch Rejected Order sent to HQ", "odsc_datetime_Branch confirmed Query from Insurance Company", "odsc_datetime_Branch confirmed rejected Reasons", "odsc_datetime_Branch confirmed rejected Reasons by Approvals", "odsc_datetime_Branch confirmed rejected Reasons by Insurance Te", "odsc_datetime_Cancel Order", "odsc_datetime_Canceled PO", "odsc_datetime_Cancelled Order Lenses Reused by Overseas Full", "odsc_datetime_Capture OFF SMART", "odsc_datetime_Collected", "odsc_datetime_Collection With Out Receipt", "odsc_datetime_Confirmed Order", "odsc_datetime_Control Room Rejected", "odsc_datetime_Corrected Form Resent to Approvals Team", "odsc_datetime_Corrected Form Resent to Optica Insurance", "odsc_datetime_Corrected Forms Resent to Invoice Desk", "odsc_datetime_Customer Bringing Different PF/PL", "odsc_datetime_Customer Called to Bring PF", "odsc_datetime_Customer Canceled Order", "odsc_datetime_Customer Changes to New Frame", "odsc_datetime_Customer Coming To Correct Forms From Approvals T", "odsc_datetime_Customer Coming To Correct Forms From Optica Insu", "odsc_datetime_Customer Coming to do SMART", "odsc_datetime_Customer Confirmed Order", "odsc_datetime_Customer Issue During Collection", "odsc_datetime_Customer Will Change Order Contents", "odsc_datetime_Customer to Revert", "odsc_datetime_Damage Sent to Control Rooom", "odsc_datetime_Damaged PF/PL Received at Branch", "odsc_datetime_Damaged PF/PL Sent to Branch", "odsc_datetime_Damaged PF/PL Sent to Packaging", "odsc_datetime_Damaged by Techinician at Branch", "odsc_datetime_Declined by Insurance Company", "odsc_datetime_Draft Order Created", "odsc_datetime_Draft Payments Posted", "odsc_datetime_Fitting Process Completed at Branch", "odsc_datetime_Forms Confirmed and Invoice Created", "odsc_datetime_Forms Corrected by Invoice Desk", "odsc_datetime_Forms Received at Invoice Desk", "odsc_datetime_Forms Sent Back to Branch for Correction", "odsc_datetime_Frame Sent to Lens Store", "odsc_datetime_Frame Sent to Overseas Desk", "odsc_datetime_Generated GRPO", "odsc_datetime_Generated PO", "odsc_datetime_Generated Partial GRPO", "odsc_datetime_HQ Item Out of Stock Getting It", "odsc_datetime_Home Delivery Collected", "odsc_datetime_Insurance Company Needs Info from Branch", "odsc_datetime_Insurance Company Query for Optica Insurance", "odsc_datetime_Insurance Fully Approved", "odsc_datetime_Insurance Invoice Attached", "odsc_datetime_Insurance Invoice Created", "odsc_datetime_Insurance Order Initiated", "odsc_datetime_Insurance Partially Approved", "odsc_datetime_Issue Finished Lenses for Both Eyes", "odsc_datetime_Issue blanks", "odsc_datetime_Item Damaged", "odsc_datetime_Lens Received at Branch", "odsc_datetime_Lens Store Awaiting Branch feedback from Client", "odsc_datetime_Lenses coming from Branch Lab", "odsc_datetime_Optica Mistake", "odsc_datetime_Order Printed", "odsc_datetime_Order Printed at Branch", "odsc_datetime_Order Printed at Overseas", "odsc_datetime_Overseas Awaiting Branch feedback from Client", "odsc_datetime_Overseas Damages Transferred To Damage Store", "odsc_datetime_Overseas Rejected at Later Stage", "odsc_datetime_PF & PL Received at HQ", "odsc_datetime_PF & PL Received at Lens Store", "odsc_datetime_PF & PL Sent to HQ", "odsc_datetime_PF & PL Sent to Lens Store", "odsc_datetime_PF Received at HQ", "odsc_datetime_PF Received from Receiver", "odsc_datetime_PF Sent to HQ", "odsc_datetime_PF Sent to Lens Store", "odsc_datetime_PF Sent to Overseas Desk", "odsc_datetime_PF to Follow Receive at Lens Store", "odsc_datetime_PF to Follow Sent to HQ", "odsc_datetime_PF to Follow Sent to Lens Store", "odsc_datetime_PF to Follow Sent to Overseas Desk", "odsc_datetime_PL Received at HQ", "odsc_datetime_PL Sent to HQ", "odsc_datetime_PL Sent to Lens Store", "odsc_datetime_Partial Confirmed All Good", "odsc_datetime_Pre-Auth Initiated For Optica Insurance", "odsc_datetime_Print Invoice Hard Copy", "odsc_datetime_Printed Frame Identifier", "odsc_datetime_Printed Frame and Lens Identifier", "odsc_datetime_Printed Lens Identifier", "odsc_datetime_Printed PF Identifier", "odsc_datetime_Printed PF to Follow Identifier", "odsc_datetime_Printed Repair Identifier", "odsc_datetime_Product Return Received at Overseas", "odsc_datetime_Quality Accepted at Branch", "odsc_datetime_Quality Rejected at Branch", "odsc_datetime_ReIssued Lens for Order", "odsc_datetime_Received PF to Follow Request", "odsc_datetime_Received at Branch", "odsc_datetime_Received at Control Room", "odsc_datetime_Received at Packaging", "odsc_datetime_Received by Optica Insurance", "odsc_datetime_Rejected Frame sent to Frame Store", "odsc_datetime_Rejected Lenses sent to Lens Store", "odsc_datetime_Rejected Order Received At Control Room", "odsc_datetime_Rejected Order Sent To Control Room", "odsc_datetime_Rejected by Approvals Team", "odsc_datetime_Rejected by Optica Insurance", "odsc_datetime_Repair Order Printed", "odsc_datetime_Requested Courier for Home Delivery", "odsc_datetime_Requesting PF to Follow from Branch", "odsc_datetime_Resent Pre-Auth to Insurance Company", "odsc_datetime_SMART Captured", "odsc_datetime_SMART Finalized", "odsc_datetime_SMART Forwarded to Approvals Team", "odsc_datetime_SMART to be captured", "odsc_datetime_Sales Order Created", "odsc_datetime_Sales Order Created Not Used", "odsc_datetime_Sales Order Created Old", "odsc_datetime_Scanned Frame at Branch", "odsc_datetime_Scanned Lenses at Branch", "odsc_datetime_Sent Email to Supplier Coating PO", "odsc_datetime_Sent For Home Delivery", "odsc_datetime_Sent For Qc", "odsc_datetime_Sent Forms to Invoice Desk", "odsc_datetime_Sent Pre-Auth to Insurance Company", "odsc_datetime_Sent to Branch", "odsc_datetime_Sent to Control Room", "odsc_datetime_Sent to Packaging", "odsc_datetime_Sent to Pre Quality", "odsc_datetime_Sent to Surfacing", "odsc_datetime_Sent to Workshop for Damage/Reject Analysis", "odsc_datetime_Supplier Mistake", "odsc_datetime_Surfaced Lenses Received at HQ", "odsc_datetime_Surfaced Lenses Sent to HQ", "odsc_datetime_Surfaced Lenses Sent to Lens Store", "odsc_datetime_Surfacing Assigned to Technician", "odsc_datetime_Surfacing Damage/Reject Received at Control Room", "odsc_datetime_Surfacing Damage/Reject Sent to Control Room", "odsc_datetime_Surfacing Damaged By Technician", "odsc_datetime_Surfacing Rejected", "odsc_datetime_To Be Paid By Customer", "odsc_datetime_Update Old Order Payments", "odsc_datetime_Update Smart", "odsc_datetime_Upload Attachment", "odsc_datetime_Use Available Amount on SMART", "odsc_datetime_Used Old Approval for New Order Value Higher than", "odsc_datetime_Used Old Approval for New Order Value Lower or Eq", "odsc_datetime_VAT JE Created", "odsc_datetime_Wrong Item Supplied", "odsc_usr_dept_Assigned to Technician", "odsc_usr_dept_Assigned to Technician at Branch", "odsc_usr_dept_Attached Collection Without Receipt Proof", "odsc_usr_dept_Awaiting for Collection", "odsc_usr_dept_Blanks Rejected at Control Room", "odsc_usr_dept_Blanks Sent to Control Room", "odsc_usr_dept_Blanks Sent to Packaging", "odsc_usr_dept_Branch Capture Off SMART", "odsc_usr_dept_Branch Frame Received at HQ", "odsc_usr_dept_Branch Frame Received from Receiver", "odsc_usr_dept_Branch Frame Sent to HQ", "odsc_usr_dept_Branch Frame Sent to Lens Store", "odsc_usr_dept_Branch Frame Sent to Overseas Desk", "odsc_usr_dept_Branch Glazing Sent to Branch", "odsc_usr_dept_Branch Received Approval Feedback", "odsc_usr_dept_Branch Received Different PF/PL", "odsc_usr_dept_Branch Reject Received at Receiver", "odsc_usr_dept_Branch Rejected Order Received At Control Room", "odsc_usr_dept_Branch Rejected Order Sent To Control Room", "odsc_usr_dept_Branch Rejected Order sent to HQ", "odsc_usr_dept_Branch confirmed Query from Insurance Company", "odsc_usr_dept_Branch confirmed rejected Reasons", "odsc_usr_dept_Branch confirmed rejected Reasons by Approvals", "odsc_usr_dept_Branch confirmed rejected Reasons by Insurance Te", "odsc_usr_dept_Cancel Order", "odsc_usr_dept_Canceled PO", "odsc_usr_dept_Cancelled Order Lenses Reused by Overseas Full", "odsc_usr_dept_Capture OFF SMART", "odsc_usr_dept_Collected", "odsc_usr_dept_Collection With Out Receipt", "odsc_usr_dept_Confirmed Order", "odsc_usr_dept_Control Room Rejected", "odsc_usr_dept_Corrected Form Resent to Approvals Team", "odsc_usr_dept_Corrected Form Resent to Optica Insurance", "odsc_usr_dept_Corrected Forms Resent to Invoice Desk", "odsc_usr_dept_Customer Bringing Different PF/PL", "odsc_usr_dept_Customer Called to Bring PF", "odsc_usr_dept_Customer Canceled Order", "odsc_usr_dept_Customer Changes to New Frame", "odsc_usr_dept_Customer Coming To Correct Forms From Approvals T", "odsc_usr_dept_Customer Coming To Correct Forms From Optica Insu", "odsc_usr_dept_Customer Coming to do SMART", "odsc_usr_dept_Customer Confirmed Order", "odsc_usr_dept_Customer Issue During Collection", "odsc_usr_dept_Customer Will Change Order Contents", "odsc_usr_dept_Customer to Revert", "odsc_usr_dept_Damage Sent to Control Rooom", "odsc_usr_dept_Damaged PF/PL Received at Branch", "odsc_usr_dept_Damaged PF/PL Sent to Branch", "odsc_usr_dept_Damaged PF/PL Sent to Packaging", "odsc_usr_dept_Damaged by Techinician at Branch", "odsc_usr_dept_Declined by Insurance Company", "odsc_usr_dept_Draft Order Created", "odsc_usr_dept_Draft Payments Posted", "odsc_usr_dept_Fitting Process Completed at Branch", "odsc_usr_dept_Forms Confirmed and Invoice Created", "odsc_usr_dept_Forms Corrected by Invoice Desk", "odsc_usr_dept_Forms Received at Invoice Desk", "odsc_usr_dept_Forms Sent Back to Branch for Correction", "odsc_usr_dept_Frame Sent to Lens Store", "odsc_usr_dept_Frame Sent to Overseas Desk", "odsc_usr_dept_Generated GRPO", "odsc_usr_dept_Generated PO", "odsc_usr_dept_Generated Partial GRPO", "odsc_usr_dept_HQ Item Out of Stock Getting It", "odsc_usr_dept_Home Delivery Collected", "odsc_usr_dept_Insurance Company Needs Info from Branch", "odsc_usr_dept_Insurance Company Query for Optica Insurance", "odsc_usr_dept_Insurance Fully Approved", "odsc_usr_dept_Insurance Invoice Attached", "odsc_usr_dept_Insurance Invoice Created", "odsc_usr_dept_Insurance Order Initiated", "odsc_usr_dept_Insurance Partially Approved", "odsc_usr_dept_Issue Finished Lenses for Both Eyes", "odsc_usr_dept_Issue blanks", "odsc_usr_dept_Item Damaged", "odsc_usr_dept_Lens Received at Branch", "odsc_usr_dept_Lens Store Awaiting Branch feedback from Client", "odsc_usr_dept_Lenses coming from Branch Lab", "odsc_usr_dept_Optica Mistake", "odsc_usr_dept_Order Printed", "odsc_usr_dept_Order Printed at Branch", "odsc_usr_dept_Order Printed at Overseas", "odsc_usr_dept_Overseas Awaiting Branch feedback from Client", "odsc_usr_dept_Overseas Damages Transferred To Damage Store", "odsc_usr_dept_Overseas Rejected at Later Stage", "odsc_usr_dept_PF & PL Received at HQ", "odsc_usr_dept_PF & PL Received at Lens Store", "odsc_usr_dept_PF & PL Sent to HQ", "odsc_usr_dept_PF & PL Sent to Lens Store", "odsc_usr_dept_PF Received at HQ", "odsc_usr_dept_PF Received from Receiver", "odsc_usr_dept_PF Sent to HQ", "odsc_usr_dept_PF Sent to Lens Store", "odsc_usr_dept_PF Sent to Overseas Desk", "odsc_usr_dept_PF to Follow Receive at Lens Store", "odsc_usr_dept_PF to Follow Sent to HQ", "odsc_usr_dept_PF to Follow Sent to Lens Store", "odsc_usr_dept_PF to Follow Sent to Overseas Desk", "odsc_usr_dept_PL Received at HQ", "odsc_usr_dept_PL Sent to HQ", "odsc_usr_dept_PL Sent to Lens Store", "odsc_usr_dept_Partial Confirmed All Good", "odsc_usr_dept_Pre-Auth Initiated For Optica Insurance", "odsc_usr_dept_Print Invoice Hard Copy", "odsc_usr_dept_Printed Frame Identifier", "odsc_usr_dept_Printed Frame and Lens Identifier", "odsc_usr_dept_Printed Lens Identifier", "odsc_usr_dept_Printed PF Identifier", "odsc_usr_dept_Printed PF to Follow Identifier", "odsc_usr_dept_Printed Repair Identifier", "odsc_usr_dept_Product Return Received at Overseas", "odsc_usr_dept_Quality Accepted at Branch", "odsc_usr_dept_Quality Rejected at Branch", "odsc_usr_dept_ReIssued Lens for Order", "odsc_usr_dept_Received PF to Follow Request", "odsc_usr_dept_Received at Branch", "odsc_usr_dept_Received at Control Room", "odsc_usr_dept_Received at Packaging", "odsc_usr_dept_Received by Optica Insurance", "odsc_usr_dept_Rejected Frame sent to Frame Store", "odsc_usr_dept_Rejected Lenses sent to Lens Store", "odsc_usr_dept_Rejected Order Received At Control Room", "odsc_usr_dept_Rejected Order Sent To Control Room", "odsc_usr_dept_Rejected by Approvals Team", "odsc_usr_dept_Rejected by Optica Insurance", "odsc_usr_dept_Repair Order Printed", "odsc_usr_dept_Requested Courier for Home Delivery", "odsc_usr_dept_Requesting PF to Follow from Branch", "odsc_usr_dept_Resent Pre-Auth to Insurance Company", "odsc_usr_dept_SMART Captured", "odsc_usr_dept_SMART Finalized", "odsc_usr_dept_SMART Forwarded to Approvals Team", "odsc_usr_dept_SMART to be captured", "odsc_usr_dept_Sales Order Created", "odsc_usr_dept_Sales Order Created Not Used", "odsc_usr_dept_Sales Order Created Old", "odsc_usr_dept_Scanned Frame at Branch", "odsc_usr_dept_Scanned Lenses at Branch", "odsc_usr_dept_Sent Email to Supplier Coating PO", "odsc_usr_dept_Sent For Home Delivery", "odsc_usr_dept_Sent For Qc", "odsc_usr_dept_Sent Forms to Invoice Desk", "odsc_usr_dept_Sent Pre-Auth to Insurance Company", "odsc_usr_dept_Sent to Branch", "odsc_usr_dept_Sent to Control Room", "odsc_usr_dept_Sent to Packaging", "odsc_usr_dept_Sent to Pre Quality", "odsc_usr_dept_Sent to Surfacing", "odsc_usr_dept_Sent to Workshop for Damage/Reject Analysis", "odsc_usr_dept_Supplier Mistake", "odsc_usr_dept_Surfaced Lenses Received at HQ", "odsc_usr_dept_Surfaced Lenses Sent to HQ", "odsc_usr_dept_Surfaced Lenses Sent to Lens Store", "odsc_usr_dept_Surfacing Assigned to Technician", "odsc_usr_dept_Surfacing Damage/Reject Received at Control Room", "odsc_usr_dept_Surfacing Damage/Reject Sent to Control Room", "odsc_usr_dept_Surfacing Damaged By Technician", "odsc_usr_dept_Surfacing Rejected", "odsc_usr_dept_To Be Paid By Customer", "odsc_usr_dept_Update Old Order Payments", "odsc_usr_dept_Update Smart", "odsc_usr_dept_Upload Attachment", "odsc_usr_dept_Use Available Amount on SMART", "odsc_usr_dept_Used Old Approval for New Order Value Higher than", "odsc_usr_dept_Used Old Approval for New Order Value Lower or Eq", "odsc_usr_dept_VAT JE Created", "odsc_usr_dept_Wrong Item Supplied", t.doc_entry, so_op_diff, op_sl_diff, op_sc_diff, op_ov_diff, so_sl_diff, so_sc_diff, so_ov_diff, sl_sc_diff, sl_isb_diff, sl_iflb_diff, sl_ov_diff, op_isb_diff, op_iflb_diff, sent_sl_rop_diff, rop_sc_diff, rop_isb_diff, rop_iflb_diff, received_sl_rop_diff, sent_sl_sc_diff, received_sl_sc_diff, sc__ssf_diff, sc_sp_diff, sc_spqc_diff, bsc_ssf_diff, bsc_sp_diff, bsc_spqc_diff, ssf__astch_diff, astch_spqc_diff, ssf_spqc_diff, spqc__astch_diff, spqc__stp_diff, astch_stp_diff, sp_stb_diff, sp_gbstb_diff
    FROM mabawa_dw.fact_orderscreenc1_test t 
    join mabawa_staging.source_collected_orders c on t.doc_entry = c.doc_entry 
    """, con=engine)

    
    data.to_sql('fact_collected_orderscreenc1', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    
    print('get_fact_collected_orders')

# create_source_orderscreenc1_staging()
# create_source_orderscreenc1_staging2()
# create_source_orderscreenc1_staging3()
# create_source_orderscreenc1_staging4()
# update_orderscreenc1_staging4()
# get_collected_orders()
# transpose_orderscreenc1()
# index_trans()
# create_fact_orderscreenc1_new()
# reindex_db()
# get_techs()
# create_source_orderscreenc1_staging5()
# index_staging5()
# create_fact_orderscreenc1_staging()
# create_fact_orderscreenc1()
# index_fact_c1()
# get_fact_collected_orders()

