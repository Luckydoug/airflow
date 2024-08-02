import sys
sys.path.append(".")
import requests
import datetime
import pandas as pd
import businesstimedelta
import holidays as pyholidays
from airflow.models import Variable
from workalendar.africa import Kenya
from pangres import upsert
from datetime import date, timedelta, time, datetime
from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate

# today = date.today()
# pastdate = today - timedelta(days=14)
# FromDate = pastdate.strftime('%Y/%m/%d')
# # FromDate = '2024/06/01'
# ToDate = date.today().strftime('%Y/%m/%d')

def fetch_sap_invt_transfer_request ():
    SessionId = return_session_id(country = "Kenya")
    pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetITRDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    pagecount_payload={}
    pagecount_headers = {}
    
    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    
    transfer_request = pd.DataFrame()
    payload={}
    headers = {}
    pages = data['result']['body']['recs']['PagesCount']

    print("Retrived No of Pages", pages)

    for i in range(1, pages+1):
        print(i)
        page = i
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetITRDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()

        try:
            response = response['result']['body']['recs']['Results']
            response = pd.DataFrame.from_dict(response)
            transfer_request = transfer_request.append(response, ignore_index=True)
        except:
            print('error')
        
    
    print(transfer_request)

    """
    CREATING DETAILS TABLE
    """

    transfer_request_details = transfer_request['details'].apply(pd.Series)
    print(transfer_request_details)
    print('transfer_request_details')

    print("Created Details DF")

    transfer_request_details.rename (columns = {
            'Internal_Number':'internal_no',
            'ITRNO':'itr_no',
            'Document_Number':'doc_no',
            'Canceled':'canceled',
            'Document_Status':'doc_status',
            'Warehouse_Status':'whse_status',
            'Posting_Date':'post_date',
            'Document_Total':'doc_total',
            'Generation_Time':'generation_time',
            'Creation_Date':'createdon',
            'User_Signature':'user_signature',
            'Creatn_Time_Incl_Secs':'creationtime_incl_secs',
            'Remarks':'remarks',
            'ExchangeType':'exchange_type',
            'Filler':'filler',
            'ToWareHouse_Code':'to_warehouse_code'}
        ,inplace=True)

    print("Renamed Columns")

    transfer_request_details.drop_duplicates(subset='internal_no',inplace=True)
    transfer_request_details = transfer_request_details.set_index('internal_no')

    print("Indexes set")

    
    upsert(engine=engine,
        df=transfer_request_details,
        schema='mabawa_staging',
        table_name='source_itr',
        if_row_exists='update',
        create_table=False)

    """
    CREATING ITEM DETAILS TABLE
    """
    transfer_request_itemdetails = transfer_request['itemdetails']
    transfer_request_itemdetails = transfer_request_itemdetails.to_frame()

    itemdetails_df = pd.DataFrame()
    for index, row in transfer_request_itemdetails.iterrows():
        row_data=row['itemdetails']
        data = pd.DataFrame.from_dict(row_data)
        data1 = data.T
        itemdetails_df = itemdetails_df.append(data1, ignore_index=True)

    print("Created Item Details DF")
    # print(itemdetails_df)
    itemdetails_df.rename (columns = {
            'Document_Internal_ID':'doc_internal_id',
            'RowNumber':'row_no',
            'Target_Document_Internal_ID':'targ_doc_internal_id',
            'Row_Status':'row_status',
            'Item_No':'item_no',
            'Quantity':'qty',
            'Posting_Date':'post_date',
            'Sales_Order_number':'sales_orderno',
            'Sales_Order_entry':'sales_order_entry',
            'Sales_Order_branch':'sales_order_branch',
            'Draft_order_entry':'draft_order_entry',
            'Draft_order_Number':'draft_orderno',
            'Draft_Order_Branch':'draft_order_branch',
            'Flag_for_INV':'flag_for_inv',
            'ITR_Status':'itr__status',
            'Replaced_ItemCode':'replaced_itemcode',
            'Picker_Name':'picker_name',
            'Line_Was_Closed_Manually': 'line_was_closed_manually'
            }
        ,inplace=True)

    print("Renamed Detailed Columns")

    itemdetails_df.drop_duplicates(subset=['doc_internal_id','row_no'],inplace=True)
    itemdetails_df = itemdetails_df.set_index(['doc_internal_id','row_no'])

    upsert(engine=engine,
        df=itemdetails_df,
        schema='mabawa_staging',
        table_name='source_itr_details',
        if_row_exists='update',
        create_table=False)

    print("Rows Inserted")

    #itemdetails_df.to_sql('source_itr_details', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    

    return 

# fetch_sap_invt_transfer_request()


def create_fact_itr_details ():

    query = """
    truncate mabawa_dw.fact_itr_details;
    insert into mabawa_dw.fact_itr_details
    SELECT 
        distinct
        doc_internal_id, row_no, targ_doc_internal_id, row_status, 
        itrd.item_no, qty, post_date, itrd.sales_orderno, sales_order_entry, sales_order_branch, 
        draft_order_entry, draft_orderno, draft_order_branch, flag_for_inv, itr__status, 
        replaced_itemcode, picker_name, t.technician_name, i.item_group, whse.warehouse_code  
    FROM mabawa_mviews.v_fact_itr_details itrd 
    left join mabawa_dw.dim_items i on itrd.item_no = i.item_code 
    left join mabawa_staging.source_technicians t on itrd.picker_name::text = t.technician_code::text
    left join mabawa_mviews.salesorders_with_item_whse whse on itrd.sales_orderno::text = whse.sales_orderno::text 
    and itrd.item_no::text = whse.item_no::text
    """
    query=pg_execute(query)
    return "Created"

def create_first_mviews_salesorders_with_item_whse():

    query = """
    truncate mabawa_mviews.salesorders_with_item_whse;
    insert into mabawa_mviews.salesorders_with_item_whse
    SELECT sales_orderno, internal_number, order_canceled, document_status, warehouse_status, 
    posting_date, order_branch, id, item_no, warehouse_code
    FROM mabawa_mviews.v_salesorders_with_item_whse;
    """
    query = pg_execute(query)

    return "Complete"

def update_fact_itr_details ():

    query = """
    update mabawa_dw.fact_itr_details itrd
    set warehouse_code = whse.warehouse_code 
    from mabawa_mviews.salesorders_with_item_whse whse 
    where itrd.sales_orderno::text = whse.sales_orderno::text 
    and itrd.replaced_itemcode::text = whse.item_no::text
    and itrd.warehouse_code is null
    """
    query=pg_execute(query)
    return "Created"

def create_itr_sales_orders():
    query = """
    truncate mabawa_mviews.itr_sales_orders;
    insert into mabawa_mviews.itr_sales_orders
    SELECT doc_internal_id, sales_orderno
    FROM mabawa_mviews.v_itr_sales_orders;
    """
    query = pg_execute(query)
    return "Updated"

def create_mviews_fact_itr():
    query = """
    truncate table mabawa_mviews.fact_itr;
    insert into mabawa_mviews.fact_itr
    SELECT internal_no, itr_no, doc_no, 
    sales_orderno, salesorder_created_datetime, 
    branch_code, canceled, doc_status, whse_status, 
    post_date, doc_total, generation_time, createdon, 
    user_signature, creationtime_incl_secs_int, 
    creationtime_incl_secs, creation_datetime, 
    remarks, exchange_type
    FROM mabawa_mviews.v_fact_itr;
    """
    query = pg_execute(query)
    return "Updated"

def update_null_branches():
    query = """
    update mabawa_mviews.fact_itr f
    set branch_code = s.warehouse_code 
    from mabawa_mviews.salesorders_with_item_whse s
    where f.branch_code is null 
    and f.sales_orderno::text = s.sales_orderno::text
    """
    query = pg_execute(query)
    return "Something"

def create_mviews_fact_itr_2():
    query = """
    truncate mabawa_mviews.fact_itr_2;
    insert into mabawa_mviews.fact_itr_2
    SELECT internal_no, itr_no, doc_no, sales_orderno, salesorder_created_datetime, salesorder_created_time, 
    branch_code, canceled, doc_status, whse_status, post_date, doc_total, generation_time, user_signature, 
    picklist_createdon, picklist_createdtime_int, picklist_createdtime, picklist_created_datetime, remarks, 
    exchange_type, whse_name, brs_first_cut, brs_second_cut, brs_cutoff_step, brs_cutoffs, lens_store_first_cut, 
    lens_store_second_cut, lenstore_cutoff_step, lens_store_cutoffs, designer_store_first_cut, designer_store_second_cut, 
    designerstore_cutoff_step, designer_store_cutoffs, main_store_first_cut, main_store_second_cut, mainstore_cutoff_step,
    main_store_cutoffs, control_first_cut, control_second_cut, control_cutoff_step, control_store_cutoffs, 
    packaging_first_cut, packaging_second_cut, packaging_cutoff_step, packaging_store_cutoffs
    FROM mabawa_mviews.v_fact_itr_2;
    """
    query = pg_execute(query)
    return "Updated"

def create_mviews_fact_itr_3():
    query = """
    truncate mabawa_mviews.fact_itr_3;
    insert into mabawa_mviews.fact_itr_3
    SELECT 
        distinct
        internal_no, itr_no, doc_no, sales_orderno, salesorder_created_datetime, salesorder_created_time, 
        branch_code, canceled, doc_status, whse_status, post_date, doc_total, generation_time, user_signature, 
        picklist_createdon, picklist_createdtime_int, picklist_createdtime, picklist_created_datetime, remarks, 
        exchange_type, whse_name, brs_first_cut, brs_second_cut, brs_cutoff_step, brs_cutoffs, brs_cutoff_status, 
        lens_store_first_cut, lens_store_second_cut, lenstore_cutoff_step, designer_store_first_cut, 
        designer_store_second_cut, designerstore_cutoff_step, main_store_first_cut, main_store_second_cut, 
        mainstore_cutoff_step, control_first_cut, control_second_cut, control_cutoff_step, packaging_first_cut, 
        packaging_second_cut, packaging_cutoff_step, replacement_check
    FROM mabawa_mviews.v_fact_itr_3;
    """
    query = pg_execute(query)
    return "Updated"

def create_mviews_fact_itr_4():
    query = """
    truncate mabawa_mviews.fact_itr_4;
    insert into mabawa_mviews.fact_itr_4
    SELECT 
        distinct
        internal_no, itr_no, doc_no, itr.sales_orderno, salesorder_created_datetime, salesorder_created_time, 
        branch_code,itrd.warehouse_code, canceled, doc_status, whse_status, itr.post_date, doc_total, generation_time, 
        user_signature, picklist_createdon, picklist_createdtime_int, picklist_createdtime, picklist_created_datetime, 
        remarks, exchange_type, whse_name, brs_first_cut, brs_second_cut, brs_cutoff_step, brs_cutoffs, 
        brs_cutoff_status, lens_store_first_cut, lens_store_second_cut, lenstore_cutoff_step, designer_store_first_cut, 
        designer_store_second_cut, designerstore_cutoff_step, main_store_first_cut, main_store_second_cut, 
        mainstore_cutoff_step, control_first_cut, control_second_cut, control_cutoff_step, packaging_first_cut, 
        packaging_second_cut, packaging_cutoff_step, replacement_check
    FROM mabawa_mviews.fact_itr_3 itr
    join mabawa_dw.fact_itr_details itrd on itr.internal_no = itrd.doc_internal_id 
    and itr.sales_orderno = itrd.sales_orderno and itrd.warehouse_code <> ' '
    """
    query = pg_execute(query)
    return "Updated"

def create_branch_picklist_cutoffs():
    query = """
    truncate mabawa_mviews.branch_picklist_cutoffs;
    insert into mabawa_mviews.branch_picklist_cutoffs
    SELECT warehouse_code, picklist_createdon, picklist_createdtime
    FROM mabawa_mviews.v_branch_picklist_cutoffs;
    """
    query = pg_execute(query)
    return "Updated"

def update_cutoff_status():
    
    query = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status,
        c.picklist_createdtime as picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c on itr.warehouse_code = c.warehouse_code 
    and salesorder_created_datetime::date = c.picklist_createdon::date
    where itr.salesorder_created_time::time < c.picklist_createdtime::time
    and itr.picklist_created_datetime <= (itr.salesorder_created_datetime::date||' '||c.picklist_createdtime::time)::timestamp) as a)
    """
    query = pg_execute(query)

    query1 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status,
        c.picklist_createdtime as picklist_cutoff,
        c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c on itr.warehouse_code = c.warehouse_code 
    and salesorder_created_datetime::date = c.picklist_createdon::date
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '1 day')::date = c1.picklist_createdon::date
    where itr.salesorder_created_time::time > c.picklist_createdtime::time
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '1 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query1 = pg_execute(query1)

    query2 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status,
        c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '2 day')::date = c1.picklist_createdon::date
    join mabawa_dw.dim_dates d on itr.salesorder_created_datetime::date = d.pk_date 
    where d.day_of_week = 6
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '2 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query2 = pg_execute(query2)

    query3 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status,
        c.picklist_createdtime as picklist_cutoff,
        c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c on itr.warehouse_code = c.warehouse_code 
    and salesorder_created_datetime::date = c.picklist_createdon::date
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '1 day')::date = c1.picklist_createdon::date
    join mabawa_dw.dim_dates d on itr.salesorder_created_datetime::date = d.pk_date 
    where d.day_of_week = 0
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '1 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query3 = pg_execute(query3)

    query4 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status,
        c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '3 day')::date = c1.picklist_createdon::date
    join mabawa_dw.dim_dates d on itr.salesorder_created_datetime::date = d.pk_date 
    where itr.salesorder_created_datetime::date = '2022-05-01' 
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '3 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query4 = pg_execute(query4)

    query5 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status,
        c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '2 day')::date = c1.picklist_createdon::date
    join mabawa_dw.dim_dates d on itr.salesorder_created_datetime::date = d.pk_date 
    where itr.salesorder_created_datetime::date = '2022-05-02' 
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '2 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query5 = pg_execute(query5)

    query6 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status,
        c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '1 day')::date = c1.picklist_createdon::date
    join mabawa_dw.dim_dates d on itr.salesorder_created_datetime::date = d.pk_date 
    where itr.salesorder_created_datetime::date = '2022-05-03' 
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '1 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query6 = pg_execute(query6)

    query7 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status,
        c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '4 day')::date = c1.picklist_createdon::date
    join mabawa_dw.dim_dates d on itr.salesorder_created_datetime::date = d.pk_date 
    where itr.salesorder_created_datetime::date = '2022-04-30' 
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '4 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query7 = pg_execute(query7)

    query8 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status,
        c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '3 day')::date = c1.picklist_createdon::date
    join mabawa_dw.dim_dates d on itr.salesorder_created_datetime::date = d.pk_date 
    where itr.salesorder_created_datetime::date = '2022-07-09' 
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '3 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query8 = pg_execute(query8)

    query9 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status,
        c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '2 day')::date = c1.picklist_createdon::date
    join mabawa_dw.dim_dates d on itr.salesorder_created_datetime::date = d.pk_date 
    where itr.salesorder_created_datetime::date = '2022-07-10' 
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '2 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query9 = pg_execute(query9)

    query10 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status,
        c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '1 day')::date = c1.picklist_createdon::date
    join mabawa_dw.dim_dates d on itr.salesorder_created_datetime::date = d.pk_date 
    where itr.salesorder_created_datetime::date = '2022-07-11' 
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '1 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query10 = pg_execute(query10)

    query11 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status,
        c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '2 day')::date = c1.picklist_createdon::date
    join mabawa_dw.dim_dates d on itr.salesorder_created_datetime::date = d.pk_date 
    where itr.salesorder_created_datetime::date = '2022-05-31' 
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '2 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query11 = pg_execute(query11)

    query12 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status
        --c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '4 day')::date = c1.picklist_createdon::date 
    where itr.salesorder_created_datetime::date = '2022-04-15' 
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '4 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query12 = pg_execute(query12)

    query13 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status
        --c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '3 day')::date = c1.picklist_createdon::date 
    where itr.salesorder_created_datetime::date = '2022-04-16' 
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '3 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query13 = pg_execute(query13)

    query14 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status
        --c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '2 day')::date = c1.picklist_createdon::date 
    where itr.salesorder_created_datetime::date = '2022-04-17' 
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '2 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query14 = pg_execute(query14)

    query15 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status
        --c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '1 day')::date = c1.picklist_createdon::date 
    where itr.salesorder_created_datetime::date = '2022-04-18' 
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '1 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query15 = pg_execute(query15)

    query16 = """
    update mabawa_mviews.fact_itr_4 itr 
    set brs_cutoff_status = 'Within Cutoff'
    where itr.internal_no||' '||itr.warehouse_code in 
    (select 
        a.internal_no||' '||a.warehouse_code
    from 
    (select 
        internal_no, doc_no, sales_orderno,
        salesorder_created_datetime,
        itr.warehouse_code,
        picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status,
        c.picklist_createdtime as picklist_cutoff,
        c1.picklist_createdtime as nextday_picklist_cutoff
    from mabawa_mviews.fact_itr_4 itr 
    join mabawa_mviews.branch_picklist_cutoffs c on itr.warehouse_code = c.warehouse_code 
    and salesorder_created_datetime::date = c.picklist_createdon::date
    join mabawa_mviews.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '1 day')::date = c1.picklist_createdon::date
    where itr.salesorder_created_time::time > c.picklist_createdtime::time
    and itr.salesorder_created_datetime::date = '2022-08-08' 
    and itr.picklist_created_datetime <= ((itr.salesorder_created_datetime+interval '2 day')::date||' '||c1.picklist_createdtime::time)::timestamp) as a)
    """
    query16 = pg_execute(query16)

    # query = """
    # update mabawa_mviews.fact_itr_4
    # set brs_cutoff_status = 'Within Cutoff'
    # where salesorder_created_datetime::date between '2022-05-01' and '2022-05-03'
    # and picklist_createdtime::time <= brs_first_cut::time
    # """
    # query = pg_execute(query)

    # query2 = """
    # update mabawa_mviews.fact_itr_4 f
    # set brs_cutoff_status = 'Within Cutoff'
    # from mabawa_dw.dim_dates d
    # where d.pk_date = f.salesorder_created_datetime::date
    # and salesorder_created_datetime::date not between '2022-05-01' and '2022-05-03'
    # and d.day_of_week = 0
    # and picklist_created_datetime  <= ((salesorder_created_datetime+interval '1 day')::date||' '||brs_first_cut::time)::timestamp
    # """
    # query2 = pg_execute(query2)

    # query3 = """
    # update mabawa_mviews.fact_itr_4 f
    # set brs_cutoff_status = 'Within Cutoff'
    # from mabawa_dw.dim_dates d
    # where d.pk_date = f.salesorder_created_datetime::date
    # and salesorder_created_datetime::date not between '2022-05-01' and '2022-05-03'
    # and d.day_of_week = 6
    # and salesorder_created_datetime::time > brs_second_cut
    # and picklist_created_datetime  <= ((salesorder_created_datetime+interval '2 day')::date||' '||brs_first_cut::time)::timestamp
    # """
    # query3 = pg_execute(query3)

    # query4 = """
    # update mabawa_mviews.fact_itr_4 f 
    # set brs_cutoff_status = 'Within Cutoff'
    # where salesorder_created_time::time > brs_second_cut 
    # and picklist_created_datetime  <= ((salesorder_created_datetime+interval '1 day')::date||' '||brs_first_cut::time)::timestamp
    # """

    # query4 = pg_execute(query4)

    # query5 = """
    # update mabawa_mviews.fact_itr_4 f 
    # set brs_cutoff_status = 'Within Cutoff'
    # where salesorder_created_datetime::date = '2022-05-31'
    # and salesorder_created_time::time > brs_second_cut 
    # and picklist_created_datetime  <= ((salesorder_created_datetime+interval '2 day')::date||' '||brs_first_cut::time)::timestamp
    # """

    # query5 = pg_execute(query5)

    # query6 = """
    # update mabawa_mviews.fact_itr_4
    # set brs_cutoff_status = 'Within Cutoff'
    # where salesorder_created_datetime::date = '2022-06-01'
    # and picklist_created_datetime  <= ((salesorder_created_datetime+interval '1 day')::date||' '||brs_first_cut::time)::timestamp
    # """
    # query6 = pg_execute(query6)

    # query7 = """
    # update mabawa_mviews.fact_itr_4 f
    # set brs_cutoff_status = 'Within Cutoff'
    # from mabawa_dw.dim_dates d
    # where d.pk_date = f.salesorder_created_datetime::date
    # and salesorder_created_datetime::date = '2022-07-09'
    # and picklist_created_datetime  <= ((salesorder_created_datetime+interval '3 day')::date||' '||brs_first_cut::time)::timestamp
    # """
    # query7 = pg_execute(query7)

    # query8 = """
    # update mabawa_mviews.fact_itr_4 f
    # set brs_cutoff_status = 'Within Cutoff'
    # from mabawa_dw.dim_dates d
    # where d.pk_date = f.salesorder_created_datetime::date
    # and salesorder_created_datetime::date = '2022-07-10'
    # and picklist_created_datetime  <= ((salesorder_created_datetime+interval '2 day')::date||' '||brs_first_cut::time)::timestamp
    # """
    # query8 = pg_execute(query8)

    # query9 = """
    # update mabawa_mviews.fact_itr_4 f
    # set brs_cutoff_status = 'Within Cutoff'
    # from mabawa_dw.dim_dates d
    # where d.pk_date = f.salesorder_created_datetime::date
    # and salesorder_created_datetime::date = '2022-07-11'
    # and picklist_created_datetime  <= ((salesorder_created_datetime+interval '1 day')::date||' '||brs_first_cut::time)::timestamp
    # """
    # query9 = pg_execute(query9)

    # query10 = """
    # update mabawa_mviews.fact_itr_4
    # set brs_cutoff_status = 'Within Cutoff'
    # where salesorder_created_datetime::date = '2022-07-11'
    # and picklist_createdtime::time <= brs_first_cut::time
    # """
    # query10 = pg_execute(query10)

    # query11 = """
    # update mabawa_mviews.fact_itr_4
    # set brs_cutoff_status = 'Within Cutoff'
    # where salesorder_created_datetime::date = '2022-07-11'
    # and picklist_created_datetime  <= ((salesorder_created_datetime+interval '1 day')::date||' '||brs_first_cut::time)::timestamp
    # """
    # query11 = pg_execute(query11)

    # query12 = """
    # update mabawa_mviews.fact_itr_4 f
    # set brs_cutoff_status = 'Within Cutoff'
    # where salesorder_created_datetime::date = '2022-07-09'
    # and salesorder_created_datetime::time > brs_second_cut
    # and picklist_created_datetime  <= ((salesorder_created_datetime+interval '3 day')::date||' '||brs_first_cut::time)::timestamp
    # """
    # query12 = pg_execute(query12)

    return "Updated"

def create_mview_fact_itr3():
    
    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)
    
    print("Fetched Holidays")
    
    df = pd.read_sql("""
    SELECT 
        internal_no, itr_no, doc_no, sales_orderno, salesorder_created_datetime, salesorder_created_time, 
        branch_code, warehouse_code, canceled, doc_status, whse_status, post_date, doc_total, generation_time, 
        user_signature, picklist_createdon, picklist_createdtime_int, picklist_createdtime, picklist_created_datetime, 
        remarks, exchange_type, whse_name, brs_first_cut, brs_second_cut, brs_cutoff_step, 
        brs_cutoff_status, lens_store_first_cut, lens_store_second_cut, lenstore_cutoff_step, designer_store_first_cut, 
        designer_store_second_cut, designerstore_cutoff_step, main_store_first_cut, main_store_second_cut, 
        mainstore_cutoff_step, control_first_cut, control_second_cut, control_cutoff_step, packaging_first_cut, 
        packaging_second_cut, packaging_cutoff_step, replacement_check
    FROM mabawa_mviews.fact_itr_4
    where picklist_created_datetime >= '2022-01-01'
    and sales_orderno is not null
    and exchange_type = 'Replacement'
    and replacement_check = 'BranchStock'
    and branch_code not in ('HOM','GSA')
    """, con=engine)

    print("Fetched ITR Data")

    df['salesorder_created_datetime'] = pd.to_datetime(df['salesorder_created_datetime'])
    df['picklist_created_datetime'] = pd.to_datetime(df['picklist_created_datetime'])

    print("Converted Date Fields to DateTime")

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

    print("Initiating Working Hours Calculations")

    df['so_brs_diff']=df.apply(lambda row: BusHrs(row['salesorder_created_datetime'], row['picklist_created_datetime']), axis=1)
    
    print("Completed Calculating Working Hours for BRS")

    df['so_brs_diff'] = pd.to_numeric(df['so_brs_diff'])

    print("Converted Numeric Fields")
    """
    df = df.set_index('internal_no')

    print("Data Indexed")

    df.index[df.index.duplicated(keep=False)]

    print("Dropped Duplicates")

    upsert(engine=engine,
       df=df,
       schema='mabawa_dw',
       table_name='fact_itr',
       if_row_exists='update',
       create_table=False)

    print("Data Upserted")
    """
    query = """truncate mabawa_mviews.fact_itr3"""
    query = pg_execute(query)

    df.to_sql('fact_itr3', con = engine, schema='mabawa_mviews', if_exists = 'append', index=False)

    return "Updated"

def update_itrs_issues():
    query = """
    update mabawa_mviews.fact_itr3 itr 
    set dropped_status = 1 
    from mabawa_dw.dim_itr_issues its 
    where itr.doc_no::text = its.itr_no::text
    and upper(its.issue_dept)= 'BRS'
    """
    query = pg_execute(query)
    return "Updated"

def update_itrs_time_issues():
    query = """
    update mabawa_mviews.fact_itr3 itr 
    set dropped_status = 1 
    from mabawa_dw.dim_itr_time_issues itts 
    where itr.salesorder_created_datetime::date = itts.issue_date
    and itr.salesorder_created_time::time between itts.issue_start_time::time and itts.issue_end_time 
    and upper(itts.issue_dept) in ('ALL', 'BRS')
    """
    query = pg_execute(query)
    return "Updated"

def create_fact_itr():
    query = """
    truncate mabawa_dw.fact_itr;
    insert into mabawa_dw.fact_itr
    SELECT 
        internal_no, itr_no, doc_no, sales_orderno, salesorder_created_datetime, 
        salesorder_created_time, branch_code, canceled, doc_status, whse_status, post_date, 
        doc_total, generation_time, user_signature, picklist_createdon, picklist_createdtime_int, 
        picklist_createdtime, picklist_created_datetime, remarks, exchange_type, whse_name, brs_first_cut, 
        brs_second_cut, brs_cutoff_step, brs_cutoff_status, lens_store_first_cut, lens_store_second_cut, 
        lenstore_cutoff_step, designer_store_first_cut, designer_store_second_cut, designerstore_cutoff_step, 
        main_store_first_cut, main_store_second_cut, mainstore_cutoff_step, control_first_cut, control_second_cut, 
        control_cutoff_step, packaging_first_cut, packaging_second_cut, packaging_cutoff_step, replacement_check, 
        so_brs_diff, dropped_status, warehouse_code
    FROM mabawa_mviews.fact_itr3
    where user_signature = 1343
    """
    query = pg_execute(query)
    return "Updated"



def update_odd_statuses():
    query = """
    update mabawa_dw.fact_itr itr 
        set brs_cutoff_status = 'Within Cutoff'
        where itr.internal_no in
    (select 
        a.internal_no
    from
    (SELECT distinct
        internal_no, doc_no, sales_orderno , salesorder_created_datetime,
        itr.branch_code , picklist_created_datetime, brs_first_cut,
        brs_second_cut, brs_cutoff_status,
        c.picklist_createdtime as picklist_cutoff,
        ((itr.salesorder_created_datetime+interval '1 day')::date||' '||c1.picklist_createdtime::time)::timestamp as checkcol
    FROM mabawa_dw.fact_itr itr 
    join mabawa_dw.branch_picklist_cutoffs c on itr.warehouse_code = c.warehouse_code 
    and salesorder_created_datetime::date = c.picklist_createdon::date
    join mabawa_dw.branch_picklist_cutoffs c1 on itr.warehouse_code = c1.warehouse_code 
    and (salesorder_created_datetime+interval '1 day')::date = c1.picklist_createdon::date
    where itr.brs_cutoff_status = 'Not Within Cutoff') as a
    where a.picklist_created_datetime <= checkcol)
    """
    query = pg_execute(query)

    # query = """
    # update mabawa_dw.fact_itr itr 
    # set brs_cutoff_status = 'Within Cutoff'
    # from mabawa_dw.branch_picklist_cutoffs c 
    # where itr.branch_code = c.branch_code 
    # and salesorder_created_datetime::date = c.picklist_createdon::date 
    # and salesorder_created_time::time >= c.picklist_createdtime::time
    # and itr.brs_cutoff_status = 'Not Within Cutoff'
    # and itr.picklist_created_datetime  <= ((itr.salesorder_created_datetime+interval '1 day')::date||' '||c.picklist_createdtime::time)::timestamp
    # """
    # query = pg_execute(query)
    return "Updated"