import sys

from numpy import nan
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import datetime
import pandas as pd
from io import StringIO
from airflow.models import Variable 
from sqlalchemy import create_engine
from datetime import date, timedelta
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 

from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.api_login.api_login import(login)

conn = psycopg2.connect(host="10.40.16.19",database="mawingu", user="postgres", password="@Akb@rp@$$w0rtf31n")

SessionId = login()

# FromDate = '2021/01/01'
# ToDate = '2020/12/31'

today = date.today()
pastdate = today - timedelta(days=1)
FromDate = pastdate.strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')

# api details

pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrders&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
pagecount_payload={}
pagecount_headers = {}

def fetch_sap_orders():

    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    
    orders_header = pd.DataFrame()
    orders_line = pd.DataFrame()
    payload={}
    headers = {}
    pages = data['result']['body']['recs']['PagesCount']

    print("Retrieved No of Pages")
    print(pages)

    for i in range(1, pages+1):
        page = i
        print(page)
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrders&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()
        response = response['result']['body']['recs']['Results']
        headers_df = pd.DataFrame([d['details'] for d in response])
        line_df = pd.DataFrame([{"id": k, **v} for d in response for k, v in d['itemdetails'].items()])
        orders_header = orders_header.append(headers_df, ignore_index=True)
        orders_line = orders_line.append(line_df, ignore_index=True)
    
    print("Finished API calls")

    '''
    ORDERS HEADERS
    '''
    orders_header.rename (columns = {
            'Internal_Number':'internal_number',
            'Document_Number':'document_number',
            'Canceled':'order_canceled',
            'Document_Status':'document_status',
            'Warehouse_Status':'warehouse_status',
            'Posting_Date':'posting_date',
            'Customer_Vendor_Code':'cust_vendor_code',
            'Total_Tax':'total_tax',
            'Discount_for_Document':'order_document_discount',
            'Total_Discount':'order_total_discount',
            'Document_Total':'doc_total',
            'Generation_Time':'generation_time',
            'Sales_Employee':'sales_employee',
            'Tax_Amount_SC':'tax_amt_sc',
            'Document_Total_SC':'doc_total_sc',
            'Creation_Date':'creation_date',
            'User_Signature':'user_signature',
            'Creatn_Time_Incl_Secs':'creation_time',
            'Prescription_Spectacle':'presc_spec',
            'Advanced_Payment':'advanced_payment',
            'Remaining_Payment':'remaining_payment',
            'Order_Screen':'orderscreen',
            'Branch':'order_branch',
            'Prescription_No':'presc_no',
            'Presc_Print_Type':'presc_print_type',
            'Draft_Order_No':'draft_orderno'}
        ,inplace=True)

    print("Header Columns Renamed")

    orders_header["posting_date"] = pd.to_datetime(orders_header["posting_date"])
    
    # orders_header = orders_header.set_index(['internal_number','document_number'])
    orders_header = orders_header.drop_duplicates(['internal_number','document_number']).set_index(['internal_number','document_number'])

    upsert(engine=engine,
    df=orders_header,
    schema='mabawa_staging',
    table_name='source_orders_header',
    if_row_exists='update',
    create_table=True)
    
    #orders_header.to_sql('source_tests', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)

    print("Inserted Orders Header")
    
    '''
    ORDERS LINE
    '''
    orders_line.rename (columns = {
            'DocumentInternal_ID':'doc_internal_id',
            'Target_Document_Internal_ID':'target_doc_internal_id',
            'Row_Status':'row_status',
            'Item_No':'item_no',
            'Quantity':'qty',
            'Unit_Price':'unit_price',
            'Discount_per_Row':'disc_per_row',
            'Warehouse_Code':'warehouse_code',
            'Gross_Price_after_Discount':'gross_price_after_disc',
            'Total_Tax_Row':'total_tax_row',
            'Flag_for_order':'flag_for_order'}
        ,inplace=True)

    print("Line Columns Renamed")

    orders_line['item_no'].fillna("No ITEM", inplace = True)

    """
    orders_line = orders_line.set_index(['id', 'doc_internal_id', 'item_no'])

    upsert(engine=engine,
       df=orders_line,
       schema='mabawa_staging',
       table_name='source_orders_line',
       if_row_exists='update',
       create_table=True)

    print("Inserted Orders Lines")
    """
    query = """truncate mabawa_staging.landing_source_orders_line;"""
    query = pg_execute(query)

    orders_line = orders_line.drop_duplicates(['id', 'doc_internal_id', 'item_no'])

    orders_line.to_sql('landing_source_orders_line', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)

    print("fetch_sap_orders done")

def source_orders_header_with_prescriptions():

    query = """
    truncate table mabawa_mviews.source_orders_header_with_prescriptions;
    insert into mabawa_mviews.source_orders_header_with_prescriptions
    SELECT 
        internal_number, document_number, presctiption_no, order_canceled, document_status, warehouse_status, 
        posting_date, cust_vendor_code, total_tax, order_document_discount, order_total_discount, doc_total, 
        generation_time, sales_employee, tax_amt_sc, doc_total_sc, creation_date, user_signature, creation_time, 
        presc_spec, advanced_payment, remaining_payment, orderscreen, order_branch, presc_no, presc_print_type, 
        draft_orderno, draftorder_posting_date, draftorder_createdate, draftorder_creation_time
    FROM mabawa_mviews.v_source_orders_header_with_prescriptions;
    """

    query = pg_execute(query)
    print("source_orders_header_with_prescriptions done")

def update_source_orders_line():

    df = pd.read_sql("""
    SELECT distinct
        id, doc_internal_id, target_doc_internal_id, row_status, item_no, qty, 
        unit_price, disc_per_row, warehouse_code, gross_price_after_disc, total_tax_row, 
        flag_for_order
    FROM mabawa_staging.landing_source_orders_line;
    """, con=engine)

    df = df.set_index(['id', 'doc_internal_id', 'item_no'])

    upsert(engine=engine,
       df=df,
       schema='mabawa_staging',
       table_name='source_orders_line',
       if_row_exists='update',
       create_table=True)

    print("Updated Orders Lines")


def update_mviews_salesorders_with_item_whse():

    query = """
    truncate mabawa_mviews.salesorders_with_item_whse;
    insert into mabawa_mviews.salesorders_with_item_whse
    SELECT sales_orderno, internal_number, order_canceled, document_status, warehouse_status, 
    posting_date, order_branch, id, item_no, warehouse_code
    FROM mabawa_mviews.v_salesorders_with_item_whse;
    """
    query = pg_execute(query)

    print("Updated Table")


def create_mviews_source_orders_line_with_item_details():

    query = """
    truncate mabawa_mviews.source_orders_line_with_item_details;
    insert into mabawa_mviews.source_orders_line_with_item_details
    SELECT id, doc_internal_id, target_doc_internal_id, row_status, item_no, item_desc, 
    item_category, new_item_category, qty, unit_price, disc_per_row, warehouse_code, 
    gross_price_after_disc, total_tax_row, flag_for_order,item_brand_name, item_model_no
    FROM mabawa_mviews.v_source_orders_line_with_item_details;
    """
    query = pg_execute(query)

    print("create_mviews_source_orders_line_with_item_details done")

def create_mviews_salesorders_line_cl_and_acc():

    query = """
    truncate mabawa_mviews.salesorders_line_cl_and_acc;
    insert into mabawa_mviews.salesorders_line_cl_and_acc
    select 
        doc_internal_id, target_doc_internal_id, new_item_category
    from
    (SELECT 
    ROW_NUMBER() OVER (PARTITION BY doc_internal_id ORDER BY doc_internal_id) AS r,
    doc_internal_id, target_doc_internal_id, new_item_category
    FROM mabawa_mviews.v_salesorders_line_cl_and_acc) as a 
    where a.r=1
    """
    query = pg_execute(query)  

    print('create_mviews_salesorders_line_cl_and_acc done')

def create_order_live(): 

    query = """
    truncate mabawa_dw.fact_orders_header;
    insert into mabawa_dw.fact_orders_header
    SELECT 
            internal_number, document_number, order_canceled, document_status, warehouse_status, 
            posting_date, cust_vendor_code, total_tax, order_document_discount, order_total_discount, 
            doc_total, generation_time, sales_employee, tax_amt_sc, doc_total_sc, user_signature, 
            creation_date, creation_time_int, creation_time, creation_datetime, presc_spec, advanced_payment, 
            remaining_payment, orderscreen, order_branch, presc_no, presc_print_type, 
            draft_orderno, so.ods_posting_date::date as draftorder_posting_date,
            so.ods_createdon::date as draftorder_createdate,
            (case when length(so.ods_createdat::text)=4 then so.ods_createdat::text::time
            when length(so.ods_createdat::text)=3 then ('0'||so.ods_createdat::text)::time
            when length(so.ods_createdat::text)=2 then ('12'||so.ods_createdat::text)::time
            end) as draftorder_createtime, so.ods_ordertype as draft_order_type
    FROM mabawa_mviews.v_fact_orders_header o
    left join mabawa_staging.source_orderscreen so on o.draft_orderno::text = so.doc_no::text

    """
    query = pg_execute(query)

    query1 = """update mabawa_dw.fact_orders_header set order_canceled = 'Y' where document_number = 22220003;"""
    query1 = pg_execute(query1)

    print("create_order_live done")
    
def create_fact_orders_header_with_categories():

    query = """
    truncate mabawa_dw.fact_orders_header_with_categories;
    insert into mabawa_dw.fact_orders_header_with_categories
    SELECT 
        internal_number, document_number, order_canceled, document_status, warehouse_status, posting_date, 
        cust_vendor_code, total_tax, order_document_discount, order_total_discount, doc_total, generation_time, 
        sales_employee, tax_amt_sc, doc_total_sc, user_signature, creation_date, creation_time_int, creation_time, 
        creation_datetime, presc_spec, advanced_payment, remaining_payment, orderscreen, order_branch, presc_no, 
        presc_print_type, draft_orderno, draftorder_posting_date, draftorder_createdate, draftorder_createtime, 
        draft_order_type, order_category, use_for_viewrx
    FROM mabawa_mviews.v_fact_orders_header_with_categories;
    """
    query = pg_execute(query)

    print("create_fact_orders_header_with_categories")

# fetch_sap_orders()
# source_orders_header_with_prescriptions()
# update_source_orders_line()
# update_mviews_salesorders_with_item_whse()
# create_mviews_source_orders_line_with_item_details()
# create_mviews_salesorders_line_cl_and_acc()
# create_order_live()
# create_fact_orders_header_with_categories()