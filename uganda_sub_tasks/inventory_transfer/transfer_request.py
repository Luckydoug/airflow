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
import businesstimedelta
from datetime import date, timedelta
import holidays as pyholidays
from airflow.models import Variable
from workalendar.africa import Kenya
from sqlalchemy import create_engine
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 

from sub_tasks.data.connect_mawingu import (pg_execute, pg_fetch_all, engine, pg_bulk_insert) 
# from sub_tasks.api_login.api_login import(login_uganda)
from sub_tasks.libraries.utils import return_session_id

# FromDate = '2023/12/13'
# ToDate = '2022/12/13'

today = date.today()
pastdate = today - timedelta(days = 7)
# pastdate = today
FromDate = pastdate.strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')

def fetch_sap_invt_transfer_request ():
    SessionId = return_session_id(country="Uganda")
   

    pagecount_url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetITRDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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
        page = i
        url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetITRDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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
            'ToWhsCode':'to_warehouse_code',
            'Filler':'filler'}
        ,inplace=True)

    print("Renamed Columns")

    transfer_request_details = transfer_request_details.set_index('internal_no')

    print("Indexes set")

    
    upsert(engine=engine,
        df=transfer_request_details,
        schema='mawingu_staging',
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
            'Picker_Name':'picker_name'
            }
        ,inplace=True)

    print("Renamed Detailed Columns")

    itemdetails_df = itemdetails_df.set_index(['doc_internal_id','row_no'])
    print(itemdetails_df)

    upsert(engine=engine,
        df=itemdetails_df,
        schema='mawingu_staging',
        table_name='source_itr_details',
        if_row_exists='update',
        create_table=False)

    print("Rows Inserted")   
    return 'something' 

# fetch_sap_invt_transfer_request ()