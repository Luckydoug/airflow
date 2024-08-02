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

# FromDate = '2022/01/01'
# # ToDate = '2022/04/30'

today = date.today()
pastdate = today - timedelta(days=7)
FromDate = pastdate.strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')

def fetch_sap_inventory_transfer():
    SessionId = return_session_id(country="Uganda")
    #SessionId = login_uganda()

    pagecount_url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetInventoryTransferDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    pagecount_payload={}
    pagecount_headers = {}

    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    
    transfer_df = pd.DataFrame()
    payload={}
    headers = {}
    pages = data['result']['body']['recs']['PagesCount']

    print("Retrived No of Pages")

    for i in range(1, pages+1):
        page = i
        url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetInventoryTransferDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()

        try:
            response = response['result']['body']['recs']['Results']
            response = pd.DataFrame.from_dict(response)
            transfer_df = transfer_df.append(response, ignore_index=True)
        except:
            print('error')
        
    

    """
    CREATING DETAILS TABLE
    """
    transfer = transfer_df['details'].apply(pd.Series)
    print(transfer.columns)

    transfer.rename (columns = {
            'Internal_Number':'internal_no',
            'Document_Number':'doc_no',
            'Posting_Date':'post_date',
            'Remarks':'remarks',
            'Generation_Time':'generation_time',
            'Creation_Date':'createdon',
            'Creatn_Time_Incl_Secs':'creation_time_incl_secs',
            'PickRemarks':'pick_remarks',
            'ExchangeType':'exchange_type',
            'DamagedReason':'damaged_reason'}
        ,inplace=True)

    transfer = transfer.set_index('internal_no')
    print(transfer)

    upsert(engine=engine,
        df=transfer,
        schema='mawingu_staging',
        table_name='source_it',
        if_row_exists='update',
        create_table=False)

    print("Rows Inserted")

    """
    CREATING ITEM DETAILS TABLE
    """
    transfer_itemdetails = transfer_df['itemdetails']
    transfer_itemdetails = transfer_itemdetails.to_frame()

    itemdetails_df = pd.DataFrame()
    for index, row in transfer_itemdetails.iterrows():
        row_data=row['itemdetails']
        data = pd.DataFrame.from_dict(row_data)
        data1 = data.T
        itemdetails_df = itemdetails_df.append(data1, ignore_index=True)
        print(itemdetails_df.columns)

    print("Created Item Details DF")

    itemdetails_df.rename (columns = {
            'Document_Internal_ID':'doc_internal_id',
            'RowNumber':'row_no',
            'Base_Document_Reference':'base_doc_ref',
            'Base_Document_Internal_ID':'base_doc_internal_id',
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
            'Replaced_ItemCode':'replaced_item_code',
            'Picker_Name':'picker_name'}
        ,inplace=True)

    itemdetails_df = itemdetails_df.set_index(['doc_internal_id','row_no'])
    print(itemdetails_df)

    upsert(engine=engine,
        df=itemdetails_df,
        schema='mawingu_staging',
        table_name='source_it_details',
        if_row_exists='update',
        create_table=False)

    return "IT Complete"

# fetch_sap_inventory_transfer()