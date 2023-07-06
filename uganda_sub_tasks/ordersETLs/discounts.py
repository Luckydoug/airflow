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
from datetime import date, timedelta
import datetime


from sub_tasks.data.connect_mawingu import (pg_execute, pg_fetch_all, engine)  
from sub_tasks.api_login.api_login import(login_uganda)


SessionId = login_uganda()

# FromDate = '2023/01/01'
# ToDate = '2023/05/04'

today = date.today()
pastdate = today - timedelta(days=7)
FromDate = pastdate.strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')

print(FromDate)
print(ToDate)

pagecount_url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrderDiscountDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
pagecount_payload={}
pagecount_headers = {}


def fetch_sap_discounts():

    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    pagecount_response = pagecount_response.json()

    df = pd.DataFrame()
    payload={}
    headers = {}
    pages = pagecount_response['result']['body']['recs']['PagesCount']

    print("Pages outputted", pages)

    for i in range(1, pages+1):
        page = i
        url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrderDiscountDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()
        response = response['result']['body']['recs']['Results']
        response = pd.DataFrame.from_dict(response)
        response = response.T

        df = df.append(response,ignore_index=True)

    print('INFO! %d rows' %(len(df)))


    df.rename(columns={
        'DocEntry': 'doc_entry',
        'DocNum': 'doc_num',
        'UserSign': 'user_sign',
        'CreateDate': 'create_date',
        'CreateTime': 'create_time',
        'Creator': 'creator',
        'Doc_Date': 'doc_date',
        'Order_Status': 'order_status',
        'ApplyDiscountPer': 'apply_discount_per',
        'ApplyDiscountAmount': 'apply_discount_amount',
        'OrderRemainingAmt': 'order_remaining_amt',
        'OrderTotalAmt': 'order_total_amt',
        'Order_DocEntry': 'order_doc_entry',
        'Order_DocNum': 'order_doc_num',
        'UserCode': 'user_code',
        'Order_Customer_Code': 'order_customer_code',
        'Order_Date': 'order_date',
        'Branch': 'branch',
        'Discount_Reason': 'discount_reason',
        'Order_Discount_Category': 'order_discount_category',
        'Old_Order_No': 'old_order_no',
        'Order_RX_Tolerance': 'order_rx_tolerance'
        },inplace=True)

    print("Header Columns Renamed")

    date_cols = ['create_date','doc_date','order_date']
    df[date_cols] = df[date_cols].apply(pd.to_datetime, yearfirst=True)
    print('Date Column Types Changed')

    num_cols = ['apply_discount_per','apply_discount_amount','order_remaining_amt','order_total_amt']
    df[num_cols] = df[num_cols].apply(pd.to_numeric)
    print('Numeric Column Types Changed')


    if df.empty:
        print('INFO! Discounts dataframe is empty!')

    else:
        df = df.set_index(['doc_entry'])
        print('TRANSFORMATION! Adding new rows')

        upsert(engine=engine,
        df=df,
        schema='mawingu_staging',
        table_name='source_discounts',
        if_row_exists='update',
        create_table=False)

        print('Update successful')
