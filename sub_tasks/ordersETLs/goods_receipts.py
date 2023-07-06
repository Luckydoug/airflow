import sys

from numpy import nan
sys.path.append(".")

#import libraries
from io import StringIO
import json
import datetime
import requests
import psycopg2
import pandas as pd
import businesstimedelta
from datetime import date, timedelta
import holidays as pyholidays
from workalendar.africa import Kenya
from airflow.models import Variable
from sqlalchemy import create_engine
from pangres import upsert, DocsExampleTable
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 

from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.api_login.api_login import(login)
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")

SessionId = login()

# FromDate = '2023/03/06'
# ToDate = '2023/03/14'

today = date.today()
pastdate = today - timedelta(days=1)
FromDate = pastdate.strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')

pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetGoodsReceiptsDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
pagecount_headers = {}
pagecount_payload = {}

def fetch_goods_receipt():
    
    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    
    goods_receipt = pd.DataFrame()
    payload={}
    headers = {}
    pages = data['result']['body']['recs']['PagesCount']
    for i in range(1, pages+1):
        page = i
        print('paginating')
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetGoodsReceiptsDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"

        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()
        response = response['result']['body']['recs']['Results']
        response = pd.DataFrame.from_dict(response)
        goods_receipt = goods_receipt.append(response, ignore_index=True)
        # print(goods_receipt)
    

    goods_receipt = goods_receipt['details'].apply(pd.Series)
    print(goods_receipt)

    goods_receipt.rename (columns = {'DocEntry':'doc_entry', 
                       'Document_Number':'doc_no', 
                       'Document_Status':'doc_status', 
                       'Posting_Date':'post_date', 
                       'Due_Date':'due_date', 
                       'Customer_Vendor_Code':'cust_value_code', 
                       'BP_Reference_No':'bp_ref_no', 
                       'Discount_for_Document':'disc_for_doc', 
                       'Total_Discount':'total_discount',
                       'Document_Currency':'doc_currency',
                       'Document_Rate':'doc_rate',
                       'Reference_1':'reference_1',
                       'Transaction_Number':'transaction_no',
                       'Generation_Time':'generation_time',
                       'Creation_Date':'creation_date',
                       'User_Signature':'user_signature',
                       'Landed_Cost_Number':'landed_cost_no',
                       'Number_of_Additional_Days':'no_of_add_days',
                       'Creatn_Time_Incl_Secs':'creation_time_incl_secs',
                       'Order_Screen':'order_screen',
                       'Sales_Order':'sales_order',
                       'Order_Entry':'order_entry',
                       'Final_ExchangeRate':'final_exchangerate'}
            ,inplace=True)

    goods_receipt = goods_receipt.set_index('doc_entry')

    upsert(engine=engine,
       df=goods_receipt,
       schema='mabawa_staging',
       table_name='source_goods_receipt',
       if_row_exists='update',
       create_table=True)


def goods_receipt_live():
    
    query = """
    truncate mabawa_dw.dim_goods_receipt;
    insert into mabawa_dw.dim_goods_receipt
    SELECT 
            doc_entry, doc_no, doc_status, post_date, due_date, cust_value_code, 
            bp_ref_no,
            regexp_replace(dhl_invoice, '.*?\s','') as dhl_invoice, 
            disc_for_doc, total_discount, doc_currency, 
            doc_rate, reference_1, transaction_no, generation_time, creation_date, 
            user_signature, landed_cost_no, no_of_add_days, creation_time_incl_secs, 
            order_screen, sales_order, order_entry, final_exchangerate
    FROM mabawa_staging.v_goods_receipt;
    """

    query = pg_execute(query)

