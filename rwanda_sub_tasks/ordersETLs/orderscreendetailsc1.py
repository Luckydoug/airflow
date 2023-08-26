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


from sub_tasks.data.connect_voler import (pg_execute, pg_fetch_all, engine)  
from sub_tasks.api_login.api_login import(login_rwanda)


SessionId = login_rwanda()

FromDate = '2023/05/01'
# ToDate = '2023/05/18'

today = date.today()
# pastdate = today - timedelta(days=3)
# FromDate = pastdate.strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')

pagecount_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrderDetailsC1&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"

pagecount_payload={}
pagecount_headers = {}

def fetch_sap_orderscreendetailsc1():
    
    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    
    orderscreenc1 = pd.DataFrame()
    payload={}
    headers = {}
    pages = data['result']['body']['recs']['PagesCount']
    print(pages)
    for i in range(1, pages+1):
        page = i
        print(page)
        url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrderDetailsC1&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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

    print('Renamed successfully')

    query = """truncate voler_staging.landing_orderscreenc1;"""
    query = pg_execute(query)

    orderscreenc1.to_sql('landing_orderscreenc1', con = engine, schema='voler_staging', if_exists = 'append', index=False)

# fetch_sap_orderscreendetailsc1()

def update_to_source_orderscreenc1():

    data = pd.read_sql("""
    SELECT distinct
    doc_entry, odsc_lineid, odsc_date, odsc_time, odsc_status, odsc_doc_no, odsc_createdby, odsc_document, odsc_remarks
    FROM voler_staging.landing_orderscreenc1;
    """, con=engine)

    data = data.set_index(['doc_entry','odsc_lineid'])

    upsert(engine=engine,
       df=data,
       schema='voler_staging',
       table_name='source_orderscreenc1',
       if_row_exists='update',
       create_table=False)
    
    print('source_orderscreenc1')

# update_to_source_orderscreenc1()