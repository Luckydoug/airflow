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
import holidays as pyholidays
from airflow.models import Variable
from datetime import date, timedelta
from sqlalchemy import create_engine
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 


from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.api_login.api_login import(login)

SessionId = login()

FromDate = '2022/08/01'
ToDate = '2022/08/16'


# today = date.today()
# pastdate = today - timedelta(days=2)
# FromDate = pastdate.strftime('%Y/%m/%d')
# ToDate = date.today().strftime('%Y/%m/%d')

# api details
pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetAjuaInformation&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
pagecount_payload={}
pagecount_headers = {}

# fetch order checking details
def fetch_ajua_info ():
    
    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    pages = data['result']['body']['recs']['PagesCount']

    print(pages)

    print("Retrived Number of Pages")

    headers = {}
    payload = {}

    ajua_details = pd.DataFrame()
    ajua_itemdetails = pd.DataFrame()
    for i in range(1, pages+1):
        page = i
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetAjuaInformation&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()

        try:
            response = response['result']['body']['recs']['Results']
            details = pd.DataFrame([d['details'] for d in response])
            itemdetails = pd.DataFrame([{"id": k, **v} for d in response for k, v in d['itemdetails'].items()])
            ajua_details = ajua_details.append(details, ignore_index=True)
            ajua_itemdetails = ajua_itemdetails.append(itemdetails, ignore_index=True)
            
        except:
            print('Error')

    print("Finished API calls")

    '''
    AJUA HEADERS
    '''
    ajua_details.rename (columns = {
            'DocEntry':'doc_entry',
            'DocNum':'doc_no',
            'UserSign': 'user_sign',
            'CreateDate': 'create_date',
            'CreateTime': 'create_time',
            'UpdateDate': 'update_date',
            'UpdateTime': 'update_time',
            'Creator': 'creator'
            }
        ,inplace=True)

    print("Columns Renamed")

    #ajua_details['create_date'] = pd.to_datetime(ajua_details.create_date).dt.date
    #ajua_details['update_date'] = pd.to_datetime(ajua_details.update_date).dt.date

    ajua_details = ajua_details.set_index(['doc_entry'])

    upsert(engine=engine,
       df=ajua_details,
       schema='mabawa_staging',
       table_name='source_ajua_info_header',
       if_row_exists='update',
       create_table=True)

    print("Inserted Orders Header")

    return ""

    