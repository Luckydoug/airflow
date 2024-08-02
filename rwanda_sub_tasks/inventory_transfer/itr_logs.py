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
import holidays as pyholidays
from airflow.models import Variable
from datetime import date, timedelta
from workalendar.africa import Kenya
from sqlalchemy import create_engine
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 

from sub_tasks.data.connect_voler import (pg_execute, pg_fetch_all, engine)  
# from sub_tasks.api_login.api_login import(login_rwanda)
from sub_tasks.libraries.utils import return_session_id

# FromDate = '2023/05/01'
# ToDate = '2023/11/30'

today = date.today()
pastdate = today - timedelta(days=5)
FromDate = pastdate.strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')


def fetch_sap_itr_logs ():
    SessionId = return_session_id(country = "Rwanda")
    #SessionId = login_rwanda()

    pagecount_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetITRLOGDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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
        url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetITRLOGDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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
        schema='voler_staging',
        table_name='source_itr_log',
        if_row_exists='update',
        create_table=False)

    print("Upsert Complete")

    return "Fetched ITR Logs"

# fetch_sap_itr_logs()

