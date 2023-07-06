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


from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.api_login.api_login import(login)
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SessionId = login()

today = date.today()
pastdate = today - timedelta(days=1)
FromDate = pastdate.strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')

pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetPaymentMeans&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
page_count_payload = {}
page_count_headers = {}

def fetch_orderscreen_co():
    pagecount_response = requests.request("GET", 
                                          pagecount_url, 
                                          headers=page_count_headers,
                                          data=page_count_payload,
                                          verify=False)
    orderscreen_c2 = pd.DataFrame()
    payload = {}
    headers = {}
    pages = pagecount_response['result']['body']['recs']['PagesCount']


    for i in range(1, pages):
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetPaymentMeans&pageNo={i}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        json_data = response.json()
        orderscreen_data = json_data['result']['body']['recs']['Results']
        orderscreen_dataframe = pd.DataFrame.from_dict(orderscreen_data)

    orderscreen_dataframe.rename(columns={
        ""
    })