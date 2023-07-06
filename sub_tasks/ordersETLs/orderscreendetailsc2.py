import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pandas as pd
from airflow.models import Variable
from datetime import date, timedelta
from pangres import upsert, fix_psycopg2_bad_cols, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 


from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine) 
from sub_tasks.api_login.api_login import(login)


SessionId = login()

# FromDate = '2023/03/20'
# ToDate = '2023/03/20'

today = date.today()
pastdate = today - timedelta(days=1)
FromDate = pastdate.strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')

print(FromDate)
print(ToDate)

# api details
#orderscreen_url = 'https://41.72.211.10:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderDetails&pageNo=1'
pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderDetailsC2&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
pagecount_payload={}
pagecount_headers = {}


def fetch_sap_orderscreendetailsc2 ():

    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    
    orderscreendf = pd.DataFrame()
    payload={}
    headers = {}
    pages = data['result']['body']['recs']['PagesCount']
    print("Retrieved Pages", pages)
    for i in range(1, pages+1):
        page = i
        print(i)
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderDetailsC2&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()
        response = response['result']['body']['recs']['Results']
        orderscreendf = pd.DataFrame(response)
        orderscreendf = orderscreendf.T
        orderscreendf = orderscreendf.append(orderscreendf, ignore_index=False)
        # response = nested_to_record(response, sep='_')
        # response= response['result_body_recs_Results']
        # response = pd.DataFrame.from_dict(response)
        # response = pd.json_normalize(response['details'])
        # orderscreendf = orderscreendf.append(response, ignore_index=True)
        print(orderscreendf[orderscreendf.duplicated(keep=False).any()==True])
        break

# fetch_sap_orderscreendetailsc2 ()