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

# FromDate = '2023/04/01'
# ToDate = '2023/05/04'

today = date.today()
pastdate = today - timedelta(days=7)
FromDate = pastdate.strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')

print(FromDate)
print(ToDate)

# api details

pagecount_url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetOptomQManagment&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
pagecount_payload={}
pagecount_headers = {}

# fetch order checking details
def fetch_optom_queue_mgmt():
    
    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    pages = data['result']['body']['recs']['PagesCount']

    print(pages)

    print("Retrived Number of Pages")

    headers = {}
    payload = {}

    optom_queue = pd.DataFrame()
    for i in range(1, pages+1):
        page = i
        url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetOptomQManagment&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()

        try:
            response = response['result']['body']['recs']['Results']
            optomdf= pd.DataFrame(response)
            optomdf = optomdf.T
            optom_queue = optom_queue.append(optomdf, ignore_index=False)
            
        except:
            print('Error')

    # rename columns
    optom_queue.rename(columns={
                        "DocEntry": "doc_entry",
                        "DocNum": "doc_no",
                        "UserSign": "user_sign",
                        "CreateDate": "create_date",
                        "CreateTime": "create_time",
                        "UpdateDate": "update_date",
                        "UpdateTime": "update_time",
                        "Creator": "creator",
                        "OutLetID": "outlet_id",
                        "CustomerID": "cust_id",
                        "OptumID": "optom_id",
                        "Status": "status",
                        "QueueType": "queue_type",
                        "VisitType": "visit_type",
                        "VisitId": "visit_id",
                        "ActivityNo": "activity_no"
                        },
                        inplace=True)
    print("Columns Renamed")

    

    # transformation
    optom_queue = optom_queue.set_index('doc_entry')
    
    # df to db
    upsert(engine=engine,
       df=optom_queue,
       schema='mawingu_staging',
       table_name='source_optom_queue_mgmt',
       if_row_exists='update',
       create_table=False)

    print('Update Successful')

