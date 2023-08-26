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

FromDate = '2023/04/01'
ToDate = '2023/06/30'

# today = date.today()
# pastdate = today - timedelta(days=1)
# FromDate = pastdate.strftime('%Y/%m/%d')
# ToDate = date.today().strftime('%Y/%m/%d')

print(FromDate)
print(ToDate)

# api details
#orderscreen_url = 'https://41.72.211.10:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderDetails&pageNo=1'
pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetNewBi&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
pagecount_payload={}
pagecount_headers = {}

    
def fetch_stock_movement():
    
    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    pagecount_response = pagecount_response.json()
    
    paymentsdf = pd.DataFrame()
    payload={}
    headers = {}
    pages = pagecount_response['result']['body']['recs']['PagesCount']


    print("Pages outputted", pages)

    for i in range(1, pages+1):
        page = i
        print(page)
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetNewBi&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()
        response = response['result']['body']['recs']['Results']
        response = pd.DataFrame.from_dict(response)
        response = response.T
        paymentsdf = paymentsdf.append(response, ignore_index=True)
    
    print('INFO! %d rows' %(len(paymentsdf)))

    paymentsdf.rename (columns = {
                                "TransNum": "trans_num",
                                "CreatedBy": "created_by",
                                "BASE_REF": "base_ref",
                                "DocDate": "doc_date",
                                "CardCode": "card_code",
                                "Ref1": "ref1",
                                "Ref2": "ref2",
                                "DocTime": "doc_time",
                                "ItemCode": "item_code",
                                "Dscription": "description",
                                "InQty": "in_qty",
                                "OutQty": "out_qty",
                                "Warehouse": "warehouse",
                                "CreateDate": "create_date"
                                }
            ,inplace=True)


    #query = """truncate mabawa_staging.landing_orderscreenc1;"""
    #query = pg_execute(query)

    # payments.to_sql('source_payment_means', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)

    if paymentsdf.empty:
        print('INFO! Payments dataframe is empty!')
    else:
        paymentsdf = paymentsdf.drop_duplicates(subset='trans_num').set_index(['trans_num'])
        print('TRANSFORMATION! Adding new rows')

        upsert(engine=engine,
        df=paymentsdf,
        schema='mabawa_staging',
        table_name='source_stock_movement',
        if_row_exists='update',
        create_table=True)

        print('Update successful')
        print('payments have been fetched successfully')



fetch_stock_movement()


