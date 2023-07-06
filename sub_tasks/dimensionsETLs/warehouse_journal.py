import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pandas as pd
from datetime import date, timedelta

from pandas.io.json._normalize import nested_to_record 
from sqlalchemy import create_engine
from airflow.models import Variable
from pandas.io.json._normalize import nested_to_record 
from pangres import upsert, DocsExampleTable

from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine, pg_bulk_insert) 
from sub_tasks.api_login.api_login import(login)

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# LOCAL_DIR = "/tmp/"
#location = Variable.get("LOCAL_DIR", deserialize_json=True)
#dimensionstore = location["dimensionstore"]

# get session id
SessionId = login()

FromDate = '2019/01/01'
ToDate = '2020/12/31'

# today = date.today()
# pastdate = today - timedelta(days=1)
# FromDate = pastdate.strftime('%Y/%m/%d')
# ToDate = date.today().strftime('%Y/%m/%d')


# api details
pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetWarehouseJournal&pageNo=1&SessionId={SessionId}&FromDate={FromDate}&ToDate={ToDate}"
pagecount_payload={}
pagecount_headers = {}

def fetch_opening_dates():
    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    pages = data['result']['body']['recs']['PagesCount']

    print("Pages outputted", pages)

    openingdate_df = pd.DataFrame()
    payload={}
    headers = {}
    for i in range(1, pages+1):
        page = i
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetWarehouseJournal&pageNo={page}&SessionId={SessionId}&FromDate={FromDate}&ToDate={ToDate}"        
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        journals = response.json()
        openingdate_df = journals['result']['body']['recs']['Results']
        openingdate_df1 = pd.DataFrame.from_dict(openingdate_df)
        openingdate_df = openingdate_df1.T
        openingdatedf = openingdate_df.append(openingdate_df, ignore_index=True) 
        openingdatedf = openingdatedf.drop_duplicates(subset = 'Transaction Type',keep ='first')
        print(openingdatedf.columns)

        query = """truncate table mabawa_staging.source_branch_opening_date;"""
        query = pg_execute(query)
        openingdatedf.to_sql('source_branch_opening_date', con=engine, schema='mabawa_staging', if_exists = 'append', index=False)

    # if openingdatedf.empty:
    #     print('INFO! dataframe is empty!')
    # else:
    #     openingdatedf = openingdatedf.set_index(['Transaction Type'])
    #     print('INFO! df upsert started...')

    #     upsert(engine=engine,
    #     df=openingdatedf,
    #     schema='mabawa_staging',
    #     table_name='source_branch_opening_date',
    #     if_row_exists='update',
    #     create_table=False)

    #     print('Update successful')
        
# fetch_opening_dates()
