import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import datetime
import pandas as pd
from datetime import date
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine
from airflow.models import Variable
from pandas.io.json._normalize import nested_to_record 


from sub_tasks.data.connect_mawingu import (pg_execute, pg_fetch_all, engine, pg_bulk_insert) 
from sub_tasks.api_login.api_login import(login_uganda)


# get session id
SessionId = login_uganda()
FromDate = '2018/01/01'
# ToDate = date.today().strftime('%Y/%m/%d')

# FromDate = date.today().strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')

# api details
pagecount_url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetBranchTargetCalculation&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
pagecount_payload={}
pagecount_headers = {}

def fetch_ajua_info ():
    #get number of pages
    page_url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetAjuaInformation&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    payload = {}
    headers = {}

    page_data = requests.request("GET", page_url, headers=headers, data=payload, verify=False)
    page_data = page_data.json()
    page_data = page_data['result']['body']['recs']['PagesCount']   
    print("Number of pages is", page_data)

    #get data
    ajua_info = pd.DataFrame()
    payload = {}
    headers = {}

    for i in range (1, page_data+1):
        page = i
        data_url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetAjuaInformation&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}" 
        try:
            response = requests.request("GET", data_url, headers=headers, data=payload, verify=False)
            response = response.json()
            response = response['result']['body']['recs']['Results']
            responsedf = pd.DataFrame(response)
            responsedf = responsedf.T
            ajua_info = ajua_info.append(responsedf, ignore_index=True)
        except:
            ("Error fetching data!")

    print("Data fetching completed!")

    #rename df columns
    ajua_info.rename (columns = {'DocEntry':'doc_entry', 
                             'LineId':'line_id',
                             'Type':"type",
                             'SAP_Internal_Number':'sap_internal_number',
                             'Branch':'branch',
                             'Survey_Id':'survey_id',
                             'Ajua_Triggered':'ajua_triggered',
                             'Ajua_Response':'ajua_response',
                             'NPS_Rating':'nps_rating',
                             'NPS_Rating_2':'nps_rating_2',
                             'FeedBack':'feedback',
                             'Feedback':'feedback2', 
                             'Triggered_Time':'triggered_time',
                             'Trigger_Date':'trigger_date', 
                             'Long_Feedback':'long_feedback'} 
                    ,inplace=True)
    print("Columns renamed!")
    print(ajua_info)

    #set index
    ajua_info = ajua_info.set_index('line_id')

    upsert(engine=engine,
    df=ajua_info,
    schema='mawingu_staging',
    table_name='source_ajua_info',
    if_row_exists='update',
    create_table=True)

    print("Data Inserted")

# fetch_ajua_info()