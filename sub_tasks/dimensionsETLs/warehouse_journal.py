import sys
sys.path.append(".")
import requests
import pandas as pd
from sub_tasks.data.connect import  engine, pg_execute
# from sub_tasks.api_login.api_login import(login)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate

def fetch_opening_dates():
    SessionId = return_session_id(country = "Kenya")
    #SessionId = login()

    pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetWarehouseJournal&pageNo=1&SessionId={SessionId}&FromDate={FromDate}&ToDate={ToDate}"
    pagecount_payload={}
    pagecount_headers = {}

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

        query = """truncate table mabawa_staging.source_branch_opening_date;"""
        query = pg_execute(query)
        openingdatedf.to_sql('source_branch_opening_date', con=engine, schema='mabawa_staging', if_exists = 'append', index=False)

