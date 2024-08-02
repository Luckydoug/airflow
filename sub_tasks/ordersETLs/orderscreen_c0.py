import sys
sys.path.append(".")
import psycopg2
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
import datetime
import businesstimedelta
import pandas as pd
import holidays as pyholidays
from workalendar.africa import Kenya
from sub_tasks.data.connect import (pg_execute, engine) 
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate

print(FromDate,ToDate)


def fetch_orderscreen_c0():
    SessionId = return_session_id(country = "Kenya")
    pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderDetailsC2&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    pagecount_payload={}
    pagecount_headers = {}
    
    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    
    orderscreenc0 = pd.DataFrame()
    payload={}
    headers = {}
    pages = data['result']['body']['recs']['PagesCount']
    for i in range(1, pages+1):
        page = i
        print(page)
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderDetailsC2&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()
        response = response['result']['body']['recs']['Results']
        response = pd.DataFrame.from_dict(response)
        response = response.T
        orderscreenc0 = orderscreenc0.append(response, ignore_index=True)    


    orderscreenc0 = orderscreenc0.rename(columns = {'DocEntry':'doc_entry', 
                                                        'ItemType':'item_type', 
                                                        'ItemCode':'item_code', 
                                                        'Eye':'eye', 
                                                        'Quantity':'quantity',
                                                        'Price':'price', 
                                                        'MRP':'mrp',
                                                        'Warehouse':'warehouse', 
                                                        'TaxAmount':'tax_amount', 
                                                        'Glazing':'glazing', 
                                                        'Glazingbranch':'glazing_branch'
                                                        })

    query = """truncate mabawa_staging.source_orderscreenc0;"""
    query = pg_execute(query)
    orderscreenc0.to_sql('source_orderscreenc0', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    
    print('source_orderscreenc0')

# fetch_orderscreen_c0()