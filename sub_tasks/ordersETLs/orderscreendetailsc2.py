import sys
sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from datetime import date, timedelta
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate


def fetch_sap_orderscreendetailsc2():

    # SessionId = login()
    SessionId = return_session_id(country = "Kenya")
    pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderDetailsC2&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    pagecount_payload={}
    pagecount_headers = {}

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
