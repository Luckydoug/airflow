import sys
sys.path.append(".")
import psycopg2
import requests
import pandas as pd
from airflow.models import Variable
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate


def fetch_orderscreen_co():
    SessionId = return_session_id(country = "Kenya")
    pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetPaymentMeans&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    page_count_payload = {}
    page_count_headers = {}

    pagecount_response = requests.request(
        "GET", 
        pagecount_url, 
        headers=page_count_headers,
        data=page_count_payload,
        verify=False
    )
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