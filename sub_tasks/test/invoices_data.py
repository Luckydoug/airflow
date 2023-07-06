# import sys

# from numpy import nan
# sys.path.append(".")

# #import libraries
# import json
# import psycopg2
# import requests
# import datetime
# import pandas as pd
# from io import StringIO
# from datetime import date, timedelta
# import holidays as pyholidays
# from airflow.models import Variable
# from sqlalchemy import create_engine
# from pangres import upsert, DocsExampleTable
# from sqlalchemy import create_engine, text, VARCHAR
# from pandas.io.json._normalize import nested_to_record 


# from sub_tasks.data.connect import (pg_execute, engine) 
# from sub_tasks.api_login.api_login import(login)

# SessionId = login()

# FromDate = '2022/01/01'
# ToDate = '2022/01/05'


# # today = date.today()
# # pastdate = today - timedelta(days=2)
# # FromDate = pastdate.strftime('%Y/%m/%d')
# # ToDate = date.today().strftime('%Y/%m/%d')

# # api details

# pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetInvoiceDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
# pagecount_payload={}
# pagecount_headers = {}

# def fetch_invoices ():
    
#     pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
#     data = pagecount_response.json()
#     pages = data['result']['body']['recs']['PagesCount']
#     print(pages)

#     print("Retrived No of Pages")
#     headers = {}
#     payload = {}

#     invoices = pd.DataFrame()
#     invoices_details = pd.DataFrame()

#     for i in range(1, pages+1):
#         page = i
#         url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetInvoiceDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
#         response = requests.request("GET", url, headers=headers, data=payload, verify=False)
#         response = response.json()

#         try:
#             response = response['result']['body']['recs']['Results']
#             invoicesdf = pd.DataFrame([d['details'] for d in response])
#             invoices = invoices.append(invoicesdf, ignore_index=True)

#             invoicesdetailsdf = pd.DataFrame([{"id": k, **v} for d in response for k, v in d['itemdetails'].items()])
#             invoices_details = invoices_details.append(invoicesdetailsdf, ignore_index=True)
#         except:
#             print('error')

#     print((invoices))
#     print(invoices_details)

#     invoices.to_sql('invoices', con=engine, index= False, if_exists='append', schema='etl_dev')
#     invoices_details.to_sql('invoices_details', con=engine, index= False, if_exists='append', schema='etl_dev')

#     return ""

    