import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pandas as pd
from pandas.io.json._normalize import nested_to_record 
from sqlalchemy import create_engine
from airflow.models import Variable
from pandas.io.json._normalize import nested_to_record 

from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine) 
from sub_tasks.api_login.api_login import(login)

# LOCAL_DIR = "/tmp/"
#location = Variable.get("LOCAL_DIR", deserialize_json=True)
#dimensionstore = location["dimensionstore"]

# get session id
SessionId = login()
FromDate = '2021/10/31'
ToDate = '2021/10/31'

# api details
pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetItemDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"

pagecount_payload={}
pagecount_headers = {}


#def fetch_sap_items():
    
pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
data = pagecount_response.json()

itemsdf = pd.DataFrame()
payload={}
headers = {}
pages = data['result']['body']['recs']['PagesCount']
for i in range(1, pages+1):
    page = i
    url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetItemDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    response = requests.request("GET", url, headers=headers, data=payload, verify=False)
    items = response.json()
    stripped_items = items['result']['body']['recs']['Results']
    items_df = pd.DataFrame.from_dict(stripped_items)
    items_df2 = items_df.T
    itemsdf = itemsdf.append(items_df2, ignore_index=True)

items_df2

'''
response = requests.get(items_url,  verify=False)
items = response.json()
stripped_items = items['result']['body']['recs']['Results']
items_df = pd.DataFrame.from_dict(stripped_items)

items_df2 = items_df.T


items_df2.rename (columns = {'BP_Code':'cust_code', 
                    'BP_Type':'cust_type', 
                    'Group_Code':'cust_groupcode', 
                    'Sales_Employee_Code':'cust_sales_employeecode', 
                    'Bill_to_City':'cust_bill_to_city', 
                    'Bill_to_Country':'cust_bill_to_country', 
                    'Creation_Date':'cust_createdon', 
                    'Active':'cust_active_status', 
                    'Creatn_Time_Incl_Secs':'cust_createdat', 
                    'Date_of_Birth':'cust_dob', 
                    'Gender':'cust_gender', 
                    'Country':'cust_country', 
                    'Invoice_No':'cust_invoiceno', 
                    'Entering_reason':'cust_entering_reason', 
                    'Customer_Type':'cust_type', 
                    'Insurance_Company_Name':'cust_insurance_company', 
                    'Insurance_Scheme':'cust_insurance_scheme', 
                    'Campaign_Master':'cust_campaign_master', 
                    'Registration_Amount':'cust_reg_amt', 
                    'Payment_Status':'cust_payment_status', 
                    'Outlet':'cust_outlet', 
                    'Old_New_BusnsPartnr':'cust_old_new_bp', 
                    'Loyalty_OPT_IN':'cust_loyalty_optin', 
                    'Promotional_SMS':'cust_promo_sms'}
        ,inplace=True)

#query = """drop table mabawa_staging.source_items;"""
#query = pg_execute(query)
items_df2.to_sql('source_items', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
return "process done"  
'''
