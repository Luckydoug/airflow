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

# FromDate = '2023/04/01'
# ToDate = '2023/05/30'

today = date.today()
pastdate = today - timedelta(days=1)
FromDate = pastdate.strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')


# api details
pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetBussinesPartners&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
pagecount_payload={}
pagecount_headers = {}

def fetch_sap_customers():
    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    pages = data['result']['body']['recs']['PagesCount']

    print("Pages outputted", pages)

    customersdf = pd.DataFrame()
    payload={}
    headers = {}
    for i in range(1, pages+1):
        page = i
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetBussinesPartners&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        customers = response.json()
        stripped_customers = customers['result']['body']['recs']['Results']
        customers_df = pd.DataFrame.from_dict(stripped_customers)
        customers_df2 = customers_df.T
        customersdf = customersdf.append(customers_df2, ignore_index=True) 


    customersdf.rename (columns = {'BP_Code':'cust_code', 
                       'BP_Type':'bp_type', 
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
                       'Promotional_SMS':'cust_promo_sms',
                       'U_VSPCONRS':'conversion_reason',
                       'U_VSPCONRE':'conversion_remark'}
            ,inplace=True)
    
    #query = """drop table mabawa_staging.source_customers;"""
    #query = pg_execute(query)
    print('INFO! %d rows' %(len(customersdf)))
    # customersdf.to_sql('source_customers', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)

    # table_name = 'source_customers'
    # schema = 'mabawa_staging'

    # print("started insert")

    # pg_bulk_insert(customers_df2, table_name, schema)
    customersdf = customersdf.drop_duplicates('cust_code')

    if customersdf.empty:
        print('INFO! customers dataframe is empty!')
    else:
        customersdf = customersdf.set_index(['cust_code'])
        print('INFO! Customer upsert started...')

        upsert(engine=engine,
        df=customersdf,
        schema='mabawa_staging',
        table_name='source_customers',
        if_row_exists='update',
        create_table=False)

        print('Update successful')

def create_dim_customers():

    query = """
    truncate mabawa_dw.dim_customers;
    insert into mabawa_dw.dim_customers
    select cust_code, bp_type, cust_groupcode, cust_sales_employeecode, cust_bill_to_city, 
    cust_bill_to_country, cust_createdon, cust_createdat, cust_created_time, cust_active_status, 
    cust_dob, cust_age, cust_gender, cust_country, cust_invoiceno, cust_entering_reason, cust_type, 
    cust_insurance_company, cust_insurance_scheme, cust_campaign_master, cust_reg_amt, cust_payment_status, 
    cust_outlet, cust_old_new_bp, cust_loyalty_optin, cust_promo_sms,conversion_reason,conversion_remark
    from mabawa_dw.v_dim_customers;
    """

    query = pg_execute(query)
def create_reg_conv():
    
    query = """
    truncate mabawa_mviews.reg_conv;
    insert into mabawa_mviews.reg_conv
    select cust_code, cust_createdon, cust_outlet, cust_sales_employeecode, cust_type, 
    cust_old_new_bp, cust_age, age_group, draft_orderno, creation_date, o_days, code, 
    create_date, t_days, days
    from mabawa_mviews.v_reg_conv;
    insert into mabawa_dw.update_log(table_name, update_time) values('reg_conv', default);
    """

    query = pg_execute(query)


# fetch_sap_customers()
# create_dim_customers()