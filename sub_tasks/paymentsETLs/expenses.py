import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
from datetime import date, timedelta
import requests
import pandas as pd
from pandas.io.json._normalize import nested_to_record 
from sqlalchemy import create_engine
from airflow.models import Variable
from pangres import upsert, DocsExampleTable

from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine, pg_bulk_insert) 
from sub_tasks.api_login.api_login import(login)

# LOCAL_DIR = "/tmp/"
#location = Variable.get("LOCAL_DIR", deserialize_json=True)
#dimensionstore = location["dimensionstore"]

# get session id
SessionId = login()

# FromDate = '2023/01/11'
# ToDate = '2023/02/05'

today = date.today()
pastdate = today - timedelta(days=1)
FromDate = pastdate.strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')

# api details


pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOptomDailyExpenses&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
pagecount_payload={}
pagecount_headers = {}

def fetch_sap_expenses():

    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    pages = data['result']['body']['recs']['PagesCount']

    print("Pages outputted", pages)

    expensesdf = pd.DataFrame()
    payload={}
    headers = {}
    
    for i in range(1, pages+1):
        page = i
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOptomDailyExpenses&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        expenses = response.json()
        stripped_expenses = expenses['result']['body']['recs']['Results']
        expenses_df = pd.DataFrame.from_dict(stripped_expenses)
        expenses_df2 = expenses_df.T
        expensesdf = expensesdf.append(expenses_df2, ignore_index=True) 
    
    # condition to add new column if optom or sales person
    # usersdf.loc[usersdf['Department'] == 1, 'user_department_name'] = 'Sales Person' 
    # usersdf.loc[usersdf['Department'] == 2, 'user_department_name'] = 'Optom' 

    print('INFO! %d rows' %(len(expensesdf)))

    expensesdf.rename (columns = {
        'DocEntry':'doc_entry',
        'DocNum':'doc_number',
        'Expense':'expense_name',
        'Expense_Account':'expense_account',
        'UserSign':'user_signature',
        'CreateDate':'create_date',
        'CreateTime':'create_time',
        'Creator':'user_code',
        'Warehouse_Code':'warehouse_code',
        'Account_Code':'account_code',
        'Account_Name':'account_name',
        'Amount':'expense_amount',
        'Document_Date':'doc_date',
        'Status':'expense_status',
        'Journal_Entry_No':'expense_journal_entry_no',
        'Remarks':'expense_remarks'}
    ,inplace=True)

    # query = """delete from mabawa_staging.source_expenses where create_date::date = current_date -3 ;"""
    # query = pg_execute(query)

    if expensesdf.empty:
        print('INFO! Items dataframe is empty!')
    else:
        expensesdf = expensesdf.set_index(['doc_entry'])

        print('TRANSFORMATION! Adding new rows')

        upsert(engine=engine,
        df=expensesdf,
        schema='mabawa_staging',
        table_name='source_expenses',
        if_row_exists='update',
        create_table=False)

    # expensesdf.to_sql('source_expenses', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)






