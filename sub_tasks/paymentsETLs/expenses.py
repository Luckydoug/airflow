import sys
sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
from sub_tasks.data.connect import engine
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate


def fetch_sap_expenses():
    # SessionId = login()
    SessionId = return_session_id(country = "Kenya")
    pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOptomDailyExpenses&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    pagecount_payload={}
    pagecount_headers = {}
   
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







