import sys

from numpy import nan
sys.path.append(".")

#import libraries
from io import StringIO
import json
import psycopg2
import requests
import pandas as pd
from pandas.io.json._normalize import nested_to_record 
from sqlalchemy import create_engine
from airflow.models import Variable
from pandas.io.json._normalize import nested_to_record 
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from datetime import date, timedelta
import datetime


from sub_tasks.data.connect_mawingu import (pg_execute, pg_fetch_all, engine)  
from sub_tasks.api_login.api_login import(login_uganda)


SessionId = login_uganda()

# FromDate = '2023/05/01'
# ToDate = '2023/05/04'

today = date.today()
pastdate = today - timedelta(days=1)
FromDate = pastdate.strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')

print(FromDate)
print(ToDate)

# api details
pagecount_url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetWebPayments&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
pagecount_payload={}
pagecount_headers = {}

def fetch_sap_web_payments():
    
    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    pagecount_response = pagecount_response.json()
    
    paymentsdf = pd.DataFrame()
    payload={}
    headers = {}
    pages = pagecount_response['result']['body']['recs']['PagesCount']


    print("Pages outputted", pages)

    for i in range(1, pages+1):
        page = i
        
        url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetWebPayments&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()
        response = response['result']['body']['recs']['Results']
        response = pd.DataFrame.from_dict(response)
        response = response.T
        paymentsdf = paymentsdf.append(response, ignore_index=True)
    
    print('INFO! %d rows' %(len(paymentsdf)))

    paymentsdf.rename (columns = {'DocEntry':'doc_entry', 
                                "DocNum": "doc_no",
                                "CreateDate": "create_date",
                                "CreateTime": "create_time",
                                "UpdateDate": "update_date",
                                "UpdateTime": "update_time",
                                "Creator": "creator",
                                "Customer Loyalty Code": "cust_code",
                                "Type": "type",
                                "Unique Id": "unique_id",
                                "Payment Mode": "payment_mode",
                                "Amount": "amount",
                                "Insurance Company Name": "insurance_company_name",
                                "Insurance Scheme": "insurance_scheme",
                                "Insurance Membership No.": "insurance_membership_no",
                                "Principal Member Name": "principal_member_name",
                                "Principal Member No.": "principal_member_no",
                                "Status": "status",
                                "Remaining Amt": "remaining_amt",
                                "Advance Amt": "advance_amt",
                                "Branch": "branch",
                                "Amount with Tax": "amount_with_tax",
                                "Mpesa Amount": "mpesa_amount",
                                "Cheque Due Date": "cheque_due_date",
                                "Cheque No.": "cheque_no",
                                "Bank Name for Cheque": "bank_name_for_cheque",
                                "Transfer Date": "transfer_date",
                                "Bank Reference": "bank_reference",
                                "Credit Card Name": "credit_card_name",
                                "CC Payment Method": "cc_payment_method",
                                "CC Voucher No.": "cc_voucher_no",
                                "Voucher Type": "voucher_type",
                                "Gift Voucher No": "gift_voucher_no",
                                "Insurance Type": "insurance_type",
                                "No of Insurances": "no_of_insurances",
                                "Order CancelPayment": "order_cancel_payment",
                                "Finance Status": "finance_status",
                                "Sales Employee": "sales_employee",
                                "Payment Means No": "payment_means_no",
                                "Mpesa Integrated": "mpesa_integrated",
                                "Mpesa Transaction ID": "mpesa_transaction_id",
                                "Cancellation Remarks": "cancellation_remarks",
                                "Home Eye Test": "home_eyetest",
                                "Diagnosis": "diagnosis"
                                }
            ,inplace=True)

    paymentsdf['amount'] = pd.to_numeric(paymentsdf['amount'])
    paymentsdf['advance_amt'] = pd.to_numeric(paymentsdf['advance_amt'])
    paymentsdf['amount_with_tax'] = pd.to_numeric(paymentsdf['amount_with_tax'])
    paymentsdf['mpesa_amount'] = pd.to_numeric(paymentsdf['mpesa_amount'])
    paymentsdf['amount'] = pd.to_numeric(paymentsdf['amount'])
    paymentsdf['advance_amt'] = pd.to_numeric(paymentsdf['advance_amt'])
    paymentsdf['amount_with_tax'] = pd.to_numeric(paymentsdf['amount_with_tax'])
    paymentsdf['mpesa_amount'] = pd.to_numeric(paymentsdf['mpesa_amount'])


    if paymentsdf.empty:
        print('INFO! Payments dataframe is empty!')
    else:
        paymentsdf = paymentsdf.set_index(['doc_entry'])
        print('TRANSFORMATION! Adding new rows')

        upsert(engine=engine,
        df=paymentsdf,
        schema='mawingu_staging',
        table_name='source_web_payments',
        if_row_exists='update',
        create_table=False)

        print('Update successful')

       