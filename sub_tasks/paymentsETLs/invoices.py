import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import datetime
import pandas as pd
from datetime import date, timedelta
from airflow.models import Variable
from sqlalchemy import create_engine
from pangres import upsert, DocsExampleTable
from pandas.io.json._normalize import nested_to_record 

from sub_tasks.data.connect import (engine) 
from sub_tasks.api_login.api_login import(login)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# get session id
SessionId = login()

# FromDate = '2022/11/30'
# ToDate = '2022/11/30'

today = date.today()
pastdate = today - timedelta(days=1)
FromDate = pastdate.strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')

# api details
pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetInvoiceDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
pagecount_payload={}
pagecount_headers = {}

def fetch_sap_invoices():

    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    pages = data['result']['body']['recs']['PagesCount']

    print("Pages outputted", pages)

    invoices_df = pd.DataFrame()
    payload={}
    headers = {}

    for i in range(1, pages+1):
        page = i
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetInvoiceDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()
        response = response['result']['body']['recs']['Results']
        response = pd.DataFrame.from_dict(response)
        invoices_df = invoices_df.append(response, ignore_index=True)
    
    if invoices_df.empty:
        print('INFO! invoices dataframe is empty!')
    else:
        
        '''
        INSERT THE invoices Header TABLE
        '''
        invoices_details = invoices_df['details'].apply(pd.Series)

        print('INFO! %d rows' %(len(invoices_details)))
        
        invoices_details.rename (columns = {
            'DocType':'doctype',
            'Internal_Number':'internal_number',
            'Document_Number':'document_number',
            'Canceled':'invoice_canceled',
            'Document_Status':'document_status',
            'Warehouse_Status':'warehouse_status',
            'Posting_Date':'posting_date',
            'Customer_Vendor_Code':'cust_code',
            'Total_Tax':'invoice_tax',
            'Discount_for_Document':'invoice_document_discount',
            'Total_Discount':'invoice_total_discount',
            'Document_Total':'invoice_total_amount',
            'Generation_Time':'invoice_generation_time',
            'Sales_Employee':'invoice_sales_employee',
            'Creation_Date':'invoice_creation_date',
            'User_Signature':'user_signature',
            'Creatn_Time_Incl_Secs':'invoice_creation_time_incl_secs',
            'Prescription_Spectacle':'prescription_spectacle',
            'Order_Screen':'order_screen_doc_entry',
            'Earned_Loyalty_Points':'invoice_earned_loyalty_points',
            'LoyaltyPointsConsumed':'invoice_loyaltypointsconsumed',
            'LoyaltyPointsExpireDate':'invoice_loyaltypointsexpiredate',
            'Draft_Order_No':'draft_order_no'
            }
        ,inplace=True)
        print('columns renamed')

        # invoices_details = invoices_details.set_index('internal_number')

        print('TRANSFORMATION! Adding new invoice header rows')

        invoices_details.to_sql('source_invoices', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)

        print('TRANSFORMATION! Added new invoice header rows')

        '''
        INSERT THE INVOICE DETAILS TABLE
        '''

        print('TRANSFORMATION! Item Details')
        
        invoices_itemdetails = invoices_df['itemdetails']
        invoices_itemdetails_df = invoices_itemdetails.to_frame()

        itemdetails_df = pd.DataFrame()
        for index, row in invoices_itemdetails_df.iterrows():
            row_data=row['itemdetails']
            data = pd.DataFrame.from_dict(row_data)
            data1 = data.T
            itemdetails_df = itemdetails_df.append(data1, ignore_index=True)

        print('INFO! %d rows' %(len(itemdetails_df)))

        #itemdetails_df['Staff_Code'] = pd.to_numeric(itemdetails_df['Staff_Code'], errors='coerce').fillna(0)
        itemdetails_df['Item_No'] = itemdetails_df['Item_No'].fillna('Registration')

        itemdetails_df.rename (columns = {
            'DocumentInternal_ID':'doc_internal_id',
            'Base_Document_Reference':'base_doc_reference',
            'Base_Document_Internal_ID':'base_doc_internal_id',
            'Item_No':'item_code',
            'Quantity':'quantity',
            'Price_after_Discount':'price_after_discount',
            'Discount_per_Row':'discount_per_row',
            'Row_Total':'total_amount',
            'Warehouse_Code':'warehouse_code',
            'Gross_Price_after_Discount':'gross_price_after_discount',
            'Total_Tax':'total_tax',
            'Gross_Total':'gross_total'}
        ,inplace=True)

        # itemdetails_df = itemdetails_df.set_index(['doc_internal_id','item_code'])

        print('TRANSFORMATION! Adding new Target Details rows')

        itemdetails_df.to_sql('source_invoices_details', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)



