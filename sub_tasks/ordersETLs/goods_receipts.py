import sys
sys.path.append(".")
import requests
import psycopg2
import pandas as pd
from datetime import date, timedelta
from pangres import upsert
from sub_tasks.data.connect import (pg_execute, engine) 
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate



def fetch_goods_receipt():
    SessionId = return_session_id(country = "Kenya")
    pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetGoodsReceiptsDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    pagecount_headers = {}
    pagecount_payload = {}
    
    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    
    goods_receipt = pd.DataFrame()
    payload={}
    headers = {}
    pages = data['result']['body']['recs']['PagesCount']
    for i in range(1, pages+1):
        page = i
        print('paginating')
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetGoodsReceiptsDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"

        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()
        response = response['result']['body']['recs']['Results']
        response = pd.DataFrame.from_dict(response)
        goods_receipt = goods_receipt.append(response, ignore_index=True)
    

    goods_receipt = goods_receipt['details'].apply(pd.Series)
    print(goods_receipt)

    goods_receipt.rename (columns = {'DocEntry':'doc_entry', 
                       'Document_Number':'doc_no', 
                       'Document_Status':'doc_status', 
                       'Posting_Date':'post_date', 
                       'Due_Date':'due_date', 
                       'Customer_Vendor_Code':'cust_value_code', 
                       'BP_Reference_No':'bp_ref_no', 
                       'Discount_for_Document':'disc_for_doc', 
                       'Total_Discount':'total_discount',
                       'Document_Currency':'doc_currency',
                       'Document_Rate':'doc_rate',
                       'Reference_1':'reference_1',
                       'Transaction_Number':'transaction_no',
                       'Generation_Time':'generation_time',
                       'Creation_Date':'creation_date',
                       'User_Signature':'user_signature',
                       'Landed_Cost_Number':'landed_cost_no',
                       'Number_of_Additional_Days':'no_of_add_days',
                       'Creatn_Time_Incl_Secs':'creation_time_incl_secs',
                       'Order_Screen':'order_screen',
                       'Sales_Order':'sales_order',
                       'Order_Entry':'order_entry',
                       'Final_ExchangeRate':'final_exchangerate'}
            ,inplace=True)

    goods_receipt = goods_receipt.set_index('doc_entry')

    upsert(engine=engine,
       df=goods_receipt,
       schema='mabawa_staging',
       table_name='source_goods_receipt',
       if_row_exists='update',
       create_table=True)


def goods_receipt_live():
    
    query = """
    truncate mabawa_dw.dim_goods_receipt;
    insert into mabawa_dw.dim_goods_receipt
    SELECT 
            doc_entry, doc_no, doc_status, post_date, due_date, cust_value_code, 
            bp_ref_no,
            regexp_replace(dhl_invoice, '.*?\s','') as dhl_invoice, 
            disc_for_doc, total_discount, doc_currency, 
            doc_rate, reference_1, transaction_no, generation_time, creation_date, 
            user_signature, landed_cost_no, no_of_add_days, creation_time_incl_secs, 
            order_screen, sales_order, order_entry, final_exchangerate
    FROM mabawa_staging.v_goods_receipt;
    """

    query = pg_execute(query)

