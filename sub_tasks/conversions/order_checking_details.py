import sys
from airflow.models import variable
sys.path.append(".")
import json
import requests
import pandas as pd
from datetime import date, timedelta
from pangres import upsert
from sub_tasks.data.connect import (pg_execute, engine) 
# from sub_tasks.api_login.api_login import(login)
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate



def fetch_order_checking_details():
    SessionId = return_session_id(country = "Kenya")
    #SessionId = login()

    pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderCheckingDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    pagecount_payload={}
    pagecount_headers = {}
    
    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    pages = data['result']['body']['recs']['PagesCount']

    print("Retrived Number of Pages")

    headers = {}
    payload = {}

    order_checking_details = pd.DataFrame()

    for i in range(1, pages+1):
        page = i
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderCheckingDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()

        try:
            response = response['result']['body']['recs']['Results']
            order_checking_detailsdf= pd.DataFrame(response)
            order_checking_detailsdf = order_checking_detailsdf.T
            order_checking_details = order_checking_details.append(order_checking_detailsdf, ignore_index=False)
            
        except:
            print('Error')

    order_checking_details.rename(columns={
                        'DocEntry': 'doc_entry',
                        'DocNum': 'doc_no',
                        'UserSign': 'user_sign',
                        'CreateDate': 'create_date',
                        'CreateTime': 'create_time',
                        'Creator': 'creator',
                        'UpdateDate': 'update_date',
                        'UpdateTime': 'update_time',
                        'Customer_Loyalty_Code': 'cust_loyalty_code',
                        'Visit_Id': 'visit_id',
                        'Order_Entry': 'order_entry',
                        'OrderNum': 'order_no',
                        'Branch': 'branch',
                        'ViewDate': 'view_date',
                        'Status': "status"},
                        inplace=True)

    print("Columns Renamed")

    order_checking_details = order_checking_details.set_index('doc_entry')
    
    
    print("Transformation Complete")

    upsert(engine=engine,
       df=order_checking_details,
       schema='mabawa_staging',
       table_name='source_order_checking_details',
       if_row_exists='update',
       create_table=True)

    print('Update Successful')

def create_fact_order_checking_details ():
    
    query = """
    truncate mabawa_dw.fact_order_checking_details;
    insert into mabawa_dw.fact_order_checking_details
    select 
        distinct
            doc_entry, doc_no, user_sign, create_date, create_time, creator, update_date, update_time, cust_loyalty_code, 
            visit_id, order_entry, branch, view_date::date, status, draft_order_no, draftorder_posting_date, draftorder_createdate,
            draftorder_creation_time, presctiption_no, salesorder_no, salesorder_date, salesorder_createdate, 
            salesorder_createtime, salesorder_canceled, salesorder_user_signature, salesorder_sales_employee
    from mabawa_mviews.v_fact_order_checking_details
    """
    query = pg_execute(query)


def create_view_rx_pres ():

    query = """
    truncate mabawa_mviews.view_rx_presc;
    insert into mabawa_mviews.view_rx_presc
    select  doc_entry::text,draft_orderno::text,
	        draftorder_createdate::date, creation_date::date, 
	        user_code, same_user from mabawa_mviews.v_view_rx_presc
    """

    query = pg_execute(query)

def create_view_rx_clrr ():

    query = """
    truncate mabawa_mviews.view_rx_clrr;
    insert into mabawa_mviews.view_rx_clrr	
    select  doc_entry::text,draft_orderno::text,
	        draftorder_createdate::date, creation_date::date, 
	        user_code, same_user from mabawa_mviews.v_view_rx_clrr 
    """

    query = pg_execute(query)

def create_view_rx_pay ():

    query = """
    truncate mabawa_mviews.view_rx_pay;
    insert into mabawa_mviews.view_rx_pay
    select 
        doc_entry::text, pay_doc_entry::text, createdon::date
    from mabawa_mviews.v_view_rx_pay
    """

    query = pg_execute(query)

def create_view_rx_conv ():

    query = """
    truncate mabawa_mviews.view_rx_conv;
    insert into mabawa_mviews.view_rx_conv
    SELECT doc_entry, view_date, creator, cust_loyalty_code, visit_id, 
    branch, doc_entry1, ord_orderno, ord_ordercreation_date, ord_user_code, 
    ord_days, doc_entry2, pres_draft_orderno, pres_ordercreation_date, pres_order_branch, 
    pres_user_code, pres_same_user, pres_days, doc_entry3, clrr_orderno, clrr_ordercreation_date, 
    clrr_order_branch, clrr_user_code, clrr_same_user, clrr_days, doc_entry4, pay_view_date, pay_doc_entry, 
    pay_createdon, pay_days, days
    FROM mabawa_mviews.v_view_rx_conv;
    insert into mabawa_dw.update_log(table_name, update_time) values('view_rx_conv', default);
    """

    query = pg_execute(query)

# fetch_order_checking_details()