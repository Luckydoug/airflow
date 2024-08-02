import sys
sys.path.append(".")
import requests
import pandas as pd
from datetime import date, timedelta
from airflow.models import Variable
from pangres import upsert
from sub_tasks.data.connect import engine 
# from sub_tasks.api_login.api_login import(login)
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate

def fetch_sap_inventory_transfer():
    SessionId = return_session_id(country = "Kenya")
   #SessionId = login()

    pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetInventoryTransferDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    pagecount_payload={}
    pagecount_headers = {}

    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    
    transfer_df = pd.DataFrame()
    payload={}
    headers = {}
    pages = data['result']['body']['recs']['PagesCount']

    print("Retrived No of Pages")

    for i in range(1, pages+1):
        page = i
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetInventoryTransferDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()

        try:
            response = response['result']['body']['recs']['Results']
            response = pd.DataFrame.from_dict(response)
            transfer_df = transfer_df.append(response, ignore_index=True)
        except:
            print('error')
        
    print(transfer_df)

    """
    CREATING DETAILS TABLE
    """
    transfer = transfer_df['details'].apply(pd.Series)

    transfer.rename (columns = {
            'Internal_Number':'internal_no',
            'Document_Number':'doc_no',
            'Posting_Date':'post_date',
            'Remarks':'remarks',
            'Generation_Time':'generation_time',
            'Creation_Date':'createdon',
            'Creatn_Time_Incl_Secs':'creation_time_incl_secs',
            'PickRemarks':'pick_remarks',
            'ExchangeType':'exchange_type',
            'DamagedReason':'damaged_reason'}
        ,inplace=True)

    transfer = transfer.set_index('internal_no')

    upsert(engine=engine,
        df=transfer,
        schema='mabawa_staging',
        table_name='source_it',
        if_row_exists='update',
        create_table=False)

    print("Rows Inserted")

    """
    CREATING ITEM DETAILS TABLE
    """
    transfer_itemdetails = transfer_df['itemdetails']
    transfer_itemdetails = transfer_itemdetails.to_frame()

    itemdetails_df = pd.DataFrame()
    for index, row in transfer_itemdetails.iterrows():
        row_data=row['itemdetails']
        data = pd.DataFrame.from_dict(row_data)
        data1 = data.T
        itemdetails_df = itemdetails_df.append(data1, ignore_index=True)

    print("Created Item Details DF")

    itemdetails_df.rename (columns = {
            'Document_Internal_ID':'doc_internal_id',
            'RowNumber':'row_no',
            'Base_Document_Reference':'base_doc_ref',
            'Base_Document_Internal_ID':'base_doc_internal_id',
            'Item_No':'item_no',
            'Quantity':'qty',
            'Posting_Date':'post_date',
            'Sales_Order_number':'sales_orderno',
            'Sales_Order_entry':'sales_order_entry',
            'Sales_Order_branch':'sales_order_branch',
            'Draft_order_entry':'draft_order_entry',
            'Draft_order_Number':'draft_orderno',
            'Draft_Order_Branch':'draft_order_branch',
            'Flag_for_INV':'flag_for_inv',
            'Replaced_ItemCode':'replaced_item_code',
            'Picker_Name':'picker_name'}
        ,inplace=True)

    itemdetails_df = itemdetails_df.set_index(['doc_internal_id','row_no'])

    upsert(engine=engine,
        df=itemdetails_df,
        schema='mabawa_staging',
        table_name='source_it_details',
        if_row_exists='update',
        create_table=False)

    return "IT Complete"

# fetch_sap_inventory_transfer()    