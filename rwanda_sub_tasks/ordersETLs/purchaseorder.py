import sys

sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from datetime import date, timedelta
from pangres import upsert
from sub_tasks.data.connect_voler import engine
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate


def fetch_purchase_orders():
    SessionId = return_session_id(country="Rwanda")

    pagecount_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetPurchaseOrders&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    pagecount_payload = {}
    pagecount_headers = {}

    pagecount_response = requests.request(
        "GET",
        pagecount_url,
        headers=pagecount_headers,
        data=pagecount_payload,
        verify=False,
    )
    data = pagecount_response.json()

    orders_header = pd.DataFrame()
    orders_line = pd.DataFrame()
    payload = {}
    headers = {}
    pages = data["result"]["body"]["recs"]["PagesCount"]

    print("Retrieved No of Pages")
    print(pages)

    for i in range(1, pages + 1):
        page = i
        print(page)
        url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetPurchaseOrders&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request(
            "GET", url, headers=headers, data=payload, verify=False
        )
        response = response.json()
        response = response["result"]["body"]["recs"]["Results"]
        headers_df = pd.DataFrame([d["details"] for d in response])
        line_df = pd.DataFrame(
            [{"id": k, **v} for d in response for k, v in d["itemdetails"].items()]
        )
        orders_header = orders_header.append(headers_df, ignore_index=True)
        orders_line = orders_line.append(line_df, ignore_index=True)

    print("Finished API calls")

    orders_header.rename(
        columns={
            "DocEntry": "doc_entry",
            "Document_Number": "document_number",
            "Document_Status": "document_status",
            "Posting_Date": "posting_date",
            "Customer_Vendor_Code": "cust_vendor_code",
            "BP_Reference_No": "bp_reference_no",
            "Reference1": "reference1",
            "Generation_Time": "generation_time",
            "Sales_Employee": "sales_employee",
            "CreateDate": "createdate",
            "Document_Date": "document_date",
            "User_Signature": "user_signature",
            "Document_Type": "document_type",
            "Canceled": "order_canceled",
            "Warehouse_Status": "warehouse_status",
            "Creatn_Time_Incl_Secs": "creatn_time_incl_secs",
            "Order_Screen": "orderscreen",
            "Sales_Order": "sales_order",
            "Order_Entry": "order_entry",
        },
        inplace=True,
    )

    print("Header Columns Renamed")

    orders_header["posting_date"] = pd.to_datetime(orders_header["posting_date"])

    orders_header.set_index("doc_entry", inplace=True)

    upsert(
        engine=engine,
        df=orders_header,
        schema="voler_staging",
        table_name="source_purchase_orders_header",
        if_row_exists="update",
        create_table=True,
    )

    print("Inserted Orders Header")

    """
    ORDERS LINE
    """

    # orders_line.rename (columns = {
    #     'Document_Internal_ID':'doc_internal_id',
    #     'Target_Document_Type':'target_document_type',
    #     'Target_Document_Internal_ID':'target_doc_internal_id',
    #     'Row_Status':'row_status',
    #     'Item_No':'item_no',
    #     'Quantity':'qty',
    #     'Warehouse_Code':'warehouse_code',
    #     'Posting_Date':'posting_date',
    #     'BaseBPCode':'base_bp_code',
    #     }
    #     ,inplace=True)

    # print("Line Columns Renamed")

    # # print(orders_line.duplicated(keep=False).sum())

    # orders_line.set_index(['id','doc_internal_id','target_doc_internal_id','item_no'],inplace=True)

    # orders_line['posting_date'] = pd.to_datetime(orders_line['posting_date'],yearfirst=True)

    # upsert(engine=engine,
    #    df=orders_line,
    #    schema='voler_staging',
    #    table_name='source_purchase_orders_line',
    #    if_row_exists='update',
    #    create_table=True)

    # print("Inserted Orders Lines")


# fetch_purchase_orders()
