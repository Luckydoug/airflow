import sys
from numpy import nan
sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
from datetime import date, timedelta
from sub_tasks.data.connect_voler import pg_execute, engine
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate


def fetch_sap_orders():
    SessionId = return_session_id(country="Rwanda")

    pagecount_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrders&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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
        url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrders&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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

    """
    ORDERS HEADERS
    """
    orders_header.rename(
        columns={
            "Internal_Number": "internal_number",
            "Document_Number": "document_number",
            "Canceled": "order_canceled",
            "Document_Status": "document_status",
            "Warehouse_Status": "warehouse_status",
            "Posting_Date": "posting_date",
            "Customer_Vendor_Code": "cust_vendor_code",
            "Total_Tax": "total_tax",
            "Discount_for_Document": "order_document_discount",
            "Total_Discount": "order_total_discount",
            "Document_Total": "doc_total",
            "Generation_Time": "generation_time",
            "Sales_Employee": "sales_employee",
            "Tax_Amount_SC": "tax_amt_sc",
            "Document_Total_SC": "doc_total_sc",
            "Creation_Date": "creation_date",
            "User_Signature": "user_signature",
            "Creatn_Time_Incl_Secs": "creation_time",
            "Prescription_Spectacle": "presc_spec",
            "Advanced_Payment": "advanced_payment",
            "Remaining_Payment": "remaining_payment",
            "Order_Screen": "orderscreen",
            "Branch": "order_branch",
            "Prescription_No": "presc_no",
            "Presc_Print_Type": "presc_print_type",
            "Draft_Order_No": "draft_orderno",
        },
        inplace=True,
    )

    print("Header Columns Renamed")

    orders_header["posting_date"] = pd.to_datetime(orders_header["posting_date"])

    # orders_header = orders_header.set_index(['internal_number','document_number'])
    orders_header = orders_header.drop_duplicates(
        ["internal_number", "document_number"]
    ).set_index(["internal_number", "document_number"])

    upsert(
        engine=engine,
        df=orders_header,
        schema="voler_staging",
        table_name="source_orders_header",
        if_row_exists="update",
        create_table=True,
    )

    # orders_header.to_sql('source_tests', con = engine, schema='voler_staging', if_exists = 'append', index=False)

    print("Inserted Orders Header")

    """
    ORDERS LINE
    """
    orders_line.rename(
        columns={
            "DocumentInternal_ID": "doc_internal_id",
            "Target_Document_Internal_ID": "target_doc_internal_id",
            "Row_Status": "row_status",
            "Item_No": "item_no",
            "Quantity": "qty",
            "Unit_Price": "unit_price",
            "Discount_per_Row": "disc_per_row",
            "Warehouse_Code": "warehouse_code",
            "Gross_Price_after_Discount": "gross_price_after_disc",
            "Total_Tax_Row": "total_tax_row",
            "Flag_for_order": "flag_for_order",
        },
        inplace=True,
    )

    print("Line Columns Renamed")

    orders_line["item_no"].fillna("No ITEM", inplace=True)

    """
    orders_line = orders_line.set_index(['id', 'doc_internal_id', 'item_no'])

    upsert(engine=engine,
       df=orders_line,
       schema='voler_staging',
       table_name='source_orders_line',
       if_row_exists='update',
       create_table=True)

    print("Inserted Orders Lines")
    """
    query = """truncate voler_staging.landing_source_orders_line;"""
    query = pg_execute(query)

    orders_line = orders_line.drop_duplicates(["id", "doc_internal_id", "item_no"])

    orders_line.to_sql(
        "landing_source_orders_line",
        con=engine,
        schema="voler_staging",
        if_exists="append",
        index=False,
    )

    print("fetch_sap_orders done")


# fetch_sap_orders()


def update_source_orders_line():

    df = pd.read_sql(
        """
    SELECT 	distinct
        id, doc_internal_id, target_doc_internal_id, row_status, item_no, qty, 
        unit_price, disc_per_row, warehouse_code, gross_price_after_disc, total_tax_row, 
        flag_for_order
    FROM voler_staging.landing_source_orders_line;
    """,
        con=engine,
    )

    df = df.set_index(["id", "doc_internal_id", "item_no"])

    upsert(
        engine=engine,
        df=df,
        schema="voler_staging",
        table_name="source_orders_line",
        if_row_exists="update",
        create_table=True,
    )

    print("Updated Orders Lines")


# update_source_orders_line()
