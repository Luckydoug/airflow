import sys

sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
from datetime import date, timedelta
from sub_tasks.data.connect_voler import engine
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate


def fetch_sap_discounts():
    SessionId = return_session_id(country="Rwanda")

    pagecount_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrderDiscountDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    pagecount_payload = {}
    pagecount_headers = {}

    pagecount_response = requests.request(
        "GET",
        pagecount_url,
        headers=pagecount_headers,
        data=pagecount_payload,
        verify=False,
    )
    pagecount_response = pagecount_response.json()

    df = pd.DataFrame()
    payload = {}
    headers = {}
    pages = pagecount_response["result"]["body"]["recs"]["PagesCount"]

    print("Pages outputted", pages)

    for i in range(1, pages + 1):
        page = i
        url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrderDiscountDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request(
            "GET", url, headers=headers, data=payload, verify=False
        )
        response = response.json()
        response = response["result"]["body"]["recs"]["Results"]
        response = pd.DataFrame.from_dict(response)
        response = response.T

        df = df.append(response, ignore_index=True)

    print("INFO! %d rows" % (len(df)))

    df.rename(
        columns={
            "DocEntry": "doc_entry",
            "DocNum": "doc_num",
            "UserSign": "user_sign",
            "CreateDate": "create_date",
            "CreateTime": "create_time",
            "Creator": "creator",
            "Doc_Date": "doc_date",
            "Order_Status": "order_status",
            "ApplyDiscountPer": "apply_discount_per",
            "ApplyDiscountAmount": "apply_discount_amount",
            "OrderRemainingAmt": "order_remaining_amt",
            "OrderTotalAmt": "order_total_amt",
            "Order_DocEntry": "order_doc_entry",
            "Order_DocNum": "order_doc_num",
            "UserCode": "user_code",
            "Order_Customer_Code": "order_customer_code",
            "Order_Date": "order_date",
            "Branch": "branch",
            "Discount_Reason": "discount_reason",
            "Order_Discount_Category": "order_discount_category",
            "Old_Order_No": "old_order_no",
            "Order_RX_Tolerance": "order_rx_tolerance",
            "Discount Summary": "discount_summary",
            "Staff Negligence": "staff_negligence",
        },
        inplace=True,
    )

    print("Header Columns Renamed")

    date_cols = ["create_date", "doc_date", "order_date"]
    df[date_cols] = df[date_cols].apply(pd.to_datetime, yearfirst=True)
    print("Date Column Types Changed")

    num_cols = [
        "apply_discount_per",
        "apply_discount_amount",
        "order_remaining_amt",
        "order_total_amt",
    ]
    df[num_cols] = df[num_cols].apply(pd.to_numeric)
    print("Numeric Column Types Changed")

    if df.empty:
        print("INFO! Discounts dataframe is empty!")

    else:
        df = df.set_index(["doc_entry"])
        print("TRANSFORMATION! Adding new rows")

        upsert(
            engine=engine,
            df=df,
            schema="voler_staging",
            table_name="source_discounts",
            if_row_exists="update",
            create_table=False,
        )

        print("Update successful")


# fetch_sap_discounts()
