import sys

sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
from sub_tasks.data.connect_voler import engine
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate


def fetch_order_checking_details():
    SessionId = return_session_id(country="Rwanda")

    pagecount_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrderCheckingDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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
    pages = data["result"]["body"]["recs"]["PagesCount"]

    print(pages)

    print("Retrived Number of Pages")

    headers = {}
    payload = {}

    order_checking_details = pd.DataFrame()

    for i in range(1, pages + 1):
        page = i
        url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrderCheckingDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request(
            "GET", url, headers=headers, data=payload, verify=False
        )
        response = response.json()

        try:
            response = response["result"]["body"]["recs"]["Results"]
            order_checking_detailsdf = pd.DataFrame(response)
            order_checking_detailsdf = order_checking_detailsdf.T
            order_checking_details = order_checking_details.append(
                order_checking_detailsdf, ignore_index=False
            )

        except:
            print("Error")

    # rename columns
    order_checking_details.rename(
        columns={
            "DocEntry": "doc_entry",
            "DocNum": "doc_no",
            "UserSign": "user_sign",
            "CreateDate": "create_date",
            "CreateTime": "create_time",
            "Creator": "creator",
            "UpdateDate": "update_date",
            "UpdateTime": "update_time",
            "Customer_Loyalty_Code": "cust_loyalty_code",
            "Visit_Id": "visit_id",
            "Order_Entry": "order_entry",
            "OrderNum": "order_no",
            "Branch": "branch",
            "ViewDate": "view_date",
            "Status": "status",
        },
        inplace=True,
    )

    print("Columns Renamed")

    # transformation
    order_checking_details = order_checking_details.set_index("doc_entry")

    # order_checking_details['create_date'] = order_checking_details['create_date'].dt.date
    # order_checking_details['create_date'] = pd.to_datetime(order_checking_details.create_date).dt.date
    print("Transformation Complete")

    # df to db
    upsert(
        engine=engine,
        df=order_checking_details,
        schema="voler_staging",
        table_name="source_order_checking_details",
        if_row_exists="update",
        create_table=True,
    )

    print("Update Successful")


# fetch_order_checking_details()
