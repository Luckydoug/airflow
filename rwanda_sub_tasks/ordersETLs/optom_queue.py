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


def fetch_optom_queue_mgmt():
    SessionId = return_session_id(country="Rwanda")

    pagecount_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetOptomQManagment&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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

    optom_queue = pd.DataFrame()

    for i in range(1, pages + 1):
        page = i
        url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetOptomQManagment&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request(
            "GET", url, headers=headers, data=payload, verify=False
        )
        response = response.json()

        try:
            response = response["result"]["body"]["recs"]["Results"]
            optomdf = pd.DataFrame(response)
            optomdf = optomdf.T
            optom_queue = optom_queue.append(optomdf, ignore_index=False)

        except:
            print("Error")

    # rename columns
    optom_queue.rename(
        columns={
            "DocEntry": "doc_entry",
            "DocNum": "doc_no",
            "UserSign": "user_sign",
            "CreateDate": "create_date",
            "CreateTime": "create_time",
            "UpdateDate": "update_date",
            "UpdateTime": "update_time",
            "Creator": "creator",
            "OutLetID": "outlet_id",
            "CustomerID": "cust_id",
            "OptumID": "optom_id",
            "Status": "status",
            "QueueType": "queue_type",
            "VisitType": "visit_type",
            "VisitId": "visit_id",
            "ActivityNo": "activity_no",
        },
        inplace=True,
    )

    print("Columns Renamed")

    # transformation
    optom_queue = optom_queue.set_index("doc_entry")

    # df to db
    upsert(
        engine=engine,
        df=optom_queue,
        schema="voler_staging",
        table_name="source_optom_queue_mgmt",
        if_row_exists="update",
        create_table=False,
    )

    print("Update Successful")


# fetch_optom_queue_mgmt()
