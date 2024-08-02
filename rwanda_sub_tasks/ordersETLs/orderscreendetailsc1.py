import sys

sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
from sub_tasks.data.connect_voler import pg_execute, engine
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate


def fetch_sap_orderscreendetailsc1():
    SessionId = return_session_id(country="Rwanda")

    pagecount_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrderDetailsC1&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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

    orderscreenc1 = pd.DataFrame()
    payload = {}
    headers = {}
    pages = data["result"]["body"]["recs"]["PagesCount"]
    print(pages)
    for i in range(1, pages + 1):
        page = i
        print(page)
        url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrderDetailsC1&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request(
            "GET", url, headers=headers, data=payload, verify=False
        )
        response = response.json()
        response = response["result"]["body"]["recs"]["Results"]
        response = pd.DataFrame.from_dict(response)
        response = response.T
        orderscreenc1 = orderscreenc1.append(response, ignore_index=True)

    orderscreenc1.rename(
        columns={
            "DocEntry": "doc_entry",
            "LineId": "odsc_lineid",
            "Date": "odsc_date",
            "Time": "odsc_time",
            "Status": "odsc_status",
            "DocumentNo": "odsc_doc_no",
            "CreatedUser": "odsc_createdby",
            "Document": "odsc_document",
            "Remarks": "odsc_remarks",
        },
        inplace=True,
    )

    print("Renamed successfully")

    query = """truncate voler_staging.landing_orderscreenc1;"""
    query = pg_execute(query)

    orderscreenc1.to_sql(
        "landing_orderscreenc1",
        con=engine,
        schema="voler_staging",
        if_exists="append",
        index=False,
    )


def update_to_source_orderscreenc1():

    data = pd.read_sql(
        """
    SELECT distinct
    doc_entry, odsc_lineid, odsc_date, odsc_time, odsc_status, odsc_doc_no, odsc_createdby, odsc_document, odsc_remarks
    FROM voler_staging.landing_orderscreenc1;
    """,
        con=engine,
    )

    data = data.set_index(["doc_entry", "odsc_lineid"])

    upsert(
        engine=engine,
        df=data,
        schema="voler_staging",
        table_name="source_orderscreenc1",
        if_row_exists="update",
        create_table=False,
    )

    print("source_orderscreenc1")
