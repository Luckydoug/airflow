import sys
from numpy import nan

sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
from datetime import date, timedelta
from sub_tasks.data.connect_mawingu import pg_execute, engine
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate

# FromDate = "2024/07/01"
# ToDate = "2024/06/30"

def fetch_sap_orderscreendetailsc1():
    SessionId = return_session_id(country="Uganda")

    pagecount_url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrderDetailsC1&pageNo=1&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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
    for i in range(1, pages + 1):
        page = i

        url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrderDetailsC1&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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
    query = """truncate mawingu_staging.landing_orderscreenc1;"""
    query = pg_execute(query)

    orderscreenc1.to_sql(
        "landing_orderscreenc1",
        con=engine,
        schema="mawingu_staging",
        if_exists="append",
        index=False,
    )


# fetch_sap_orderscreendetailsc1()


def update_to_source_orderscreenc1():

    data = pd.read_sql(
        """
    SELECT distinct
    doc_entry, odsc_lineid, odsc_date, odsc_time, odsc_status, odsc_doc_no, odsc_createdby, odsc_document, odsc_remarks
    FROM mawingu_staging.landing_orderscreenc1;
    """,
        con=engine,
    )

    data = data.set_index(["doc_entry", "odsc_lineid"])

    upsert(
        engine=engine,
        df=data,
        schema="mawingu_staging",
        table_name="source_orderscreenc1",
        if_row_exists="update",
        create_table=False,
    )

    print("source_orderscreenc1")

# update_to_source_orderscreenc1()