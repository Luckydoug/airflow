import sys

sys.path.append(".")

# import libraries
import json
import psycopg2
import requests
import datetime
import pandas as pd
from datetime import date
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine
from airflow.models import Variable
from pandas.io.json._normalize import nested_to_record


from sub_tasks.data.connect_voler import (
    pg_execute,
    pg_fetch_all,
    engine,
    pg_bulk_insert,
)
from sub_tasks.api_login.api_login import login_rwanda
from sub_tasks.libraries.utils import return_session_id


FromDate = "2018/01/01"
# ToDate = date.today().strftime('%Y/%m/%d')
# FromDate = date.today().strftime('%Y/%m/%d')
ToDate = date.today().strftime("%Y/%m/%d")


def fetch_sap_incentive_slab():
    SessionId = return_session_id(country="Rwanda")
    # SessionId = login_rwanda()

    pagecount_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetCashIncentive&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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
    print("Pages outputted", pages)

    incentives_df = pd.DataFrame()
    payload = {}
    headers = {}

    for i in range(1, pages + 1):
        page = i
        url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetCashIncentive&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request(
            "GET", url, headers=headers, data=payload, verify=False
        )
        incentives = response.json()
        stripped_incentives = incentives["result"]["body"]["recs"]["Results"]
        stripped_incentives_df = pd.DataFrame.from_dict(stripped_incentives)
        incentives_df = incentives_df.append(stripped_incentives_df, ignore_index=True)

    if incentives_df.empty:

        print("INFO! Targets dataframe is empty!")

    else:
        """
        INSERT THE Incentives Header TABLE
        """
        incentives_details = incentives_df["details"].apply(pd.Series)

        print("INFO! %d rows" % (len(incentives_details)))

        incentives_details.rename(
            columns={
                "DocEntry": "doc_entry",
                "DocNum": "doc_num",
                "UserSign": "user_signature",
                "CreateDate": "incentive_create_date",
                "CreateTime": "incentive_create_time",
                "Creator": "incentive_creator",
                "Monthly": "incentive_month",
                "Year": "incentive_year",
            },
            inplace=True,
        )

        incentives_details = incentives_details.set_index("doc_entry")

        print("TRANSFORMATION! Adding new incentive header rows")

        upsert(
            engine=engine,
            df=incentives_details,
            schema="voler_staging",
            table_name="source_incentives",
            if_row_exists="update",
            create_table=False,
        )

        incentives_itemdetails = incentives_df["itemdetails"]
        incentives_itemdetails_df = incentives_itemdetails.to_frame()

        itemdetails_df = pd.DataFrame()

        for index, row in incentives_itemdetails_df.iterrows():
            row_data = row["itemdetails"]
            data = pd.DataFrame.from_dict(row_data)
            data1 = data.T
            itemdetails_df = itemdetails_df.append(data1, ignore_index=True)

        print("INFO! %d rows" % (len(itemdetails_df)))

        itemdetails_df.rename(
            columns={
                "DocEntry": "doc_entry",
                "LineId": "line_id",
                "Branch_Code": "branch_code",
                "From_Cash_Target_Achd": "from_cash_target_achd",
                "Cash_Incentive": "cash_incentive",
                "Insurance_Incentive": "insurance_incentive",
                "To_Cash_Target_Achd": "to_cash_target_achd",
                "From_Ins_Target_Achd": "from_ins_target_achd",
                "To_Ins_Target_Achd": "to_ins_target_achd",
            },
            inplace=True,
        )

        itemdetails_df = itemdetails_df.set_index(["doc_entry", "line_id"])

        print("TRANSFORMATION! Adding new Incentive Details rows")

        upsert(
            engine=engine,
            df=itemdetails_df,
            schema="voler_staging",
            table_name="source_incentives_details",
            if_row_exists="update",
            create_table=False,
        )


# fetch_sap_incentive_slab()
