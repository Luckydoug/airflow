import sys

sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
from sub_tasks.data.connect_mawingu import engine
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate

FromDate = "2018/01/01"
# ToDate = "2024/07/10"


def fetch_sap_users():
    SessionId = return_session_id(country="Uganda")
    pagecount_url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetUserDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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

    usersdf = pd.DataFrame()
    payload = {}
    headers = {}
    for i in range(1, pages + 1):
        page = i
        url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetUserDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request(
            "GET", url, headers=headers, data=payload, verify=False
        )
        users = response.json()
        stripped_users = users["result"]["body"]["recs"]["Results"]
        users_df = pd.DataFrame.from_dict(stripped_users)
        users_df2 = users_df.T
        usersdf = usersdf.append(users_df2, ignore_index=True)

    print("TRANSFORMATION! Adding new columns")

    # condition to add new column if optom or sales person
    usersdf.loc[usersdf["Department"] == 1, "user_department_name"] = "Sales Person"
    usersdf.loc[usersdf["Department"] == 2, "user_department_name"] = "Optom"

    print("INFO! %d rows" % (len(usersdf)))

    usersdf.rename(
        columns={
            "UserSignature": "user_signature",
            "Internal_Number": "internal_no",
            "User_Code": "user_code",
            "User_Name": "user_name",
            "Max_Discount": "max_discount",
            "User_Locked": "user_locked_status",
            "Department": "user_department_code",
            "Spct_Len_Eligible": "spct_len_eligible",
            "SE_Optom": "se_optom",
            "PAYROLL_NUMBER": "payroll_no",
        },
        inplace=True,
    )

    if usersdf.empty:
        print("INFO! Users dataframe is empty!")
    else:
        usersdf = usersdf.set_index(["user_signature"])
        print("INFO! Users upsert started...")

        upsert(
            engine=engine,
            df=usersdf,
            schema="mawingu_staging",
            table_name="source_users",
            if_row_exists="update",
            create_table=False,
        )

        print("Update successful")

# fetch_sap_users()