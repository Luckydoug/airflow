import sys
from numpy import nan

sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
from datetime import date, timedelta
from sub_tasks.data.connect_mawingu import engine
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate


def fetch_sap_payments():
    SessionId = return_session_id(country="Uganda")

    pagecount_url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetPaymentMeans&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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

    paymentsdf = pd.DataFrame()
    payload = {}
    headers = {}
    pages = pagecount_response["result"]["body"]["recs"]["PagesCount"]

    print("Pages outputted", pages)

    for i in range(1, pages + 1):
        page = i
        # print(page)
        url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetPaymentMeans&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request(
            "GET", url, headers=headers, data=payload, verify=False
        )
        response = response.json()
        response = response["result"]["body"]["recs"]["Results"]
        response = pd.DataFrame.from_dict(response)
        response = response.T
        paymentsdf = paymentsdf.append(response, ignore_index=True)

    print("INFO! %d rows" % (len(paymentsdf)))

    paymentsdf.rename(
        columns={
            "DocEntry": "doc_entry",
            "DocNum": "doc_no",
            "UserSign": "user_sign",
            "CreateDate": "createdon",
            "CreateTime": "createdat",
            "Creator": "createdby",
            "Customer_Code": "cust_code",
            "Base_Doc": "base_doc",
            "Order_Number": "order_no",
            "Full_Document_Type": "full_doc_type",
            "Mode_of_Pay": "mode_of_pay",
            "Total_Advance_Amount": "total_advance_amt",
            "Advance_Amount": "advance_amt",
            "Remaining_Amount": "remaining_amt",
            "Total_Amount": "total_amt",
            "Status": "status",
            "Branch": "branch_code",
            "Credit_Card_Name": "credit_card_name",
            "Payment_Method": "payment_method",
            "MPesa_Code": "mpesa_code",
            "MPesa_Date": "mpesa_date",
            "MPesa_Amount": "mpesa_amt",
            "Insurance_Company_Name": "insurance_company_name",
            "Insurance_Scheme": "insurance_scheme",
            "Insurance_Membership_No": "insurance_membership_no",
            "Principal_Member_No": "principal_member_no",
            "Principal_Member_Name": "principal_member_name",
            "Web_Payment_No": "web_payment_no",
            "Insurance_Type": "insurance_type",
            "Insuracne_TotAprvAmt": "insurance_totl_apprv_amt",
            "Insuracne_FrameAprvAmt": "insurance_frame_apprv_amt",
            "Insuracne_LensAprvAmt": "insurance_lens_apprv_amt",
            "Insuracne_ActAprvAmt": "insurance_act_apprv_amt",
            "Draft_OrderNo": "draft_orderno",
        },
        inplace=True,
    )
    print("Columns renamed")

    if paymentsdf.empty:
        print("INFO! Payments dataframe is empty!")
    else:
        paymentsdf = paymentsdf.set_index(["doc_entry"])
        print("TRANSFORMATION! Adding new rows")

        upsert(
            engine=engine,
            df=paymentsdf,
            schema="mawingu_staging",
            table_name="source_payment",
            if_row_exists="update",
            create_table=False,
        )

        print("Update successful")
        print("payments have been fetched successfully")


# fetch_sap_payments()
