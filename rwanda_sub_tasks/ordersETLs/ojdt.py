import sys

sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
from sub_tasks.data.connect_voler import engine
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate


def fetch_sap_ojdt():
    SessionId = return_session_id(country="Rwanda")

    # api details
    pagecount_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetJEinformation&pageNo=1&SessionId={SessionId}&FromDate={FromDate}&ToDate={ToDate}"
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
        url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetJEinformation&pageNo={page}&SessionId={SessionId}&FromDate={FromDate}&ToDate={ToDate}"
        response = requests.request(
            "GET", url, headers=headers, data=payload, verify=False
        )
        response = response.json()
        response = response["result"]["body"]["recs"]["Results"]
        response = pd.DataFrame.from_dict(response)
        response = response.T

        df = df.append(response, ignore_index=True)

    print("INFO! %d rows" % (len(df)))

    print("Replace successful")

    df.rename(
        columns={
            "Journal Voucher No.": "journal_voucher_no",
            "Status": "status",
            "Origin": "origin",
            "Origin No.": "origin_no",
            "Posting Date": "posting_date",
            "Remarks": "remarks",
            "Reference 1": "reference_1",
            "Reference 2": "reference_2",
            "Due Date": "due_date",
            "DocumentDate": "document_date",
            "Date of Update": "date_of_update",
            "Recording Date": "recording_date",
            "Updating User": "updating_user",
            "Number": "number",
            "Generation Time": "generation_time",
            "Reference 3": "reference_3",
            "Order Entry": "order_entry",
            "Order No": "order_no",
            "JE Type": "je_type",
            "Web Payment Entry": "web_payment_entry",
            "Draft Payment Entry": "draft_payment_entry",
            "Draft Order No": "draft_order_no",
            "JE Status": "je_status",
            "Invoice Branch": "invoice_branch",
            "Insurense Company": "insurance_company",
            "JE Remarks": "je_remarks",
            "POD Date": "pod_date",
            "Invoice Customer Code": "invoice_customer_code",
            "Invoice Customer Name": "invoice_customer_name",
            "Insurense Scheme Name": "insurance_scheme_name",
            "Insurense Scheme Type": "insurance_scheme_type",
            "Rejected Reasons": "rejected_reasons",
            "Credit Note No": "credit_note_no",
            "POD No": "pod_no",
            "BRANCH": "branch",
            "Expense No": "expense_no",
            "Invoice JE No": "invoice_je_no",
            "VAT JE No": "vat_je_no",
            "POD Entry": "pod_entry",
            "To Be PaidBy": "to_be_paid_by",
            "PaymentToBeDone": "payment_to_be_done",
            "PaymentReceivedDate": "payment_received_date",
            "Old DraftOrder Entry": "old_draftorder_entry",
            "Old DraftOrder No": "old_draftorder_no",
            "Old SalesOrder No": "old_salesorder_no",
            "Cancelled  Insurance JE No": "cancelled__insurance_je_no",
            "Cancelled  VAT JE No": "cancelled__vat_je_no",
            "Cancelled VAT JE Status": "cancelled_vat_je_status",
            "Vat Reversal JE": "vat_reversal_je",
            "Principal Member Name": "principal_member_name",
            "Principal Membership No": "principal_membership_no",
            "Advance VAT Reversed": "advance_vat_reversed",
            "Posted from": "posted_from",
            "Cancelled By": "cancelled_by",
            "Advanced Payment": "advanced_payment",
            "Advance VAT Reversed Num": "advance_vat_reversed_num",
            "Advance VAT Num": "advance_vat_num",
            "Incoming payment number": "incoming_payment_number",
            "Invoice number": "invoice_number",
            "Create Date": "create_date",
            "Reverse Date": "reverse_date",
            "Customer Code": "customer_code",
        },
        inplace=True,
    )

    print("Header Columns Renamed")

    df.columns = df.columns.str.replace("^\s+|\s+$", "")
    print("stripped")
    df.columns = df.columns.str.replace("[#,@,&,%,(,)]", "_")

    date_cols = [
        "posting_date",
        "due_date",
        "document_date",
        "date_of_update",
        "recording_date",
        "payment_received_date",
        "create_date",
    ]
    print("date_cols")
    print(df)

    # df[date_cols] = df[date_cols].apply(pd.to_datetime, yearfirst=True)
    df.columns = df.columns.str.replace("^\s+|\s+$", "")
    print(df.columns)
    print("Date Column Types Changed")

    if df.empty:
        print("INFO! OJDT dataframe is empty!")

    else:
        df = df.set_index(["origin_no"])
        print("TRANSFORMATION! Adding new rows")

        upsert(
            engine=engine,
            df=df,
            schema="voler_staging",
            table_name="source_ojdt",
            if_row_exists="update",
            create_table=False,
        )

        print("Update successful")


# fetch_sap_ojdt()
