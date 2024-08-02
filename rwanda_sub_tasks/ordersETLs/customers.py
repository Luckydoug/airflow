import sys

sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
from datetime import date, timedelta
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
pd.set_option("display.max_columns", 100)
from sub_tasks.data.connect_voler import engine
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate


def fetch_sap_customers():
    SessionId = return_session_id(country="Rwanda")

    pagecount_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetBussinesPartners&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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

    customersdf = pd.DataFrame()
    payload = {}
    headers = {}
    for i in range(1, pages + 1):
        page = i
        url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetBussinesPartners&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request(
            "GET", url, headers=headers, data=payload, verify=False
        )
        customers = response.json()
        stripped_customers = customers["result"]["body"]["recs"]["Results"]
        customers_df = pd.DataFrame.from_dict(stripped_customers)
        customers_df2 = customers_df.T
        customersdf = customersdf.append(customers_df2, ignore_index=True)

    customersdf.rename(
        columns={
            "BP_Code": "cust_code",
            "BP_Type": "bp_type",
            "Group_Code": "cust_groupcode",
            "Sales_Employee_Code": "cust_sales_employeecode",
            "Bill_to_City": "cust_bill_to_city",
            "Bill_to_Country": "cust_bill_to_country",
            "Creation_Date": "cust_createdon",
            "Active": "cust_active_status",
            "Creatn_Time_Incl_Secs": "cust_createdat",
            "Date_of_Birth": "cust_dob",
            "Gender": "cust_gender",
            "Country": "cust_country",
            "Invoice_No": "cust_invoiceno",
            "Entering_reason": "cust_entering_reason",
            "Customer_Type": "cust_type",
            "Insurance_Company_Name": "cust_insurance_company",
            "Insurance_Scheme": "cust_insurance_scheme",
            "Campaign_Master": "cust_campaign_master",
            "Registration_Amount": "cust_reg_amt",
            "Payment_Status": "cust_payment_status",
            "Outlet": "cust_outlet",
            "Old_New_BusnsPartnr": "cust_old_new_bp",
            "Loyalty_OPT_IN": "cust_loyalty_optin",
            "Promotional_SMS": "cust_promo_sms",
            "Conversion Reason": "conversion_reason",
            "Conversion Remark": "conversion_remark",
        },
        inplace=True,
    )

    print("INFO! %d rows" % (len(customersdf)))

    customersdf = customersdf.drop_duplicates("cust_code")
    customersdf["cust_reg_amt"] = pd.to_numeric(customersdf["cust_reg_amt"])
    if customersdf.empty:
        print("INFO! customers dataframe is empty!")
    else:
        customersdf = customersdf.set_index(["cust_code"])
        print("INFO! Customer upsert started...")
        print(customersdf)

        upsert(
            engine=engine,
            df=customersdf,
            schema="voler_staging",
            table_name="source_customers",
            if_row_exists="update",
            create_table=False,
        )

        print("Update successful")


# fetch_sap_customers()
