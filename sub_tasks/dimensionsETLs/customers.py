import sys

sys.path.append(".")
import requests
import pandas as pd
from datetime import date, timedelta
from airflow.models import Variable
from pangres import upsert
from sub_tasks.data.connect import pg_execute, engine
# from sub_tasks.api_login.api_login import login
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate

# FromDate = '2024/05/21'
# ToDate = '2024/05/21'


def fetch_sap_customers():
    SessionId = return_session_id(country="Kenya")
    # SessionId = login()

    pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetBussinesPartners&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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

    customersdf = pd.DataFrame()
    payload = {}
    headers = {}
    for i in range(1, pages + 1):
        page = i
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetBussinesPartners&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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
            "U_VSPCONRS": "conversion_reason",
            "U_VSPCONRE": "conversion_remark",
        },
        inplace=True,
    )

    customersdf = customersdf.drop_duplicates("cust_code")

    if customersdf.empty:
        print("INFO! customers dataframe is empty!")
    else:
        customersdf = customersdf.set_index(["cust_code"])

        upsert(
            engine=engine,
            df=customersdf,
            schema="mabawa_staging",
            table_name="source_customers",
            if_row_exists="update",
            create_table=False,
        )


def create_dim_customers():

    query = """
    truncate mabawa_dw.dim_customers;
    insert into mabawa_dw.dim_customers
    select cust_code, bp_type, cust_groupcode, cust_sales_employeecode, cust_bill_to_city, 
    cust_bill_to_country, cust_createdon, cust_createdat, cust_created_time, cust_active_status, 
    cust_dob, cust_age, cust_gender, cust_country, cust_invoiceno, cust_entering_reason, cust_type, 
    cust_insurance_company, cust_insurance_scheme, cust_campaign_master, cust_reg_amt, cust_payment_status, 
    cust_outlet, cust_old_new_bp, cust_loyalty_optin, cust_promo_sms,conversion_reason,conversion_remark
    from mabawa_dw.v_dim_customers;
    """

    query = pg_execute(query)


def create_reg_conv():

    query = """
    refresh materialized view mabawa_mviews.reg_conv;
    insert into mabawa_dw.update_log(table_name, update_time) values('reg_conv', default);
    """

    query = pg_execute(query)


# fetch_sap_customers()
# create_dim_customers()
# create_reg_conv()
