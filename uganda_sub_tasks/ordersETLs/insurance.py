import sys

sys.path.append(".")
import requests
import pandas as pd
from pangres import upsert
from airflow.models import Variable
from sub_tasks.data.connect_mawingu import engine
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate


def fetch_sap_insurance():
    SessionId = return_session_id(country="Uganda")
    pagecount_url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetInsuranceSchemeDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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

    insurance_df = pd.DataFrame()
    payload = {}
    headers = {}
    for i in range(1, pages + 1):
        page = i
        url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetInsuranceSchemeDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request(
            "GET", url, headers=headers, data=payload, verify=False
        )
        insurance = response.json()
        stripped_insurance = insurance["result"]["body"]["recs"]["Results"]
        stripped_insurance_df = pd.DataFrame.from_dict(stripped_insurance)
        insurance_df = insurance_df.append(stripped_insurance_df, ignore_index=True)

    if insurance_df.empty:
        print("INFO! Items dataframe is empty!")
    else:

        """
        INSERT THE INSURANCE TABLE
        """
        insurance_details = insurance_df["details"].apply(pd.Series)

        print("INFO! %d rows" % (len(insurance_details)))

        insurance_details.rename(
            columns={
                "Code": "id",
                "DocEntry": "doc_entry",
                "UserSign": "user_signature",
                "CreateDate": "create_date",
                "CreateTime": "create_time",
                "UpdateDate": "update_date",
                "UpdateTime": "update_time",
                "Company_Code": "insurance_code",
                "ComapnyName": "insurance_name",
            },
            inplace=True,
        )

        insurance_details = insurance_details.set_index("id")

        print("TRANSFORMATION! Adding new insurance rows")

        upsert(
            engine=engine,
            df=insurance_details,
            schema="mawingu_staging",
            table_name="source_insurance_company",
            if_row_exists="update",
            create_table=False,
        )

        """
        INSERT THE INSURANCE PLAN TABLE
        """

        insurance_itemdetails = insurance_df["itemdetails"]
        insurance_itemdetails_df = insurance_itemdetails.to_frame()

        itemdetails_df = pd.DataFrame()
        for index, row in insurance_itemdetails_df.iterrows():
            row_data = row["itemdetails"]
            data = pd.DataFrame.from_dict(row_data)
            data1 = data.T
            itemdetails_df = itemdetails_df.append(data1, ignore_index=True)

        print("INFO! %d rows" % (len(itemdetails_df)))

        itemdetails_df.rename(
            columns={
                "Code": "insurance_id",
                "LineId": "plan_id",
                "Scheme_Name": "plan_scheme_name",
                "Start_Date": "start_date",
                "EndDate": "end_date",
                "Status": "plan_status",
                "Initial_Status_for_Insur": "plan_initial_status",
                "Scheme_Type": "plan_scheme_type",
                "Copay": "plan_copay",
                "CopayPercentage": "plan_copay_percent",
                "Opht_Pres_Required": "plan_opht_pre_required",
                "Total_Limit": "plan_limit",
                "Frame_Limit": "plan_frame_limit",
                "Lens_Limit": "plan_lens_limit",
                "Dependents_Covered": "plan_dependents",
                "Plano_Rx": "plan_rx",
                "PhotoTrans_Lenses": "plan_phototrans_lenses",
                "Anti_Glare": "plan_anti_glare",
                "Colored_CL": "plan_colored_cl",
                "Rx_CL": "plan_rx_cl",
                "Sunglasses": "plan_sunglasses",
                "Preauth_Validity": "plan_preauth_validity",
                "Approval_Validity": "plan_approval_validity",
                "Smart_Validity_period": "plan_smart_validity_period",
                "Out_Patient_Overall": "plan_out_patient",
                "Comments": "plan_comments",
                "PreAuth_mandatory": "plan_preauth_mandatory",
                "Claim_mandatory": "plan_claim_mandatory",
                "OutsideRx_mandatory": "plan_Outsiderx_mandatory",
                "MedicalId_mandatory": "plan_decalid_mandatory",
                "SMART_mandatory": "plan_smart_mandatory",
                "FeedBkForm_mandatory": "plan_feedbkform_mandatory",
                "WordDoc_mandatory": "plan_worddoc_mandatory",
                "HoldDays_BefAprvl": "plan_holddays_befoaprvl",
                "HoldDays_AftrAprvl": "plan_holddays_aftraprvl",
                "Check_Our_Limit": "plan_check_our_limits",
                "Scheme_Active": "plan_scheme_active",
                "Slade_Schemes": "plan_slade_schemes",
                "Approval_mandatory": "plan_approval_mandatory",
            },
            inplace=True,
        )

        itemdetails_df = itemdetails_df.set_index(["insurance_id", "plan_id"])

        print("TRANSFORMATION! Adding new insurance plan rows")

        upsert(
            engine=engine,
            df=itemdetails_df,
            schema="mawingu_staging",
            table_name="source_insurance_plan",
            if_row_exists="update",
            create_table=False,
            add_new_columns=True,
        )
