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


def fetch_prescriptions():
    SessionId = return_session_id(country="Rwanda")

    # api details
    pagecount_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetPrescriptionDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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

    print("Retrived Number of Pages")
    print(pages)

    headers = {}
    payload = {}

    prescription_details = pd.DataFrame()
    prescription_c1 = pd.DataFrame()

    for i in range(1, pages + 1):
        page = i
        print(page)
        url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetPrescriptionDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request(
            "GET", url, headers=headers, data=payload, verify=False
        )
        response = response.json()

        try:
            response = response["result"]["body"]["recs"]["Results"]
            details = pd.DataFrame([d["details"] for d in response])
            c1 = pd.DataFrame(
                [
                    {"id": k, **v}
                    for d in response
                    for k, v in d["VSP_OPT_PRES_C1"].items()
                ]
            )
            prescription_details = prescription_details.append(
                details, ignore_index=True
            )
            prescription_c1 = prescription_c1.append(c1, ignore_index=True)
        except:
            print("Error")

    """
    PRESCRIPTION DETAILS
    """
    # rename columns
    prescription_details.rename(
        columns={
            "Code": "code",
            "DocEntry": "doc_entry",
            "UserSign": "user_sign",
            "CreateDate": "create_date",
            "CreateTime": "create_time",
            "UpdateDate": "update_date",
            "UpdateTime": "update_time",
            "TypeofLenses": "type_of_lenses",
            "Print": "print",
            "Order": "order",
            "PrintCount": "print_count",
            "PrintDate": "print_date",
            "PrintAmount": "print_amt",
            "PrintAdvanceAmt": "print_advance_amt",
            "Optom": "optom",
            "PFRX": "pfrx",
            "Old_OutsideRx": "old_outside_rx",
            "BPAdvAmt": "bp_adv_amt",
            "BPStatus": "bp_status",
            "CLAmountwithTax": "cl_amt_with_tax",
            "LatestUpdatedOpthomId": "latest_updated_optomid",
            "RXType": "rx_type",
            "Branch": "branch_code",
            "CustomerCode": "cust_code",
            "Status": "status",
            "RoutineEyeExamination": "routine_eye_exam",
            "SecondOpinion": "second_opinion",
            "RedEyes": "red_eyes",
            "BlurredNearVision": "blurred_vision",
            "Headache": "headache",
            "LightSensitivity": "light_sensitivity",
            "GoodHealth": "good_health",
            "EyeTearing": "eye_tearing",
            "ItchyEyes": "itchy_eyes",
            "ComputerVisionSyndrome": "computer_vision_syndrome",
            "OtherRemarks_ORMKS": "other_remarks_ormks",
            "GlaucomaDiabetes": "glaucoma_diabetes",
            "OtherRemarks": "other_remarks",
            "GlaUnderMedica": "gla_under_medica",
            "Glaucoma": "glaucoma",
            "GlaUnderMedica1": "gla_under_medical",
            "GlaucomaReamrks": "glaucoma_remarks",
            "Hyper": "hyper",
            "OtherRemarks_DRK": "other_remarks_drk",
            "Other": "other",
            "Remarks": "remarks",
            "Hypertension": "hypertension",
            "RemarksHyper": "remarks_hyper",
            "RemarksDiab": "remarks_diab",
            "Reamrks2": "remarks_2",
            "Others": "others",
            "OtherReason": "other_reason",
            "MostlyOutndoors": "mostly_outdoors",
            "ComputerUsage": "computer_usage",
            "Sports": "sports",
            "Occupation": "occupation",
            "ExcessNearWork": "excess_near_work",
            "PollutedDustyEnvironments": "polluted_dusty_environments",
            "RemarksOpenField": "remarks_open_field",
            "CustCrntlyWearingSpct": "cust_currntly_wearing_specs",
            "IOLEye": "iol_eye",
            "Laser": "laser",
            "IsOcularApplicable": "is_ocular_applicable",
            "IOL": "iol",
            "EyePain": "eye_pain",
            "OldRxSameasLast": "old_rx_sameaslast",
            "OldrxDV": "old_rx_dv",
            "OldRxNV": "old_rx_nv",
            "Bifocal": "bifocal",
            "Progressive": "progressive",
            "OldRxRE_DV_Sph": "old_rxre_dv_sph",
            "OldRxRE_DV_Cyl": "old_rxre_dv_cyl",
            "OldRxRE_DV_Axi": "old_rxre_dv_axi",
            "OldRxRE_DV_VA": "old_rxre_dv_va",
            "OldRxRE_DV_AD": "old_rxre_dv_ad",
            "OldRxRE_NV_Sph": "old_rxre_nv_sph",
            "OldRxRE_NV_Cyl": "old_rxre_nv_cyl",
            "OldRxRE_NV_Axi": "old_rxre_nv_axi",
            "OldRxRE_NV_VA": "old_rxre_nv_va",
            "OldRxRE_NV_AD": "old_rxre_nv_ad",
            "OldRxLE_DV_Sph": "old_rxle_dv_sph",
            "OldRxLE_DV_Cyl": "old_rxle_dv_cyl",
            "OldRxLE_DV_Axi": "old_rxle_dv_axi",
            "OldRxLE_DV_VA": "old_rxle_dv_va",
            "OldRxLE_DV_AD": "old_rxle_dv_ad",
            "OldRxLE_NV_Sph": "old_rxle_nv_sph",
            "OldRxLE_NV_Cyl": "old_rxle_nv_cyl",
            "OldRxLE_NV_Axi": "old_rxle_nv_axi",
            "OldRxLE_NV_VA": "old_rxle_nv_va",
            "OldRxLE_NV_AD": "old_rxle_nv_ad",
            "WhereisRxfrom": "where_isrx_from",
            "OldRxDate": "old_rx_date",
            "PFRX1": "pf_rx_1",
            "PGPDVCHK": "pgpdvchk",
            "PGPNVCHK": "pgpnvchk",
            "PGPDV_NVCHK": "pgpdv_nvchk",
            "Progressive1": "progressive1",
            "HospitalName": "hospital_name",
            "PD": "pd",
            "Ophthalmologist": "Opthalmologist",
            "Recommendations": "Recommendations",
            "PGPDate": "PGP_date",
            "PREDVSpch": "predvspch",
            "PREDVCycl": "predvcycl",
            "PREDVAxis": "predvaxis",
            "PREDVVA": "predvva",
            "PREDV_RE_RX_Add": "predv_re_rx_add",
            "PRENVSpch": "prenvspch",
            "PRENVCycl": "prenvcycl",
            "PRENVAxis": "prenvaxis",
            "PRENVVA": "prenvva",
            "PLEDVSpch": "pledvspch",
            "PLEDVCycl": "pledvcycl",
            "PLEDVAxis": "pledvaxis",
            "PLEDVVA": "pledvva",
            "PREDV_LE_RX_Add": "predv_le_rx_add",
            "PLENVSpch": "plenvspch",
            "PLENVCycl": "plenvcycl",
            "PLENVVA": "plenvva",
            "PRENV_LE_RX_Add_NV": "prenv_le_rx_add_nv",
            "Glucoma": "glucoma",
            "EyeDropDown": "eye_drop_down",
            "GlucomaRemakrs": "glucoma_remarks",
            "OpticaEyeTest": "optica_eyetest",
            "BloodPressure": "blood_pressure",
            "BloodSugar": "blood_sugar",
            "TestDone": "test_done",
            "EyeNear": "eye_near",
            "EyeDst": "eye_dst",
            "MonoRigtEye": "mono_rigt_eye",
            "MonoLeftEye": "mono_left_eye",
            "NearVisionReadingAt": "near_vision_readingat",
            "Mar": "mar",
            "HiIndex": "hiindex",
            "Standard": "standard",
            "BlockedDV": "blockeddv",
            "BlockedNV": "blockednv",
            "BlockedBF": "blockedbf",
            "PPFRX": "ppfrx",
            "OutsideRx": "outside_rx",
            "LastRx": "last_rx",
            "Bifocal_Progressive": "bifocal_progressive",
            "SphRE_DV_NVA": "sphre_dv_nva",
            "SREDVSpch": "sredvspch",
            "SREDVCycl": "sredvcycl",
            "SREDVAxis": "sredvaxis",
            "SREDVVA": "sredvva",
            "SphRE_DV_Add": "sphre_dv_add",
            "SphLE_DV_NVA": "sphle_dv_nva",
            "SLEDVSpch": "sledvspch",
            "SLEDVCycl": "sledvcycl",
            "SLEDVAxis": "sledvaxis",
            "SLEDVVA": "sledvva",
            "SphLE_DV_Add": "sphle_dv_add",
            "SphRE_NV_NVA": "sphre_nv_nva",
            "BothC": "bothc",
            "SphRE_NV_Add": "sphre_nv_add",
            "SphLE_NV_NVA": "sphle_nv_nva",
            "SLENV": "slenv",
            "BothG": "bothg",
            "SLENVVA": "slennvva",
            "SphLE_NV_Add": "sphle_nv_add",
            "ClinicRemarks": "clinic_remarks",
            "SalesPersonRMks": "slaesperson_rmks",
            "NeedCL": "needcl",
            "CLAmount": "cl_amt",
            "PatienttoOphth": "patient_to_ophth",
            "OphthNote": "opht_note",
            "BlurredDistanceVision": "blurred_distance_vision",
            "OphthName": "ophth_name",
            "TypeSoft": "typesoft",
            "Clear": "clear",
            "Crizol": "crizol",
            "Photo": "photo",
            "specrecommendations": "spec_recommendations",
            "RxSummerySelc": "rx_summery_selc",
            "RxStableUnstable": "rx_stable_unstable",
            "SalesEmployee": "sales_employee",
            "PDDistance": "pd_distance",
            "SalesEmployeeS": "sales_employees",
            "Reason": "reasson",
            "OptomRemarks": "optom_remarks",
            "SRE NV": "sre_nv",
            "NV in Cyl": "nv_in_cyl",
            "LastRx Prsc Prog": "lastrx_prsc_prog",
            "LastRx Prsc BF": "lastrx_prsc_bf",
            "U_VSPACTNO": "activity_no",
            "Conversion Reason": "conversion_reason",
            "Conversion Remark": "conversion_remarks",
        },
        inplace=True,
    )

    print("Details Columns Renamed")

    prescription_details = prescription_details.sort_values(
        "update_date"
    ).drop_duplicates("code", keep="last")

    # transformation
    prescription_details = prescription_details.set_index(["code"])

    # df to db
    upsert(
        engine=engine,
        df=prescription_details,
        schema="voler_staging",
        table_name="source_prescriptions",
        if_row_exists="update",
        create_table=False,
    )

    print("Prescriptions Inserted")

    """
    PRESCRIPTION C1
    """
    # rename columns
    prescription_c1.rename(
        columns={
            "Code": "code",
            "LineId": "line_id",
            "ViewTime": "view_time",
            "ViewDate": "view_date",
            "RequestedBy": "requestedby",
        },
        inplace=True,
    )

    print("Columns Renamed")

    # transformation
    prescription_c1 = prescription_c1.drop_duplicates(["code", "line_id"], keep="last")

    prescription_c1 = prescription_c1.set_index(["code", "line_id"])

    # df to db
    upsert(
        engine=engine,
        df=prescription_c1,
        schema="voler_staging",
        table_name="source_prescriptions_c1",
        if_row_exists="update",
        create_table=False,
    )

    print("Update Successful")


# fetch_prescriptions()
