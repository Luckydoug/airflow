import sys

sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
from pandas.io.json._normalize import nested_to_record
from sub_tasks.data.connect_voler import pg_execute, engine
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate


def fetch_sap_orderscreendetails():
    SessionId = return_session_id(country="Rwanda")

    pagecount_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrderDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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
    print("data")
    orderscreendf = pd.DataFrame()
    payload = {}
    headers = {}
    pages = data["result"]["body"]["recs"]["PagesCount"]
    print("Retrieved Pages", pages)
    for i in range(1, pages + 1):
        page = i
        print(i)
        url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetOrderDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"

        response = requests.request(
            "GET", url, headers=headers, data=payload, verify=False
        )
        response = response.json()
        print("JSON")
        response = nested_to_record(response, sep="_")
        response = response["result_body_recs_Results"]
        response = pd.DataFrame.from_dict(response)
        print(response)
        response = pd.json_normalize(response["details"])
        orderscreendf = orderscreendf.append(response, ignore_index=True)
        print("Finished Fetching Data")

    orderscreendf.rename(
        columns={
            "DocEntry": "doc_entry",
            "DocNum": "doc_no",
            "UserSign": "user",
            "CreateDate": "ods_createdon",
            "CreateTime": "ods_createdat",
            "UpdateDate": "ods_updatedon",
            "UpdateTime": "ods_updatedat",
            "Creator": "ods_creator",
            "CustomerCode": "cust_code",
            "NormalOrRepairOrder": "ods_normal_repair_order",
            "PrescriptionNo": "presctiption_no",
            "Status": "ods_status",
            "Status_1": "ods_status1",
            "Outlet": "ods_outlet",
            "PostingDate": "ods_posting_date",
            "DueDate": "ods_due_date",
            "DocDate": "ods_doc_date",
            "OrderCancelledDate": "ods_order_canceldate",
            "OrderNo": "ods_orderno",
            "InvoiceNo": "ods_invoiceno",
            "RewardPoints": "ods_reward_points",
            "OrderCriteria": "ods_order_criteria",
            "PrescriptionType": "ods_prescription_type",
            "SaleType": "ods_salestype",
            "OrderType": "ods_ordertype",
            "ReOrder": "ods_reorder",
            "OrderNotoRecord": "ods_order_nottorecord",
            "MirrorCoating": "ods_mirrorcoating",
            "MirrorColor": "ods_mirrorcolor",
            "MirrorPrice": "ods_mirrorprice",
            "Tint": "ods_tint",
            "TintType": "ods_tint_type",
            "TintColor": "ods_tintcolor",
            "TintPrice": "ods_tintprice",
            "FrameType": "ods_frametype",
            "FrameMatrl": "ods_framematr1",
            "FrameBrand": "ods_framebrand",
            "FRModelNo": "ods_fr_modelno",
            "FRColourC": "ods_fr_colorc",
            "PFtoFollow": "ods_pf_tofollow",
            "LensMaterial": "ods_lensmaterial",
            "ITREntry": "ods_itrentry",
            "CutLens": "ods_cutlens",
            "LensReadyDt": "ods_lens_readydt",
            "LensReadyTm": "ods_lens_readytm",
            "AllGood": "ods_allgood",
            "ColourPeeling": "ods_colorpeeling",
            "Cracked_Broken": "ods_cracked_broken",
            "MissingPart": "ods_missingpart",
            "OtherPF": "ods_otherpf",
            "RemarksPF": "ods_remarkspf",
            "Scratched": "ods_scratched",
            "Cracked_Chipped": "ods_cracked_chipped",
            "CoatingPeeling": "ods_coating_peeling",
            "OtherPL": "ods_other_pl",
            "RemarksPLRight": "ods_remarks_plright",
            "Scratched2": "ods_scratched2",
            "Coating_Peeling": "ods_coating_peeling",
            "RemarksPLLeft": "ods_remarks_plleft",
            "MeasurementsBy": "ods_measurementsby",
            "SegmentHtRE": "ods_segment_htre",
            "SegmentHtLE": "ods_segment_htle",
            "ThickPDCntr": "ods_thickpdc_ntr",
            "ThickPHCntr": "ods_thickphc_ntr",
            "NseEdgeThickRht": "ods_nseedgethick_rht",
            "NseEdgeThickLft": "ods_nseedgethick_lft",
            "TmplEdgeThickRht": "ods_tmpledgethick_rht",
            "TmplEdgeThickLft": "ods_tmpledgethick_lft",
            "OtherThick": "ods_otherthick",
            "NeedCustmzdMst": "ods_needcustmzdmst",
            "Diamm": "ods_diamm",
            "BaseCurve": "ods_basecurve",
            "PrecalREPD": "ods_precal_repd",
            "PrecalREPH": "ods_precal_reph",
            "PrecalLEPD": "ods_precal_lepd",
            "PrecalLEPH": "ods_precal_leph",
            "IncHeightTop": "ods_incheighttop",
            "DecHeightTop": "ods_decheighttop",
            "IncWidthTemple": "ods_incwidthtemple",
            "DecWidthTemple": "ods_decwidthtemple",
            "IncHeightBottom": "ods_incheightbottom",
            "DecHeightBottom": "ods_decheightbottom",
            "IncWidthBridge": "ods_incwidthbridge",
            "DecWidthBridge": "ods_decwidthbridge",
            "AdjustFrame": "ods_adjustframe",
            "NoteAdjust": "ods_noteadjust",
            "WhatShpcopy": "ods_whatshpcopy",
            "WidthBox": "ods_widthbox",
            "HtBox": "ods_htbox",
            "Bridz": "ods_bridz",
            "DstDialg": "ods_dstdialg",
            "HeightAdj": "ods_heightadj",
            "LocalCollection": "ods_localcollection",
            "BrandNm": "ods_brandnm",
            "DeliveryDate": "ods_delivery_date",
            "DeliveryTime": "ods_delivery_time",
            "SMSAlternateNo": "ods_smsalternateno",
            "Optom": "ods_optom",
            "Area": "ods_area",
            "Street": "ods_street",
            "City": "ods_city",
            "Country": "ods_country",
            "DeliveryAmt": "ods_delivery_amt",
            "Urgent": "ods_urgent",
            "OrderInstruction": "ods_order_instruction",
            "FinalRxRE": "ods_final_rxre",
            "FinalRxLE": "ods_final_rxle",
            "FPRightDVSph": "ods_fprightdvsph",
            "FPLeftDVSph": "ods_fpleftdvsph",
            "FP_Right_DV_Cyl": "ods_fprightdvcyl",
            "FP_Left_DV_Cyl": "ods_fpleftdvcyl",
            "FP_Right_DV_Axis": "ods_fprightdvaxis",
            "FP_Left_DV_Axis": "ods_fpleftdvaxis",
            "R_DV_Add": "ods_rdvadd",
            "L_DV_Add": "ods_ldvadd",
            "R_NV_Add": "ods_rnvadd",
            "L_NV_Add": "ods_lnvadd",
            "R_IV_Add": "ods_rivadd",
            "L_IV_Add": "ods_livadd",
            "FP_Right_NV_Sph": "ods_fprightnvsph",
            "FP_Left_NV_Sph": "ods_fpleftnvsph",
            "FP_Right_NV_Cyl": "ods_fprightnvcyl",
            "FP_Left_NV_Cyl": "ods_fpleftnvcyl",
            "FP_Right_NV_Axis": "ods_fprightnvaxis",
            "FP_Left_NV_Axis": "ods_fpleftnvaxis",
            "FP_Right_IV_Sph": "ods_fprightivsph",
            "FP_Left_IV_Sph": "ods_fpleftivsph",
            "FP_Right_IV_Cyl": "ods_fprightivcyl",
            "FP_Left_IV_Cyl": "ods_fpleftivcyl",
            "FP_Right_IV_Axis": "ods_rightivaxis",
            "FP_Left_IV_Axis": "ods_fpleftivaxis",
            "Remarks": "ods_remarks",
            "Total_Before_Discount": "ods_total_before_discount",
            "Discount_Amount": "ods_discount_amt",
            "Tax_Amount": "ods_tax_amt",
            "Total_Amount": "ods_total_amt",
            "Advance_Payment": "ods_advance_payment",
            "Remaining_Payment": "ods_remaining_payment",
            "Prescription_Check": "ods_prescription_check",
            "OrdEntr_RorNo": "ods_orderentr_rorno",
            "OrderProcess_Branch": "ods_orderprocess_branch",
            "Insurance_Order": "ods_insurance_order",
            "Insuracne_TotAprvAmt1": "ods__insurance_totaprvamt1",
            "Insuracne_TotAprvAmt2": "ods_insurance_totaprvamt2",
            "Insuracne_FrameAprvAmt1": "ods_insurance_frameaprvamt1",
            "Insuracne_FrameAprvAmt2": "ods_insurance_frameaprvamt2",
            "Insuracne_LensAprvAmt1": "ods_insurance_lensaprvamt1",
            "Insuracne_LensAprvAmt2": "ods_insurance_lensaprvamt2",
            "Insuracne_ActAprvAmt1": "ods_insurance_actaprvamt1",
            "Insuracne_ActAprvAmt2": "ods__insurance_actaprvamt2",
            "Insuracne_PATotAprvAmt1": "ods_insurance_patotaprvamt1",
            "Insuracne_PATotAprvAmt2": "ods_insurance_patotaprvamt2",
            "Insuracne_PAFrameAprvAmt1": "ods_insurance_paframeaprvamt1",
            "Insuracne_PAFrameAprvAmt2": "ods_insurance_paframeaprvamt2",
            "Insuracne_PALensAprvAmt1": "ods_insurance_palensaprvamt1",
            "Insuracne_PALensAprvAmt2": "ods_palensaprvamt2",
            "Insuracne_PAActAprvAmt1": "ods_paactaprvamt1",
            "Insuracne_PAActAprvAmt2": "ods_paactaprvamt2",
            "Insuracne_SMTL1": "ods_insurance_smtl1",
            "Insuracne_SMTL2": "ods_insurance_smtl2",
            "Insuracne_SMFL1": "ods_insurance_smfl1",
            "Insuracne_SMFL2": "ods_insurance_smfl2",
            "Insuracne_SMLL1": "ods_insurance_smll1",
            "Insuracne_SMLL2": "ods_insurance_smll2",
            "Insurance_SmartUpdP1": "ods_insurance_smartupdp1",
            "Insurance_SmartUpdP2": "ods_insurance_smartupdp2",
            "Insurance_SmartCptP1": "ods_insurance_smartcptp1",
            "Insurance_SmartCptP2": "ods_insurance_smartcptp2",
            "CusCmng_SmarDT1": "ods_cuscmng_smartdt1",
            "CusCmng_SmarDT2": "ods_cuscmng_smartdt2",
            "InsPrAuth_AprvlDT1": "ods_insprauth_aprvldt1",
            "InsPrAuth_AprvlDT2": "ods_insprauth_aprvldt2",
            "InsPrAuth_AprvlTM1": "ods_insprauth_aprvltm1",
            "InsPrAuth_AprvlTM2": "ods_insprauth_aprvltm2",
            "Aprvl_Frame_RA1": "ods_aprvl_frame_ra1",
            "Aprvl_Frame_RA2": "ods_aprvl_frame_ra2",
            "Aprvl_Lens_RA1": "ods_aprvl_lens_ra1",
            "Aprvl_Lens_RA2": "ods_aprvl_lens_ra2",
            "Insurance_FeedBack1": "ods_insurance_feedback1",
            "Insurance_FeedBack2": "ods_insurance_feedback2",
            "Insurance_RejectRsn1": "ods_insurance_rejectrsn1",
            "Insurance_RejectRsn2": "ods_insurance_rejectrsn2",
            "Order_DashboardReasons": "ods_order_dashboard_reasons",
            "BLOT_No": "ods_blot_no",
            "BLOT_No_Date": "ods_blotno_date",
            "BLOT_SeqNo": "ods_blot_seqno",
            "FLOT_No": "ods_flotno",
            "FLOT_No_Date": "ods_flotno_date",
            "FLOT_SeqNo": "ods_flot_seqno",
            "ILOT_No": "ods_ilot_no",
            "ILOT_No_Date": "ods_ilotno_date",
            "ILOT_SeqNo": "ods_ilot_seqno",
            "LE_Blank_Lens": "ods_le_blank_lens",
            "RE_Blank_Lens": "ods_re_blank_lens",
            "Insurance_Status1": "ods_insurance_status1",
            "Insurance_Status2": "ods_insurance_status2",
            "NoofInsurances": "ods_noofinsurances",
            "OrderCriteriaStatus": "ods_ordercriteriastatus",
            "CustomizeMesrShape": "ods_customiemesrshape",
            "RepairBranch": "ods_repair_branch",
            "InsuranceSmartCprBrn1": "ods_insurance_smartcprbrn1",
            "InsuranceSmartCprBrn2": "ods_insurance_smartcprbrn2",
            "BLOTMissed_1": "ods_blot_missed1",
            "BLOTMissed_2": "ods_blot_missed2",
            "BLOTMissed_3": "ods_blot_missed3",
            "ActivityNo": "ods_activityno",
            "PackingBoxItem": "ods_packingboxitem",
            "ClothItem": "ods_clothitem",
            "Re_OrderUsed": "ods_re_order_used",
            "ShapeFrametoCopy": "ods_shapeframetocopy",
            "ReOrderDocEntry": "ods_reorder_docentry",
            "GiftVoucher": "ods_giftvoucher",
            "NaturePrice": "ods_natureprice",
            "PreQCTechnician": "ods_preqc_tech",
            "FinalQCTechnician": "ods_final_qc_tech",
            "Order Hours": "order_hours",
        },
        inplace=True,
    )

    # print(orderscreendf)

    print("Renamed Columns")

    query = """truncate table voler_staging.landing_orderscreen;"""
    query = pg_execute(query)

    print("Dropped Landing Table")

    orderscreendf.columns = orderscreendf.columns.str.replace("[#,@,&,%,(,)]", "_")
    orderscreendf.to_sql(
        "landing_orderscreen",
        con=engine,
        schema="voler_staging",
        if_exists="append",
        index=False,
    )


# fetch_sap_orderscreendetails()


def update_to_source_orderscreen():

    data = pd.read_sql(
        """
    select 
        *
    from 
    (SELECT 
        ROW_NUMBER() OVER (PARTITION BY doc_entry ORDER BY doc_entry) AS r,
        *
        FROM voler_staging.landing_orderscreen) as t
    where t.r = 1;
    """,
        con=engine,
    )

    data = data.set_index("doc_entry")

    upsert(
        engine=engine,
        df=data,
        schema="voler_staging",
        table_name="source_orderscreen",
        if_row_exists="update",
        create_table=False,
        add_new_columns=True,
    )

    print("update_to_source_orderscreen")


# update_to_source_orderscreen()
