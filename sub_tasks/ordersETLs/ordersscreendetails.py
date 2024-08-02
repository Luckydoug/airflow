import sys
sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from datetime import date, timedelta
from pangres import upsert
from pandas.io.json._normalize import nested_to_record 
from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate

# FromDate = '2024/07/01'
# ToDate = '2024/07/31'

def fetch_sap_orderscreendetails():
    SessionId = return_session_id(country = "Kenya")

    pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    # pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId=64d92d3e-c721-11ee-8000-00505685ba7e"
    pagecount_payload={}
    pagecount_headers = {}

    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    
    orderscreendf = pd.DataFrame()
    payload={}
    headers = {}
    # print(data)
    pages = data['result']['body']['recs']['PagesCount']
    for i in range(1, pages+1):
        page = i
        print(i)
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()
        response = nested_to_record(response, sep='_')
        # print(response)
        response= response['result_body_recs_Results']
        response = pd.DataFrame.from_dict(response)
        response = pd.json_normalize(response['details'])
        orderscreendf = orderscreendf.append(response, ignore_index=True)

    print("Finished Fetching Data")
    orderscreendf.rename (columns = {'DocEntry':'doc_entry', 
                       'DocNum':'doc_no', 
                       'UserSign': 'user',
                       'CreateDate':'ods_createdon', 
                       'CreateTime':'ods_createdat', 
                       'UpdateDate': 'ods_updatedon',
                       'UpdateTime':'ods_updatedat', 
                       'Creator':'ods_creator', 
                       'CustomerCode': 'cust_code',
                       'NormalOrRepairOrder':'ods_normal_repair_order', 
                       'PrescriptionNo':'presctiption_no', 
                       'Status':'ods_status', 
                       'Status_1': 'ods_status1',
                       'Outlet':'ods_outlet', 
                       'PostingDate':'ods_posting_date', 
                       'DueDate': 'ods_due_date',
                       'DocDate':'ods_doc_date', 
                       'OrderCancelledDate':'ods_order_canceldate',
                       'OrderNo':'ods_orderno', 
                       'InvoiceNo':'ods_invoiceno', 
                       'RewardPoints': 'ods_reward_points',
                       'OrderCriteria':'ods_order_criteria', 
                       'PrescriptionType':'ods_prescription_type', 
                       'SaleType': 'ods_salestype',
                       'OrderType':'ods_ordertype', 
                       'ReOrder':'ods_reorder', 
                       'OrderNotoRecord': 'ods_order_nottorecord',
                       'MirrorCoating':'ods_mirrorcoating', 
                       'MirrorColor':'ods_mirrorcolor', 
                       'MirrorPrice':'ods_mirrorprice', 
                       'Tint': 'ods_tint',
                       'TintType':'ods_tint_type', 
                       'TintColor':'ods_tintcolor', 
                       'TintPrice': 'ods_tintprice',
                       'FrameType':'ods_frametype', 
                       'FrameMatrl':'ods_framematr1',
                       'FrameBrand':'ods_framebrand', 
                       'FRModelNo':'ods_fr_modelno', 
                       'FRColourC':'ods_fr_colorc', 
                       'PFtoFollow':'ods_pf_tofollow', 
                       'LensMaterial':'ods_lensmaterial', 
                       'ITREntry':'ods_itrentry', 
                       'CutLens':'ods_cutlens', 
                       'LensReadyDt':'ods_lens_readydt', 
                       'LensReadyTm':'ods_lens_readytm', 
                       'AllGood':'ods_allgood', 
                       'ColourPeeling':'ods_colorpeeling', 
                       'Cracked_Broken':'ods_cracked_broken', 
                       'MissingPart':'ods_missingpart', 
                       'OtherPF':'ods_otherpf', 
                       'RemarksPF':'ods_remarkspf', 
                       'Scratched':'ods_scratched', 
                       'Cracked_Chipped':'ods_cracked_chipped', 
                       'CoatingPeeling':'ods_coating_peeling', 
                       'OtherPL':'ods_other_pl', 
                       'RemarksPLRight':'ods_remarks_plright', 
                       'Scratched2':'ods_scratched2', 
                       'Coating_Peeling':'ods_coating_peeling', 
                       'RemarksPLLeft':'ods_remarks_plleft', 
                       'MeasurementsBy':'ods_measurementsby', 
                       'SegmentHtRE':'ods_segment_htre', 
                       'SegmentHtLE':'ods_segment_htle', 
                       'ThickPDCntr':'ods_thickpdc_ntr', 
                       'ThickPHCntr':'ods_thickphc_ntr', 
                       'NseEdgeThickRht':'ods_nseedgethick_rht', 
                       'NseEdgeThickLft':'ods_nseedgethick_lft', 
                       'TmplEdgeThickRht':'ods_tmpledgethick_rht', 
                       'TmplEdgeThickLft':'ods_tmpledgethick_lft', 
                       'OtherThick':'ods_otherthick', 
                       'NeedCustmzdMst':'ods_needcustmzdmst', 
                       'Diamm':'ods_diamm', 
                       'BaseCurve':'ods_basecurve', 
                       'PrecalREPD':'ods_precal_repd', 
                       'PrecalREPH':'ods_precal_reph', 
                       'PrecalLEPD':'ods_precal_lepd', 
                       'PrecalLEPH':'ods_precal_leph', 
                       'IncHeightTop':'ods_incheighttop', 
                       'DecHeightTop':'ods_decheighttop', 
                       'IncWidthTemple':'ods_incwidthtemple', 
                       'DecWidthTemple':'ods_decwidthtemple', 
                       'IncHeightBottom':'ods_incheightbottom', 
                       'DecHeightBottom':'ods_decheightbottom', 
                       'IncWidthBridge':'ods_incwidthbridge', 
                       'DecWidthBridge':'ods_decwidthbridge', 
                       'AdjustFrame':'ods_adjustframe', 
                       'NoteAdjust':'ods_noteadjust', 
                       'WhatShpcopy':'ods_whatshpcopy', 
                       'WidthBox':'ods_widthbox', 
                       'HtBox':'ods_htbox', 
                       'Bridz':'ods_bridz', 
                       'DstDialg':'ods_dstdialg', 
                       'HeightAdj':'ods_heightadj', 
                       'LocalCollection':'ods_localcollection', 
                       'BrandNm':'ods_brandnm', 
                       'DeliveryDate':'ods_delivery_date', 
                       'DeliveryTime':'ods_delivery_time', 
                       'SMSAlternateNo':'ods_smsalternateno', 
                       'Optom':'ods_optom', 
                       'Area':'ods_area', 
                       'Street':'ods_street', 
                       'City':'ods_city', 
                       'Country':'ods_country', 
                       'DeliveryAmt':'ods_delivery_amt', 
                       'Urgent':'ods_urgent', 
                       'OrderInstruction':'ods_order_instruction', 
                       'FinalRxRE':'ods_final_rxre', 
                       'FinalRxLE':'ods_final_rxle', 
                       'FPRightDVSph':'ods_fprightdvsph', 
                       'FPLeftDVSph':'ods_fpleftdvsph', 
                       'FP_Right_DV_Cyl':'ods_fprightdvcyl', 
                       'FP_Left_DV_Cyl':'ods_fpleftdvcyl', 
                       'FP_Right_DV_Axis':'ods_fprightdvaxis', 
                       'FP_Left_DV_Axis':'ods_fpleftdvaxis', 
                       'R_DV_Add':'ods_rdvadd', 
                       'L_DV_Add':'ods_ldvadd', 
                       'R_NV_Add':'ods_rnvadd', 
                       'L_NV_Add':'ods_lnvadd', 
                       'R_IV_Add':'ods_rivadd', 
                       'L_IV_Add':'ods_livadd', 
                       'FP_Right_NV_Sph':'ods_fprightnvsph', 
                       'FP_Left_NV_Sph':'ods_fpleftnvsph', 
                       'FP_Right_NV_Cyl':'ods_fprightnvcyl', 
                       'FP_Left_NV_Cyl':'ods_fpleftnvcyl', 
                       'FP_Right_NV_Axis':'ods_fprightnvaxis', 
                       'FP_Left_NV_Axis':'ods_fpleftnvaxis', 
                       'FP_Right_IV_Sph':'ods_fprightivsph',
                       'FP_Left_IV_Sph':'ods_fpleftivsph', 
                       'FP_Right_IV_Cyl':'ods_fprightivcyl', 
                       'FP_Left_IV_Cyl':'ods_fpleftivcyl', 
                       'FP_Right_IV_Axis':'ods_rightivaxis', 
                       'FP_Left_IV_Axis':'ods_fpleftivaxis', 
                       'Remarks':'ods_remarks', 
                       'Total_Before_Discount':'ods_total_before_discount', 
                       'Discount_Amount':'ods_discount_amt', 
                       'Tax_Amount':'ods_tax_amt', 
                       'Total_Amount':'ods_total_amt', 
                       'Advance_Payment':'ods_advance_payment', 
                       'Remaining_Payment':'ods_remaining_payment', 
                       'Prescription_Check':'ods_prescription_check', 
                       'OrdEntr_RorNo':'ods_orderentr_rorno', 
                       'OrderProcess_Branch':'ods_orderprocess_branch', 
                       'Insurance_Order':'ods_insurance_order', 
                       'Insuracne_TotAprvAmt1':'ods__insurance_totaprvamt1', 
                       'Insuracne_TotAprvAmt2':'ods_insurance_totaprvamt2', 
                       'Insuracne_FrameAprvAmt1':'ods_insurance_frameaprvamt1', 
                       'Insuracne_FrameAprvAmt2':'ods_insurance_frameaprvamt2', 
                       'Insuracne_LensAprvAmt1':'ods_insurance_lensaprvamt1', 
                       'Insuracne_LensAprvAmt2':'ods_insurance_lensaprvamt2', 
                       'Insuracne_ActAprvAmt1':'ods_insurance_actaprvamt1', 
                       'Insuracne_ActAprvAmt2':'ods__insurance_actaprvamt2', 
                       'Insuracne_PATotAprvAmt1':'ods_insurance_patotaprvamt1', 
                       'Insuracne_PATotAprvAmt2':'ods_insurance_patotaprvamt2', 
                       'Insuracne_PAFrameAprvAmt1':'ods_insurance_paframeaprvamt1', 
                       'Insuracne_PAFrameAprvAmt2':'ods_insurance_paframeaprvamt2', 
                       'Insuracne_PALensAprvAmt1':'ods_insurance_palensaprvamt1', 
                       'Insuracne_PALensAprvAmt2':'ods_palensaprvamt2', 
                       'Insuracne_PAActAprvAmt1':'ods_paactaprvamt1', 
                       'Insuracne_PAActAprvAmt2':'ods_paactaprvamt2', 
                       'Insuracne_SMTL1':'ods_insurance_smtl1', 
                       'Insuracne_SMTL2':'ods_insurance_smtl2', 
                       'Insuracne_SMFL1':'ods_insurance_smfl1', 
                       'Insuracne_SMFL2':'ods_insurance_smfl2', 
                       'Insuracne_SMLL1':'ods_insurance_smll1', 
                       'Insuracne_SMLL2':'ods_insurance_smll2', 
                       'Insurance_SmartUpdP1':'ods_insurance_smartupdp1', 
                       'Insurance_SmartUpdP2':'ods_insurance_smartupdp2', 
                       'Insurance_SmartCptP1':'ods_insurance_smartcptp1', 
                       'Insurance_SmartCptP2':'ods_insurance_smartcptp2', 
                       'CusCmng_SmarDT1':'ods_cuscmng_smartdt1', 
                       'CusCmng_SmarDT2':'ods_cuscmng_smartdt2', 
                       'InsPrAuth_AprvlDT1':'ods_insprauth_aprvldt1', 
                       'InsPrAuth_AprvlDT2':'ods_insprauth_aprvldt2', 
                       'InsPrAuth_AprvlTM1':'ods_insprauth_aprvltm1', 
                       'InsPrAuth_AprvlTM2':'ods_insprauth_aprvltm2', 
                       'Aprvl_Frame_RA1':'ods_aprvl_frame_ra1', 
                       'Aprvl_Frame_RA2':'ods_aprvl_frame_ra2', 
                       'Aprvl_Lens_RA1':'ods_aprvl_lens_ra1', 
                       'Aprvl_Lens_RA2':'ods_aprvl_lens_ra2', 
                       'Insurance_FeedBack1':'ods_insurance_feedback1', 
                       'Insurance_FeedBack2':'ods_insurance_feedback2', 
                       'Insurance_RejectRsn1':'ods_insurance_rejectrsn1', 
                       'Insurance_RejectRsn2':'ods_insurance_rejectrsn2', 
                       'Order_DashboardReasons':'ods_order_dashboard_reasons', 
                       'BLOT_No':'ods_blot_no', 
                       'BLOT_No_Date':'ods_blotno_date', 
                       'BLOT_SeqNo':'ods_blot_seqno', 
                       'FLOT_No':'ods_flotno', 
                       'FLOT_No_Date':'ods_flotno_date', 
                       'FLOT_SeqNo':'ods_flot_seqno', 
                       'ILOT_No':'ods_ilot_no', 
                       'ILOT_No_Date':'ods_ilotno_date', 
                       'ILOT_SeqNo':'ods_ilot_seqno', 
                       'LE_Blank_Lens':'ods_le_blank_lens', 
                       'RE_Blank_Lens':'ods_re_blank_lens', 
                       'Insurance_Status1':'ods_insurance_status1', 
                       'Insurance_Status2':'ods_insurance_status2', 
                       'NoofInsurances':'ods_noofinsurances', 
                       'OrderCriteriaStatus':'ods_ordercriteriastatus', 
                       'CustomizeMesrShape':'ods_customiemesrshape',
                       'RepairBranch':'ods_repair_branch', 
                       'InsuranceSmartCprBrn1':'ods_insurance_smartcprbrn1', 
                       'InsuranceSmartCprBrn2':'ods_insurance_smartcprbrn2', 
                       'BLOTMissed_1':'ods_blot_missed1', 
                       'BLOTMissed_2':'ods_blot_missed2', 
                       'BLOTMissed_3':'ods_blot_missed3', 
                       'ActivityNo':'ods_activityno', 
                       'PackingBoxItem':'ods_packingboxitem', 
                       'ClothItem':'ods_clothitem', 
                       'Re_OrderUsed':'ods_re_order_used', 
                       'ShapeFrametoCopy':'ods_shapeframetocopy', 
                       'ReOrderDocEntry':'ods_reorder_docentry', 
                       'GiftVoucher':'ods_giftvoucher', 
                       'NaturePrice':'ods_natureprice', 
                       'PreQCTechnician':'ods_preqc_tech', 
                       'FinalQCTechnician':'ods_final_qc_tech',
                       'Order Hours':'order_hours'}
            ,inplace=True)
    
    print("Renamed Columns")

    query = """truncate table mabawa_staging.landing_orderscreen;"""
    query = pg_execute(query)

    print("Dropped Landing Table")

    orderscreendf.columns = orderscreendf.columns.str.replace('[#,@,&,%,(,)]', '_')

    orderscreendf.to_sql('landing_orderscreen', con=engine, schema='mabawa_staging', if_exists = 'append', index=False)
    
    print('fetch_sap_orderscreendetails')



def update_to_source_orderscreen():

    data = pd.read_sql("""
    select 
        *
    from 
    (SELECT 
        ROW_NUMBER() OVER (PARTITION BY doc_entry ORDER BY doc_entry) AS r,
        *
        FROM mabawa_staging.landing_orderscreen) as t
    where t.r = 1;
    """, con=engine)
    
    #data = fix_psycopg2_bad_cols(data)

    data = data.set_index('doc_entry')

    upsert(engine=engine,
       df=data,
       schema='mabawa_staging',
       table_name='source_orderscreen',
       if_row_exists='update',
       create_table=False,
       add_new_columns=True)
    
    print('update_to_source_orderscreen')



def create_source_orderscreen_staging():

    query = """
    truncate mabawa_staging.source_orderscreen_staging;
    insert into mabawa_staging.source_orderscreen_staging
    SELECT doc_entry, doc_no, ods_userid, ods_createdon, ods_createdat_int, ods_createdat, ods_updatedon, ods_updatedat_int, ods_creator, cust_code, ods_normal_repair_order, presctiption_no, ods_status, ods_status1, ods_outlet, ods_posting_date, ods_due_date, ods_doc_date, ods_order_canceldate, ods_orderno, ods_invoiceno, ods_reward_points, ods_order_criteria, ods_prescription_type, ods_salestype, ods_ordertype, ods_reorder, ods_order_nottorecord, ods_mirrorcoating, ods_mirrorcolor, ods_mirrorprice, ods_tint, ods_tint_type, ods_tintcolor, ods_tintprice, ods_frametype, ods_framematr1, ods_framebrand, ods_fr_modelno, ods_fr_colorc, ods_pf_tofollow, ods_lensmaterial, ods_itrentry, ods_cutlens, ods_lens_readydt, ods_lens_readytm, ods_allgood, ods_colorpeeling, ods_cracked_broken, ods_missingpart, ods_otherpf, ods_remarkspf, ods_scratched, ods_cracked_chipped, ods_coating_peeling, ods_other_pl, ods_remarks_plright, ods_scratched2, ods_remarks_plleft, ods_measurementsby, ods_segment_htre, ods_segment_htle, ods_thickpdc_ntr, ods_thickphc_ntr, ods_nseedgethick_rht, ods_nseedgethick_lft, ods_tmpledgethick_rht, ods_tmpledgethick_lft, ods_otherthick, ods_needcustmzdmst, ods_diamm, ods_basecurve, ods_precal_repd, ods_precal_reph, ods_precal_lepd, ods_precal_leph, ods_incheighttop, ods_decheighttop, ods_incwidthtemple, ods_decwidthtemple, ods_incheightbottom, ods_decheightbottom, ods_incwidthbridge, ods_decwidthbridge, ods_adjustframe, ods_noteadjust, ods_whatshpcopy, ods_widthbox, ods_htbox, ods_bridz, ods_dstdialg, ods_heightadj, ods_localcollection, ods_brandnm, ods_delivery_date, ods_delivery_time, ods_smsalternateno, ods_optom, ods_area, ods_street, ods_city, ods_country, ods_delivery_amt, ods_urgent, ods_order_instruction, ods_final_rxre, ods_final_rxle, ods_fprightdvsph, ods_fpleftdvsph, ods_fprightdvcyl, ods_fpleftdvcyl, ods_fprightdvaxis, ods_fpleftdvaxis, ods_rdvadd, ods_ldvadd, ods_rnvadd, ods_lnvadd, ods_rivadd, ods_livadd, ods_fprightnvsph, ods_fpleftnvsph, ods_fprightnvcyl, ods_fpleftnvcyl, ods_fprightnvaxis, ods_fpleftnvaxis, ods_fprightivsph, ods_fpleftivsph, ods_fprightivcyl, ods_fpleftivcyl, ods_rightivaxis, ods_fpleftivaxis, ods_remarks, ods_total_before_discount, ods_discount_amt, ods_tax_amt, ods_total_amt, ods_advance_payment, ods_remaining_payment, ods_prescription_check, ods_orderentr_rorno, ods_orderprocess_branch, ods_insurance_order, ods__insurance_totaprvamt1, ods_insurance_totaprvamt2, ods_insurance_frameaprvamt1, ods_insurance_frameaprvamt2, ods_insurance_lensaprvamt1, ods_insurance_lensaprvamt2, ods_insurance_actaprvamt1, ods__insurance_actaprvamt2, ods_insurance_patotaprvamt1, ods_insurance_patotaprvamt2, ods_insurance_paframeaprvamt1, ods_insurance_paframeaprvamt2, ods_insurance_palensaprvamt1, ods_palensaprvamt2, ods_paactaprvamt1, ods_paactaprvamt2, ods_insurance_smtl1, ods_insurance_smtl2, ods_insurance_smfl1, ods_insurance_smfl2, ods_insurance_smll1, ods_insurance_smll2, ods_insurance_smartupdp1, ods_insurance_smartupdp2, ods_insurance_smartcptp1, ods_insurance_smartcptp2, ods_cuscmng_smartdt1, ods_cuscmng_smartdt2, ods_insprauth_aprvldt1, ods_insprauth_aprvldt2, ods_insprauth_aprvltm1, ods_insprauth_aprvltm2, ods_aprvl_frame_ra1, ods_aprvl_frame_ra2, ods_aprvl_lens_ra1, ods_aprvl_lens_ra2, ods_insurance_feedback1, ods_insurance_feedback2, ods_insurance_rejectrsn1, ods_insurance_rejectrsn2, ods_order_dashboard_reasons, ods_blot_no, ods_blotno_date, ods_blot_seqno, ods_flotno, ods_flotno_date, ods_flot_seqno, ods_ilot_no, ods_ilotno_date, ods_ilot_seqno, ods_le_blank_lens, ods_re_blank_lens, ods_insurance_status1, ods_insurance_status2, ods_noofinsurances, ods_ordercriteriastatus, ods_customiemesrshape, ods_repair_branch, ods_insurance_smartcprbrn1, ods_insurance_smartcprbrn2, ods_blot_missed1, ods_blot_missed2, ods_blot_missed3, ods_activityno, ods_packingboxitem, ods_clothitem, ods_re_order_used, ods_shapeframetocopy, ods_reorder_docentry, ods_giftvoucher, ods_natureprice, ods_preqc_tech, ods_final_qc_tech, "'itemdetailsC0'", "'itemdetailsC1'"
    FROM mabawa_staging.v_source_orderscreen_staging;

    """

    query = pg_execute(query)

    print('create_source_orderscreen_staging')

# create_source_orderscreen_staging()

def create_fact_orderscreen():

    query = """
    truncate mabawa_dw.fact_orderscreen;
    insert into mabawa_dw.fact_orderscreen
     SELECT 
        doc_entry, doc_no, "user" as ods_userid, 
        ods_createdon::date, 
        ods_createdat as ods_createdat_int, 
        case
 			when length(ods_createdat::text) in (1,2) then null
 		else
        	(left(ods_createdat::text,(length(ods_createdat::text)-2))||':'||right(ods_createdat::text, 2))::time 
        end 
        as ods_createdat, 
        ods_updatedon::date, 
        ods_updatedat as ods_updatedat_int,
        --(left(ods_updatedat::text,(length(ods_updatedat::text)-2))||':'||right(ods_updatedat::text, 2))::time as ods_updatedat, 
        ods_creator, cust_code, ods_normal_repair_order, presctiption_no, ods_status, 
        ods_status1, ods_outlet, ods_posting_date::date, ods_due_date::date, ods_doc_date, ods_order_canceldate, 
        ods_orderno, ods_invoiceno, ods_reward_points, ods_order_criteria, ods_prescription_type, ods_salestype, 
        ods_ordertype, ods_reorder, ods_order_nottorecord, ods_mirrorcoating, ods_mirrorcolor, ods_mirrorprice, ods_tint, 
        ods_tint_type, ods_tintcolor, ods_tintprice, ods_frametype, ods_framematr1, ods_framebrand, ods_fr_modelno, ods_fr_colorc, 
        ods_pf_tofollow, ods_lensmaterial, ods_itrentry, ods_cutlens, ods_lens_readydt, ods_lens_readytm, ods_allgood, 
        ods_colorpeeling, ods_cracked_broken, ods_missingpart, ods_otherpf, ods_remarkspf, ods_scratched, ods_cracked_chipped, 
        ods_coating_peeling, ods_other_pl, ods_remarks_plright, ods_scratched2, ods_remarks_plleft, ods_measurementsby, 
        ods_segment_htre, ods_segment_htle, ods_thickpdc_ntr, ods_thickphc_ntr, ods_nseedgethick_rht, ods_nseedgethick_lft, 
        ods_tmpledgethick_rht, ods_tmpledgethick_lft, ods_otherthick, ods_needcustmzdmst, ods_diamm, ods_basecurve, 
        ods_precal_repd, ods_precal_reph, ods_precal_lepd, ods_precal_leph, ods_incheighttop, ods_decheighttop, 
        ods_incwidthtemple, ods_decwidthtemple, ods_incheightbottom, ods_decheightbottom, ods_incwidthbridge, 
        ods_decwidthbridge, ods_adjustframe, ods_noteadjust, ods_whatshpcopy, ods_widthbox, ods_htbox, ods_bridz, 
        ods_dstdialg, ods_heightadj, ods_localcollection, ods_brandnm, ods_delivery_date::date, ods_delivery_time, 
        ods_smsalternateno, ods_optom, ods_area, ods_street, ods_city, ods_country, ods_delivery_amt, ods_urgent, 
        ods_order_instruction, ods_final_rxre, ods_final_rxle, ods_fprightdvsph, ods_fpleftdvsph, ods_fprightdvcyl, 
        ods_fpleftdvcyl, ods_fprightdvaxis, ods_fpleftdvaxis, ods_rdvadd, ods_ldvadd, ods_rnvadd, ods_lnvadd, ods_rivadd, 
        ods_livadd, ods_fprightnvsph, ods_fpleftnvsph, ods_fprightnvcyl, ods_fpleftnvcyl, ods_fprightnvaxis, ods_fpleftnvaxis, 
        ods_fprightivsph, ods_fpleftivsph, ods_fprightivcyl, ods_fpleftivcyl, ods_rightivaxis, ods_fpleftivaxis, ods_remarks, 
        ods_total_before_discount, ods_discount_amt, ods_tax_amt, ods_total_amt, ods_advance_payment, ods_remaining_payment, 
        ods_prescription_check, ods_orderentr_rorno, ods_orderprocess_branch, ods_insurance_order, ods__insurance_totaprvamt1, 
        ods_insurance_totaprvamt2, ods_insurance_frameaprvamt1, ods_insurance_frameaprvamt2, ods_insurance_lensaprvamt1, 
        ods_insurance_lensaprvamt2, ods_insurance_actaprvamt1, ods__insurance_actaprvamt2, ods_insurance_patotaprvamt1, 
        ods_insurance_patotaprvamt2, ods_insurance_paframeaprvamt1, ods_insurance_paframeaprvamt2, ods_insurance_palensaprvamt1, 
        ods_palensaprvamt2, ods_paactaprvamt1, ods_paactaprvamt2, ods_insurance_smtl1, ods_insurance_smtl2, ods_insurance_smfl1, 
        ods_insurance_smfl2, ods_insurance_smll1, ods_insurance_smll2, ods_insurance_smartupdp1, ods_insurance_smartupdp2, 
        ods_insurance_smartcptp1, ods_insurance_smartcptp2, ods_cuscmng_smartdt1, ods_cuscmng_smartdt2, ods_insprauth_aprvldt1, 
        ods_insprauth_aprvldt2, ods_insprauth_aprvltm1, ods_insprauth_aprvltm2, ods_aprvl_frame_ra1, ods_aprvl_frame_ra2, 
        ods_aprvl_lens_ra1, ods_aprvl_lens_ra2, ods_insurance_feedback1, ods_insurance_feedback2, ods_insurance_rejectrsn1, 
        ods_insurance_rejectrsn2, ods_order_dashboard_reasons, ods_blot_no, ods_blotno_date, ods_blot_seqno, ods_flotno, 
        ods_flotno_date, ods_flot_seqno, ods_ilot_no, ods_ilotno_date, ods_ilot_seqno, ods_le_blank_lens, ods_re_blank_lens, 
        ods_insurance_status1, ods_insurance_status2, ods_noofinsurances, ods_ordercriteriastatus, ods_customiemesrshape, 
        ods_repair_branch, ods_insurance_smartcprbrn1, ods_insurance_smartcprbrn2, ods_blot_missed1, ods_blot_missed2, 
        ods_blot_missed3, ods_activityno, ods_packingboxitem, ods_clothitem, ods_re_order_used, ods_shapeframetocopy, 
        ods_reorder_docentry, ods_giftvoucher, ods_natureprice, ods_preqc_tech, ods_final_qc_tech, "'itemdetailsC0'", "'itemdetailsC1'"
    FROM mabawa_staging.source_orderscreen;

    """

    query = pg_execute(query)
    
    print('create_fact_orderscreen')

# fetch_sap_orderscreendetails()
# update_to_source_orderscreen()

 