import sys
sys.path.append(".")
import requests
import pandas as pd
from datetime import date, timedelta
from airflow.models import variable
from pangres import upsert
from sub_tasks.data.connect import (pg_execute, engine)
# from sub_tasks.api_login.api_login import(login)
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate

def fetch_prescriptions():
    SessionId = return_session_id(country = "Kenya")

    pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetPrescriptionDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    pagecount_payload={}
    pagecount_headers = {}
    
    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    pages = data['result']['body']['recs']['PagesCount']

    print("Retrived Number of Pages")
    print(pages)

    headers = {}
    payload = {}

    prescription_details = pd.DataFrame()
    prescription_c1 = pd.DataFrame()

    for i in range(1, pages+1):
        page = i
        print(page)
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetPrescriptionDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response = response.json()

    # try:
        response = response['result']['body']['recs']['Results']
        details = pd.DataFrame([d['details'] for d in response])
        c1 = pd.DataFrame([{"id": k, **v} for d in response for k, v in d['VSP_OPT_PRES_C1'].items()])
        prescription_details = prescription_details.append(details, ignore_index=True)
        prescription_c1 = prescription_c1.append(c1, ignore_index=True)
    # except:
    #     print('Error')

    '''
    PRESCRIPTION DETAILS
    '''
    # rename columns
    prescription_details.rename(columns={
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
                        "SRE NV":"sre_nv",
                        "NV in Cyl":"nv_in_cyl",
                        "LastRx Prsc Prog":"lastrx_prsc_prog",
                        "LastRx Prsc BF":"lastrx_prsc_bf",
                        "Conversion Reason":"conversion_reason",
                        "Conversion Remarks":"conversion_remarks",
                        "U_VSPACTNO":"activity_no"
                        },
                        inplace=True)

    print("Details Columns Renamed")

    
    prescription_details = prescription_details.sort_values('update_date').drop_duplicates('code',keep='last')

    #transformation
    prescription_details = prescription_details.set_index(['code'])

    # df to db
    upsert(engine=engine,
       df=prescription_details,
       schema='mabawa_staging',
       table_name='source_prescriptions',
       if_row_exists='update',
       create_table=True)

    print("Prescriptions Inserted")

    '''
    PRESCRIPTION C1
    '''
    # rename columns
    prescription_c1.rename(columns={
                        "Code": "code",
                        "LineId": "line_id",
                        "ViewTime": "view_time",
                        "ViewDate": "view_date",
                        "RequestedBy": "requestedby"
                        },
                        inplace=True)

    print("Columns Renamed")

    # transformation
    prescription_c1 = prescription_c1.drop_duplicates(['code','line_id'],keep='last')
    
    prescription_c1 = prescription_c1.set_index(['code', 'line_id'])
    
    # df to db
    upsert(engine=engine,
       df=prescription_c1,
       schema='mabawa_staging',
       table_name='source_prescriptions_c1',
       if_row_exists='update',
       create_table=True)

    # prescription_c1.to_sql('source_prescriptions_c1', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)

    print('Update Successful')

def fact_prescriptions ():

    # prescription_details = pd.read_sql("""
    # SELECT 
    #     code, 
    #     o.document_number, o.posting_date as salesorder_posting_date,
    #     o.creation_date as salesorder_createdate,
    #     o.creation_time as salesorder_time,
    #     o.order_canceled as salesorder_canceled,
    #     o.draft_orderno, o.draftorder_posting_date,
    #     o.draftorder_createdate::date,
    #     o.draftorder_creation_time,
    #     o.user_signature as order_user_signature, o.sales_employee as order_sales_employee,
    #     doc_entry, user_sign, create_date::date, create_time, 
    #     (case when length(create_time::text)=4 then create_time::text::time
    #     when length(create_time::text)=3 then ('0'||create_time::text)::time
    #     when length(create_time::text)=2 then ('12'||create_time::text)::time
    #     end) as create_time_time,
    #     update_date::date, update_time, 
    #     (case when length(update_time::text)=4 then update_time::text::time
    #     when length(update_time::text)=3 then ('0'||update_time::text)::time
    #     when length(update_time::text)=2 then ('12'||update_time::text)::time
    #     end) as update_time_time,
    #     type_of_lenses, print, "order", 
    #     print_count, print_date, print_amt, print_advance_amt, optom, pfrx, 
    #     old_outside_rx, bp_adv_amt, bp_status, cl_amt_with_tax, latest_updated_optomid, 
    #     rx_type, branch_code, cust_code, status, routine_eye_exam, second_opinion, 
    #     red_eyes, blurred_vision, headache, light_sensitivity, good_health, eye_tearing, 
    #     itchy_eyes, computer_vision_syndrome, other_remarks_ormks, glaucoma_diabetes, other_remarks, 
    #     gla_under_medica, glaucoma, gla_under_medical, glaucoma_remarks, hyper, other_remarks_drk, other, 
    #     remarks, hypertension, remarks_hyper, remarks_diab, remarks_2, "others", other_reason, mostly_outdoors, 
    #     computer_usage, sports, occupation, excess_near_work, polluted_dusty_environments, remarks_open_field, 
    #     cust_currntly_wearing_specs, iol_eye, laser, is_ocular_applicable, iol, eye_pain, old_rx_sameaslast, old_rx_dv, 
    #     old_rx_nv, bifocal, progressive, old_rxre_dv_sph, old_rxre_dv_cyl, old_rxre_dv_axi, old_rxre_dv_va, old_rxre_dv_ad, 
    #     old_rxre_nv_sph, old_rxre_nv_cyl, old_rxre_nv_axi, old_rxre_nv_va, old_rxre_nv_ad, old_rxle_dv_sph, old_rxle_dv_cyl, 
    #     old_rxle_dv_axi, old_rxle_dv_va, old_rxle_dv_ad, old_rxle_nv_sph, old_rxle_nv_cyl, old_rxle_nv_axi, old_rxle_nv_va, 
    #     old_rxle_nv_ad, where_isrx_from, old_rx_date, pf_rx_1, pgpdvchk, pgpnvchk, pgpdv_nvchk, progressive1, hospital_name, pd, 
    #     "Opthalmologist", "Recommendations", "PGP_date", predvspch, predvcycl, predvaxis, predvva, predv_re_rx_add, prenvspch, prenvcycl, 
    #     prenvaxis, prenvva, pledvspch, pledvcycl, pledvaxis, pledvva, predv_le_rx_add, plenvspch, plenvcycl, plenvva, prenv_le_rx_add_nv, 
    #     glucoma, eye_drop_down, glucoma_remarks, optica_eyetest, blood_pressure, blood_sugar, test_done, eye_near, eye_dst, mono_rigt_eye, 
    #     mono_left_eye, near_vision_readingat, mar, hiindex, standard, blockeddv, blockednv, blockedbf, ppfrx, outside_rx, last_rx, bifocal_progressive, sphre_dv_nva, sredvspch, sredvcycl, sredvaxis, sredvva, sphre_dv_add, sphle_dv_nva, sledvspch, sledvcycl, sledvaxis, sledvva, sphle_dv_add, sphre_nv_nva, bothc, sphre_nv_add, sphle_nv_nva, slenv, bothg, slennvva, sphle_nv_add, clinic_remarks, slaesperson_rmks, needcl, cl_amt, patient_to_ophth, opht_note, blurred_distance_vision, ophth_name, typesoft, clear, crizol, photo, spec_recommendations, rx_summery_selc, rx_stable_unstable, p.sales_employee, pd_distance, p.sales_employees, reasson, optom_remarks,
    #     null as rx_high_low
    # FROM mabawa_staging.source_prescriptions p
    # left join mabawa_mviews.source_orders_header_with_prescriptions o on p.code::text = o.presctiption_no::text
    # """, con=engine)

    # print('fact prescription data fetched')
    
    query = """truncate mabawa_dw.fact_prescriptions;
                insert into mabawa_dw.fact_prescriptions
                SELECT 
                code, 
                o.document_number, o.posting_date as salesorder_posting_date,
                o.creation_date as salesorder_createdate,
                o.creation_time as salesorder_time,
                o.order_canceled as salesorder_canceled,
                o.draft_orderno, o.draftorder_posting_date,
                o.draftorder_createdate::date,
                o.draftorder_creation_time,
                o.user_signature as order_user_signature, o.sales_employee as order_sales_employee,
                doc_entry, user_sign, create_date::date, create_time, 
                (case when length(create_time::text)=4 then create_time::text::time
                when length(create_time::text)=3 then ('0'||create_time::text)::time
                when length(create_time::text)=2 then ('12'||create_time::text)::time
                end) as create_time_time,
                update_date::date, update_time, 
                (case when length(update_time::text)=4 then update_time::text::time
                when length(update_time::text)=3 then ('0'||update_time::text)::time
                when length(update_time::text)=2 then ('12'||update_time::text)::time
                end) as update_time_time,
                type_of_lenses, print, "order", 
                print_count, print_date, print_amt, print_advance_amt, optom, pfrx, 
                old_outside_rx, bp_adv_amt, bp_status, cl_amt_with_tax, latest_updated_optomid, 
                rx_type, branch_code, cust_code, status, routine_eye_exam, second_opinion, 
                red_eyes, blurred_vision, headache, light_sensitivity, good_health, eye_tearing, 
                itchy_eyes, computer_vision_syndrome, other_remarks_ormks, glaucoma_diabetes, other_remarks, 
                gla_under_medica, glaucoma, gla_under_medical, glaucoma_remarks, hyper, other_remarks_drk, other, 
                remarks, hypertension, remarks_hyper, remarks_diab, remarks_2, "others", other_reason, mostly_outdoors, 
                computer_usage, sports, occupation, excess_near_work, polluted_dusty_environments, remarks_open_field, 
                cust_currntly_wearing_specs, iol_eye, laser, is_ocular_applicable, iol, eye_pain, old_rx_sameaslast, old_rx_dv, 
                old_rx_nv, bifocal, progressive, old_rxre_dv_sph, old_rxre_dv_cyl, old_rxre_dv_axi, old_rxre_dv_va, old_rxre_dv_ad, 
                old_rxre_nv_sph, old_rxre_nv_cyl, old_rxre_nv_axi, old_rxre_nv_va, old_rxre_nv_ad, old_rxle_dv_sph, old_rxle_dv_cyl, 
                old_rxle_dv_axi, old_rxle_dv_va, old_rxle_dv_ad, old_rxle_nv_sph, old_rxle_nv_cyl, old_rxle_nv_axi, old_rxle_nv_va, 
                old_rxle_nv_ad, where_isrx_from, old_rx_date, pf_rx_1, pgpdvchk, pgpnvchk, pgpdv_nvchk, progressive1, hospital_name, pd, 
                "Opthalmologist", "Recommendations", "PGP_date", predvspch, predvcycl, predvaxis, predvva, predv_re_rx_add, prenvspch, prenvcycl, 
                prenvaxis, prenvva, pledvspch, pledvcycl, pledvaxis, pledvva, predv_le_rx_add, plenvspch, plenvcycl, plenvva, prenv_le_rx_add_nv, 
                glucoma, eye_drop_down, glucoma_remarks, optica_eyetest, blood_pressure, blood_sugar, test_done, eye_near, eye_dst, mono_rigt_eye, 
                mono_left_eye, near_vision_readingat, mar, hiindex, standard, blockeddv, blockednv, blockedbf, ppfrx, outside_rx, last_rx, 
                bifocal_progressive, sphre_dv_nva, sredvspch, sredvcycl, sredvaxis, sredvva, sphre_dv_add, sphle_dv_nva, sledvspch, sledvcycl, 
                sledvaxis, sledvva, sphle_dv_add, sphre_nv_nva, bothc, sphre_nv_add, sphle_nv_nva, slenv, bothg, slennvva, sphle_nv_add, 
                clinic_remarks, slaesperson_rmks, needcl, cl_amt, patient_to_ophth, opht_note, blurred_distance_vision, ophth_name, typesoft, 
                clear, crizol, photo, spec_recommendations, rx_summery_selc, rx_stable_unstable, p.sales_employee, pd_distance, p.sales_employees, 
                reasson, optom_remarks,
                null as rx_high_low
            FROM mabawa_staging.source_prescriptions p
            left join mabawa_mviews.source_orders_header_with_prescriptions o on p.code::text = o.presctiption_no::text 
    """

    query = pg_execute(query)

    # prescription_details = prescription_details.to_sql('fact_prescriptions', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    # prescription_details = prescription_details.set_index(['code'])
    # upsert(engine=engine,
    #     df=prescription_details,
    #     schema='mabawa_dw',
    #     table_name='fact_prescriptions',
    #     if_row_exists='update',
    #     create_table=False,
    #     add_new_columns=True)

def fact_orders_header_with_categories_new ():

    query = """truncate mabawa_dw.fact_orders_header_with_categories_new;
                insert into mabawa_dw.fact_orders_header_with_categories_new
                SELECT *
                FROM
                (SELECT * 
                FROM 
                (SELECT 
                    o.internal_number, o.order_canceled, o.document_status,
                    o.warehouse_status, o.posting_date, o.cust_vendor_code, o.total_tax, o.order_document_discount, 
                    o.order_total_discount, o.doc_total, o.generation_time, o.sales_employee, o.tax_amt_sc, o.doc_total_sc, 
                    o.user_signature, o.creation_time_int, o.creation_time, o.creation_datetime, o.presc_spec, 
                    o.advanced_payment, o.remaining_payment, o.orderscreen, o.order_branch, o.presc_no, o.presc_print_type, 
                    o.draftorder_posting_date, o.draftorder_createtime, o.draft_order_type, o.order_category, 
                    o.use_for_viewrx,p.create_date,
                    (case when p.document_number is null then o.document_number
                    else p.document_number end) as document_number,
                    (case when p.salesorder_createdate::date is null then o.creation_date::date
                    else p.salesorder_createdate::date end) as creation_date,
                    (case when p.draft_orderno is null then o.draft_orderno
                    else o.draft_orderno end) as draft_orderno,
                    (case when p.draftorder_createdate::date is null then o.draftorder_createdate::date
                    else p.draftorder_createdate::date end) as draftorder_createdate
                FROM
                ((SELECT    
                    *
                FROM mabawa_dw.fact_prescriptions 
                    where ophth_name is null
                    and status not in ('Cancel', 'CanceledEyeTest')) as p
                    left join mabawa_dw.fact_orders_header_with_categories o on p.branch_code = o.order_branch and p.cust_code = o.cust_vendor_code)
                    where order_canceled <> 'Y'     
                    and order_category in ('CONTACT LENSES', 'READY READER')) as t) as tt
                    where tt.draftorder_createdate >= tt.create_date
    """

    query = pg_execute(query)


def calculate_rx ():

    query = """
    update mabawa_dw.fact_prescriptions p 
    set rx_high_low = r."RX" 
    from mabawa_mviews.v_fact_prescriptions_rx r
    where r.code = p.code
    """
    query = pg_execute(query)


def create_et_conv ():

    query = """
    refresh materialized view mabawa_mviews.et_conv;
    insert into mabawa_dw.update_log(table_name, update_time) values('et_conv', default);
    refresh materialized view mabawa_mviews.order_contents;
    refresh materialized view mabawa_mviews.old_eyetest_viewed_conversion;
    refresh materialized view mabawa_mviews.optoms_older_than_30days_eyetest_viewed_conversion;
    refresh materialized view mabawa_mviews.salespersons_older_than_30days_eyetest_viewed_conversion;  

    """
    query = pg_execute(query)

# fetch_prescriptions()
# fact_prescriptions()

