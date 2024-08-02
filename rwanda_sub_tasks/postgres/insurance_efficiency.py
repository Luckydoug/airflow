import sys
from airflow.models import variable

sys.path.append(".")
import pandas as pd
from datetime import datetime
from pangres import upsert
from sub_tasks.libraries.utils import create_rwanda_engine
from reports.draft_to_upload.data.fetch_data import fetch_working_hours
from sub_tasks.data.connect_mawingu import pg_execute
from reports.draft_to_upload.data.fetch_data import fetch_holidays
from sub_tasks.libraries.time_diff import (
    calculate_time_difference,
    working_hours_dictionary,
)


def update_insurance_efficiency_before_feedback():

    engine = create_rwanda_engine()

    df_q = """  
    select drft_rw, upld_rw, doc_entry, doc_no, insurance_name1, insurance_name2, ods_outlet, ods_creator, 
    user_name, draft_rejected, draft_rejected_tm, upload_resent, upload_resent_tm, sent_preauth, 
    sent_preauth_tm, sentby, sentby_branch_code, sentby_branch_name,plan_scheme_type1,plan_scheme_type2 
    from voler_mviews.v_insurance_efficiency_before_feedback
    where coalesce(draft_rejected_tm::date,upload_resent_tm::date,sent_preauth_tm::date) >= current_date - interval '14 days'
    and ods_outlet not in ('Uganda','Rwanda','null')
    and sentby_branch_code not in ('Uganda','Rwanda','null')
    """

    df = pd.read_sql(df_q, con=engine)

    df = df[df["ods_outlet"] != None]
    df = df[df["sentby_branch_code"] != None]

    working_hours = fetch_working_hours(engine=engine)
    holidays = fetch_holidays(engine=engine, dw="voler_dw")

    work_hours = working_hours_dictionary(working_hours=working_hours)

    df["drftorrjctd_to_upldorrsnt"] = df.apply(
        lambda row: calculate_time_difference(
            row=row,
            x="draft_rejected_tm",
            y="upload_resent_tm",
            holiday_dict=holidays,
            working_hours=work_hours,
            country="Uganda",
            outlet="ods_outlet",
        ),
        axis=1,
    )

    df["upldorrsnt_to_sntprthorrjctd"] = df.apply(
        lambda row: calculate_time_difference(
            row=row,
            x="upload_resent_tm",
            y="sent_preauth_tm",
            holiday_dict=holidays,
            working_hours=work_hours,
            country="Uganda",
            outlet="sentby_branch_code",
        ),
        axis=1,
    )

    df = df.drop_duplicates(
        subset=[
            "drft_rw",
            "upld_rw",
            "doc_entry",
            "draft_rejected_tm",
            "upload_resent_tm",
            "sent_preauth_tm",
        ]
    )

    df.set_index(
        [
            "drft_rw",
            "upld_rw",
            "doc_entry",
            "draft_rejected_tm",
            "upload_resent_tm",
            "sent_preauth_tm",
        ],
        inplace=True,
    )

    upsert(
        engine=engine,
        df=df,
        schema="voler_mviews",
        table_name="insurance_efficiency_before_feedback",
        if_row_exists="update",
        create_table=False,
    )


def update_insurance_efficiency_after_feedback():

    engine = create_rwanda_engine()

    df_q = """  
    select 
        doc_entry,doc_no,ods_outlet,ods_creator,ods_creator_name,fdbk_stts,fdbck_rmrks,fdbck_rmrk_by,
        rmrk_branch,rmrk_name,fdbk_rmrk_tm,fdbk_rmrk_tm - interval '3 minutes' as fdbk_rmrk_tm2,
        apprvl_updt_tm,cstmr_cntctd,cstmr_cntctd_rmrk,cntctd_by,cstmr_cntctd_tm
    from voler_mviews.v_insurance_efficiency_after_feedback
    where coalesce(apprvl_updt_tm::date,cstmr_cntctd_tm::date) >= current_date - interval '14 days'
    and rmrk_branch not in ('null')
    and rmrk_branch is not null
    """

    df = pd.read_sql(df_q, con=engine)

    working_hours = fetch_working_hours(engine=engine)
    holidays = fetch_holidays(engine=engine, dw="voler_dw")

    work_hours = working_hours_dictionary(working_hours=working_hours)

    df["rmrk_to_updt"] = df.apply(
        lambda row: calculate_time_difference(
            row=row,
            x="fdbk_rmrk_tm",
            y="apprvl_updt_tm",
            holiday_dict=holidays,
            working_hours=work_hours,
            country="Uganda",
            outlet="rmrk_branch",
        ),
        axis=1,
    )

    # df['rmrk2_to_updt'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'rmrk_branch', 'fdbk_rmrk_tm2', 'apprvl_updt_tm',branch_data,ke_holidays),axis=1)

    df["rmrk2_to_updt"] = df.apply(
        lambda row: calculate_time_difference(
            row=row,
            x="fdbk_rmrk_tm2",
            y="apprvl_updt_tm",
            holiday_dict=holidays,
            working_hours=work_hours,
            country="Uganda",
            outlet="rmrk_branch",
        ),
        axis=1,
    )

    # df['apprvl_to_cstmr_cntctd'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'ods_outlet', 'apprvl_updt_tm', 'cstmr_cntctd_tm',branch_data,ke_holidays),axis=1)

    df["apprvl_to_cstmr_cntctd"] = df.apply(
        lambda row: calculate_time_difference(
            row=row,
            x="apprvl_updt_tm",
            y="cstmr_cntctd_tm",
            holiday_dict=holidays,
            working_hours=work_hours,
            country="Uganda",
            outlet="ods_outlet",
        ),
        axis=1,
    )

    df = df.drop_duplicates(subset=["doc_entry", "apprvl_updt_tm"])

    df.set_index(["doc_entry", "apprvl_updt_tm"], inplace=True)

    upsert(
        engine=engine,
        df=df,
        schema="voler_mviews",
        table_name="insurance_efficiency_after_feedback",
        if_row_exists="update",
        create_table=False,
    )


def update_approvals_efficiency():

    engine = create_rwanda_engine()

    df_q = """  
    select 
        doc_entry, doc_no, appr_strt, appr_strt_tm, appr_end, appr_end_tm, apprvd_by, 'APR' as ods_outlet
    from voler_mviews.v_approvals_efficiency
    where coalesce(appr_end_tm,appr_strt_tm)::date >= current_date - interval '14 days'
    """

    df = pd.read_sql(df_q, con=engine)

    working_hours = fetch_working_hours(engine=engine)
    holidays = fetch_holidays(engine=engine, dw="voler_dw")

    work_hours = working_hours_dictionary(working_hours=working_hours)

    # df['time_taken'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'ods_outlet', 'appr_strt_tm', 'appr_end_tm',branch_data,ke_holidays),axis=1)

    df["time_taken"] = df.apply(
        lambda row: calculate_time_difference(
            row=row,
            x="appr_strt_tm",
            y="appr_end_tm",
            holiday_dict=holidays,
            working_hours=work_hours,
            country="Uganda",
            outlet="ods_outlet",
        ),
        axis=1,
    )

    df.drop("ods_outlet", axis=1, inplace=True)

    df.set_index(["doc_entry", "appr_strt_tm"], inplace=True)

    upsert(
        engine=engine,
        df=df,
        schema="voler_mviews",
        table_name="approvals_efficiency",
        if_row_exists="update",
        create_table=False,
    )


# update_insurance_efficiency_before_feedback()
# update_insurance_efficiency_after_feedback()
# update_approvals_efficiency()
# # refresh_insurance_request_no_feedback()