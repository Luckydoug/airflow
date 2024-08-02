import sys
from airflow.models import variable
sys.path.append(".")
import numpy as np
import pandas as pd
import holidays as pyholidays
from datetime import date, timedelta, datetime, time
from pangres import upsert
from sub_tasks.data.connect import (pg_execute, engine) 

from sub_tasks.libraries.utils import (calculate_time_taken,calculate_time_taken_for_row,ke_holidays)


def update_insurance_efficiency_before_feedback():

    df_q = """  
    select drft_rw, upld_rw, doc_entry, doc_no, insurance_name1, insurance_name2, ods_outlet, ods_creator, 
    user_name, draft_rejected, draft_rejected_tm, upload_resent, upload_resent_tm, sent_preauth, 
    sent_preauth_tm, sentby, sentby_branch_code, sentby_branch_name,plan_scheme_type1,plan_scheme_type2 
    from mabawa_mviews.v_insurance_efficiency_before_feedback
    where coalesce(draft_rejected_tm::date,upload_resent_tm::date,sent_preauth_tm::date) >= current_date - interval '14 days'
    and ods_outlet not in ('Uganda','Rwanda','null')
    and sentby_branch_code not in ('Uganda','Rwanda','null')
    """

    df = pd.read_sql(df_q,con=engine)
    
    df = df[df['ods_outlet']!=None]
    df = df[df['sentby_branch_code']!=None]
    
    wrkng_hrs_q = """    
    SELECT warehouse_code, warehouse_name, docnum, days, start_time, end_time, auto_time
    FROM reports_tables.working_hours
    """

    wrkng_hrs = pd.read_sql(wrkng_hrs_q,engine)

    branch_data = {}

    for _, row in wrkng_hrs.iterrows():
        branch = row['warehouse_code']
        day = row['days']
        start_time = row['start_time']
        end_time = row['end_time']

        start_time = datetime.strptime(start_time,'%H:%M').time()
        end_time = datetime.strptime(end_time,'%H:%M').time()

        if branch not in branch_data:

            branch_data[branch] = {
                'working_hours': {},
                }
    
        branch_data[branch]['working_hours'][day] = (start_time, end_time)

    df['drftorrjctd_to_upldorrsnt'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'ods_outlet', 'draft_rejected_tm', 'upload_resent_tm',branch_data,ke_holidays),axis=1)
    df['upldorrsnt_to_sntprthorrjctd'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'sentby_branch_code', 'upload_resent_tm', 'sent_preauth_tm',branch_data,ke_holidays),axis=1)
  
    # df.to_sql('insurance_efficiency_before_feedback', con=engine, schema='mabawa_mviews', if_exists = 'append', index=False)

    df.set_index(['drft_rw','upld_rw','doc_entry','draft_rejected_tm','upload_resent_tm','sent_preauth_tm'],inplace=True)

    upsert(engine=engine,
    df=df,
    schema='mabawa_mviews',
    table_name='insurance_efficiency_before_feedback',
    if_row_exists='update',
    create_table=False)


def update_insurance_efficiency_after_feedback():

    df_q = """  
    select 
        doc_entry,doc_no,ods_outlet,ods_creator,ods_creator_name,fdbk_stts,fdbck_rmrks,fdbck_rmrk_by,
        rmrk_branch,rmrk_name,fdbk_rmrk_tm,fdbk_rmrk_tm - interval '3 minutes' as fdbk_rmrk_tm2,
        apprvl_updt_tm,cstmr_cntctd,cstmr_cntctd_rmrk,cntctd_by,cstmr_cntctd_tm
    from mabawa_mviews.v_insurance_efficiency_after_feedback
    where coalesce(apprvl_updt_tm::date,cstmr_cntctd_tm::date) >= current_date - interval '14 days'
    """

    df = pd.read_sql(df_q,con=engine)

    # df = df[df['ods_outlet']!=None]
    # df = df[df['rmrk_branch']!=None]
    df.dropna(subset=['ods_outlet'],inplace=True)
    df.dropna(subset=['rmrk_branch'],inplace=True)

    # print(df[df['ods_outlet'].isna()==True])
    
    wrkng_hrs_q = """    
    SELECT warehouse_code, warehouse_name, docnum, days, start_time, end_time, auto_time
    FROM reports_tables.working_hours
    """

    wrkng_hrs = pd.read_sql(wrkng_hrs_q,engine)

    branch_data = {}

    for _, row in wrkng_hrs.iterrows():
        branch = row['warehouse_code']
        day = row['days']
        start_time = row['start_time']
        end_time = row['end_time']

        start_time = datetime.strptime(start_time,'%H:%M').time()
        end_time = datetime.strptime(end_time,'%H:%M').time()

        if branch not in branch_data:

            branch_data[branch] = {
                'working_hours': {},
                }
    
        branch_data[branch]['working_hours'][day] = (start_time, end_time)

    df['rmrk_to_updt'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'rmrk_branch', 'fdbk_rmrk_tm', 'apprvl_updt_tm',branch_data,ke_holidays),axis=1)
    df['rmrk2_to_updt'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'rmrk_branch', 'fdbk_rmrk_tm2', 'apprvl_updt_tm',branch_data,ke_holidays),axis=1)
    df['apprvl_to_cstmr_cntctd'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'ods_outlet', 'apprvl_updt_tm', 'cstmr_cntctd_tm',branch_data,ke_holidays),axis=1)

    df.set_index(['doc_entry','apprvl_updt_tm'],inplace=True)

    upsert(engine=engine,
    df=df,
    schema='mabawa_mviews',
    table_name='insurance_efficiency_after_feedback',
    if_row_exists='update',
    create_table=False)

def update_approvals_efficiency():

    df_q = """  
    select 
        doc_entry, doc_no, appr_strt, appr_strt_tm, appr_end, appr_end_tm, apprvd_by, 'APR' as ods_outlet
    from mabawa_mviews.v_approvals_efficiency
    where coalesce(appr_end_tm,appr_strt_tm)::date >= current_date - interval '14 days'
    """

    df = pd.read_sql(df_q,con=engine)
    
    wrkng_hrs_q = """    
    SELECT warehouse_code, warehouse_name, docnum, days, start_time, end_time, auto_time
    FROM reports_tables.working_hours
    """

    wrkng_hrs = pd.read_sql(wrkng_hrs_q,engine)

    branch_data = {}

    for _, row in wrkng_hrs.iterrows():
        branch = row['warehouse_code']
        day = row['days']
        start_time = row['start_time']
        end_time = row['end_time']

        start_time = datetime.strptime(start_time,'%H:%M').time()
        end_time = datetime.strptime(end_time,'%H:%M').time()

        if branch not in branch_data:

            branch_data[branch] = {
                'working_hours': {},
                }
    
        branch_data[branch]['working_hours'][day] = (start_time, end_time)
    
    print(ke_holidays)
    df['time_taken'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'ods_outlet', 'appr_strt_tm', 'appr_end_tm',branch_data,ke_holidays),axis=1)
    df.drop('ods_outlet',axis=1,inplace=True)

    df.set_index(['doc_entry','appr_strt_tm'],inplace=True)

    upsert(engine=engine,
    df=df,
    schema='mabawa_mviews',
    table_name='approvals_efficiency',
    if_row_exists='update',
    create_table=False)

def refresh_insurance_request_no_feedback():

    query = """
    refresh materialized view mabawa_mviews.insurance_request_no_feedback;
    """
    query = pg_execute(query)

# update_insurance_efficiency_before_feedback()
# update_insurance_efficiency_after_feedback()
# update_approvals_efficiency()
# refresh_insurance_request_no_feedback()