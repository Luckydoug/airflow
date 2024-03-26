import sys
sys.path.append(".")
import numpy as np
import pandas as pd
import holidays as pyholidays
from datetime import date, timedelta, datetime, time
from pangres import upsert
from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.libraries.utils import (calculate_time_taken,calculate_time_taken_for_row,today,pastdate)


def update_insurance_efficiency_before_feedback():

    df_q = """  
    with df as (
    select 
        row_number() over(partition by doc_entry order by sent_preauth_tm) as rw,
        doc_entry, doc_no, ods_outlet, ods_creator, ods_creator_name, draft_rejected, draft_rejected_tm, upload_resent, upload_resent_tm, sent_preauth, sent_preauth_tm, sentby, sent_branch_code, sent_branch_name, email_sent, subject
    from mabawa_mviews.v_insurance_efficiency_before_feedback
    where draft_rejected_tm::date >= current_date - interval '7 days'
    and sent_preauth in ('Sent Pre-Auth to Insurance Company','Resent Pre-Auth to Insurance Company')
    order by sent_preauth desc nulls last
    )
    select 
        doc_entry,doc_no,ods_outlet,ods_creator,ods_creator_name,draft_rejected,draft_rejected_tm,upload_resent,upload_resent_tm,sent_preauth,sent_preauth_tm,sentby,sent_branch_code,sent_branch_name,
        case 
            when rw=1 then email_sent else null
        end as email_sent,
        case 
            when rw=1 then subject else null
        end as subject
    from df 
    union all 
    select 
        doc_entry, doc_no, ods_outlet, ods_creator, ods_creator_name, draft_rejected, draft_rejected_tm, upload_resent, upload_resent_tm, sent_preauth, sent_preauth_tm, sentby, sent_branch_code, sent_branch_name, email_sent, subject
    from mabawa_mviews.v_insurance_efficiency_before_feedback
    where draft_rejected_tm::date >= current_date - interval '7 days'
    and sent_preauth in ('Rejected by Optica Insurance')
    """

    df = pd.read_sql(df_q,con=engine)

    df = df[df['ods_outlet'].isna()!=True]
    
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
    
    ke_holidays = pyholidays.KE()

    df['drftorrjctd_to_upldorrsnt'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'ods_outlet', 'draft_rejected_tm', 'upload_resent_tm',branch_data,ke_holidays),axis=1)
    df['upldorrsnt_to_sntprthorrjctd'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'sent_branch_code', 'upload_resent_tm', 'sent_preauth_tm',branch_data,ke_holidays),axis=1)
    df['upldorrsnt_to_snteml'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'sent_branch_code', 'upload_resent_tm', 'email_sent',branch_data,ke_holidays),axis=1)

    df.set_index(['doc_entry','draft_rejected_tm','upload_resent_tm','sent_preauth_tm'],inplace=True)

    upsert(engine=engine,
    df=df,
    schema='mabawa_mviews',
    table_name='insurance_efficiency_before_feedback',
    if_row_exists='update',
    create_table=False)


def update_insurance_efficiency_after_feedback():

    df_q = """  
    select doc_entry, doc_no, ods_outlet, ods_creator, ods_creator_name, approval_remark, rmrk_by, rmrk_branch, rmrk_name, odsc_remarks, 
    approval_remark_tm, approval_remark_tm - interval '3 minutes' as approval_remark_tm2, approval_update_tm, fdbck_rcvd, rcvd_by, fdbck_rcvd_tm
    from mabawa_mviews.v_insurance_efficiency_after_feedback
    where coalesce(approval_update_tm::date,fdbck_rcvd_tm::date) >= current_date - interval '7 days'
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
    
    ke_holidays = pyholidays.KE()

    df['rmrk_to_updt'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'rmrk_branch', 'approval_remark_tm', 'approval_update_tm',branch_data,ke_holidays),axis=1)
    df['rmrk2_to_updt'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'rmrk_branch', 'approval_remark_tm2', 'approval_update_tm',branch_data,ke_holidays),axis=1)
    df['apprvl_to_rcvd'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'ods_outlet', 'approval_update_tm', 'fdbck_rcvd_tm',branch_data,ke_holidays),axis=1)

    df.set_index(['doc_entry','approval_update_tm'],inplace=True)

    upsert(engine=engine,
    df=df,
    schema='mabawa_mviews',
    table_name='insurance_efficiency_after_feedback',
    if_row_exists='update',
    create_table=False)
