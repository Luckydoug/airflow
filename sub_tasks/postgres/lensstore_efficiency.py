import sys
sys.path.append(".")
import numpy as np
import pandas as pd
from airflow.models import variable
import holidays as pyholidays
from datetime import date, timedelta, datetime, time
from pangres import upsert
from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.libraries.utils import (calculate_time_taken,calculate_time_taken_for_row,ke_holidays)

def update_lensstore_efficiency_from_receiving():

    df_q = """  
    select doc_entry, odsc_lineid, itm_snt, rcvng_to_lnsstr, itm_rcvd, rcvd_lnsstr, intrmdry_stp, intrmdry_stp_rmks, intrmdry_stp_tmstmp, out_lensstore, 'OHO' as ods_outlet
    from mabawa_mviews.v_lensstore_efficiency_from_receiving
    where rcvng_to_lnsstr >= current_date - interval '7 days'
    """

    # df = pd.read_sql(df_q,con=engine,params={'From':pastdate,'To':today})
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

    df['tm_to_rcv'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'ods_outlet', 'rcvng_to_lnsstr', 'rcvd_lnsstr', branch_data, ke_holidays), axis=1)
    df['tm_rcv_to_out'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'ods_outlet', 'rcvd_lnsstr', 'out_lensstore', branch_data, ke_holidays), axis=1)
    df['tm_intrmdry_stp_to_out'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'ods_outlet', 'intrmdry_stp_tmstmp', 'out_lensstore', branch_data, ke_holidays), axis=1)

    df.drop('ods_outlet',axis=1,inplace=True)
    df.set_index(['doc_entry','odsc_lineid'],inplace=True)

    upsert(engine=engine,
    df=df,
    schema='mabawa_mviews',
    table_name='lensstore_efficiency_from_receiving',
    if_row_exists='update',
    create_table=True)

# update_lensstore_efficiency_from_receiving()

def update_lensstore_efficiency_from_mainstore():

    df_q = """  
    select doc_entry, odsc_lineid, mnstr_to_lnsstr, intrmdry_stp, intrmdry_stp_rmks, intrmdry_stp_tmstmp, out_lensstore, 'OHO' as ods_outlet
    from mabawa_mviews.v_lensstore_efficiency_from_mainstore
    where mnstr_to_lnsstr >= current_date - interval '7 days'
    """

    # df = pd.read_sql(df_q,con=engine,params={'From':pastdate,'To':today})
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

    df['tm_rcv_to_out'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'ods_outlet', 'mnstr_to_lnsstr', 'out_lensstore', branch_data, ke_holidays), axis=1)
    df['tm_intrmdry_stp_to_out'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'ods_outlet', 'intrmdry_stp_tmstmp', 'out_lensstore', branch_data, ke_holidays), axis=1)

    df.drop('ods_outlet',axis=1,inplace=True)
    df.set_index(['doc_entry','odsc_lineid'],inplace=True)

    upsert(engine=engine,
    df=df,
    schema='mabawa_mviews',
    table_name='lensstore_efficiency_from_mainstore',
    if_row_exists='update',
    create_table=True)

# update_lensstore_efficiency_from_mainstore()

def update_salesorder_to_senttolensstore():

    df_q = """  
    select doc_entry,doc_no,odsc_createdby,ods_outlet,sale_order,order_printed,frame_to_lenstore 
    from mabawa_mviews.v_salesorder_to_senttolensstore
    where sale_order >= current_date - interval '7 days'
    """
    df = pd.read_sql(df_q,con=engine)
    df = df.drop_duplicates(subset = 'doc_entry',keep='first')

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
                                                                                         
    df['sale_order_to_order_printed'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'ods_outlet','sale_order', 'order_printed', branch_data, ke_holidays), axis=1)
    df['order_printed_to_frame_to_lenstore'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'ods_outlet','order_printed', 'frame_to_lenstore', branch_data, ke_holidays), axis=1)

    # df.drop('ods_outlet',axis=1,inplace=True)
    df.set_index(['doc_entry'],inplace=True)

    upsert(engine=engine,
    df=df,
    schema='mabawa_mviews',
    table_name='salesorder_to_senttolensstore',
    if_row_exists='update',
    create_table=True)

# update_salesorder_to_senttolensstore()