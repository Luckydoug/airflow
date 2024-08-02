import sys
sys.path.append(".")
import numpy as np
import pandas as pd
import holidays as pyholidays
from datetime import date, timedelta, datetime, time
from pangres import upsert
from sub_tasks.data.connect import (engine) 
from sub_tasks.libraries.utils import (calculate_time_taken_for_row,add_autotime_branchlens,ke_holidays)


def update_branchlens_glazing_efficiency():

    df_q = """  
    select 
        doc_entry, draft_orderno, creation_date, ods_createdon, order_canceled, ods_insurance_order, cust_vendor_code, 
        order_branch, ods_orderprocess_branch, branch_type, ods_creator, user_name, ods_ordercriteriastatus, ods_localcollection, 
        ods_brandnm, order_hours, delivery_date, min_time, sap_time, auto_time, strt_stts, strt_tmstmp, so_stts, so_tmstmp, idntfr_stts, idntfr_tmstmp, 
        ordrprntd_tmstmp, assgnd_tech_tmstmp, dmgd_tmstmp, dmg_remarks, fttng_cmpltd_tmstmp, awtng_cllctn_tmstmp, cllctd_tmstmp
    from mabawa_mviews.v_branchlens_glazing_efficiency
    where greatest(creation_date::date,awtng_cllctn_tmstmp::date) >= '2024-01-01'
    """

    df = pd.read_sql(df_q,con=engine)
    
    wrkng_hrs_q = """    
    SELECT warehouse_code, warehouse_name, docnum, days, start_time, end_time, auto_time
    FROM reports_tables.working_hours
    """

    wrkng_hrs = pd.read_sql(wrkng_hrs_q,engine)

    branch_data = {}
    branch_data2 = {}

    for _, row in wrkng_hrs.iterrows():
        branch = row['warehouse_code']
        day = row['days']
        start_time = row['start_time']
        end_time = row['end_time']
        auto_time = row['auto_time']

        start_time = datetime.strptime(start_time,'%H:%M').time()
        end_time = datetime.strptime(end_time,'%H:%M').time()
        auto_time = datetime.strptime(auto_time,'%H:%M').time()

        if branch not in branch_data:

            branch_data[branch] = {
                'working_hours': {},
                }
            branch_data2[branch] = {
            'working_hours': {},
        }
    
        branch_data[branch]['working_hours'][day] = (start_time, end_time)
        branch_data2[branch]['working_hours'][day] = (start_time, auto_time)
    
    df['clcltd_delivery_date'] = df.apply(lambda row: add_autotime_branchlens(row['strt_tmstmp'], row['order_branch'], row['auto_time'], branch_data2, ke_holidays),axis=1)

    df['strt_to_so'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'order_branch', 'strt_tmstmp', 'so_tmstmp',branch_data,ke_holidays),axis=1)
    df['so_to_idnfr'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'order_branch', 'so_tmstmp', 'idntfr_tmstmp',branch_data,ke_holidays),axis=1)
    df['so_to_ordrprntd'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'order_branch', 'so_tmstmp', 'ordrprntd_tmstmp',branch_data,ke_holidays),axis=1)
    df['so_to_assgnd'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'order_branch', 'so_tmstmp', 'assgnd_tech_tmstmp',branch_data,ke_holidays),axis=1)
    df['idnfr_to_ordrprntd'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'order_branch', 'so_tmstmp', 'assgnd_tech_tmstmp',branch_data,ke_holidays),axis=1)
    df['ordrprntd_to_assgnd'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'order_branch', 'ordrprntd_tmstmp', 'assgnd_tech_tmstmp',branch_data,ke_holidays),axis=1)
    df['assgnd_to_fttng'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'order_branch', 'assgnd_tech_tmstmp', 'fttng_cmpltd_tmstmp',branch_data,ke_holidays),axis=1)
    df['fttng_to_awtng'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'order_branch', 'fttng_cmpltd_tmstmp', 'awtng_cllctn_tmstmp',branch_data,ke_holidays),axis=1)
    df['strt_to_awtng'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'order_branch', 'strt_tmstmp', 'awtng_cllctn_tmstmp',branch_data,ke_holidays),axis=1)
    df['so_to_awtng'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'order_branch', 'so_tmstmp', 'awtng_cllctn_tmstmp',branch_data,ke_holidays),axis=1)
    df['strt_to_cllctd'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'order_branch', 'strt_tmstmp', 'cllctd_tmstmp',branch_data,ke_holidays),axis=1)
    df['so_to_cllctd'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'order_branch', 'so_tmstmp', 'cllctd_tmstmp',branch_data,ke_holidays),axis=1)
    df['strt_to_dlvry'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'order_branch', 'strt_tmstmp', 'delivery_date',branch_data2,ke_holidays),axis=1)
  
    df.set_index('doc_entry',inplace=True)

    upsert(engine=engine,
    df=df,
    schema='mabawa_mviews',
    table_name='branchlens_glazing_efficiency',
    if_row_exists='update',
    create_table=True)


def update_hqlens_glazing_efficiency():

    df_q = """  
    select 
        doc_entry, draft_orderno, creation_date, ods_createdon, order_canceled, ods_insurance_order, cust_vendor_code, 
        order_branch, ods_orderprocess_branch, branch_type, ods_creator, user_name, ods_ordercriteriastatus, ods_localcollection, 
        ods_brandnm, order_hours, delivery_date, strt_stts, strt_tmstmp, so_stts, so_tmstmp, idntfr_stts, idntfr_tmstmp, hqlns_snt_tmstmp, 
        hqlns_rcvd_tmstmp, assgnd_tech_tmstmp, dmgd_tmstmp, dmg_remarks, fttng_cmpltd_tmstmp, awtng_cllctn_tmstmp, cllctd_tmstmp
    from mabawa_mviews.v_hqlens_glazing_efficiency
    where greatest(creation_date::date,awtng_cllctn_tmstmp::date) >= '2024-01-01'
    """

    df = pd.read_sql(df_q,con=engine)
    
    wrkng_hrs_q = """    
    SELECT warehouse_code, warehouse_name, docnum, days, start_time, end_time, auto_time
    FROM reports_tables.working_hours
    """

    wrkng_hrs = pd.read_sql(wrkng_hrs_q,engine)

    branch_data = {}
    branch_data2 = {}

    for _, row in wrkng_hrs.iterrows():
        branch = row['warehouse_code']
        day = row['days']
        start_time = row['start_time']
        end_time = row['end_time']
        auto_time = row['auto_time']

        start_time = datetime.strptime(start_time,'%H:%M').time()
        end_time = datetime.strptime(end_time,'%H:%M').time()
        auto_time = datetime.strptime(auto_time,'%H:%M').time()

        if branch not in branch_data:

            branch_data[branch] = {
                'working_hours': {},
                }
            branch_data2[branch] = {
            'working_hours': {},
        }
    
        branch_data[branch]['working_hours'][day] = (start_time, end_time)
        branch_data2[branch]['working_hours'][day] = (start_time, auto_time)
    
    df['assgnd_to_fttng'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'order_branch', 'assgnd_tech_tmstmp', 'fttng_cmpltd_tmstmp',branch_data,ke_holidays),axis=1)

    df.set_index('doc_entry',inplace=True) 

    upsert(engine=engine,
    df=df,
    schema='mabawa_mviews',
    table_name='hqlens_glazing_efficiency',
    if_row_exists='update',
    create_table=False)

def update_promised_collectiondate():

    df_q = """  
    select 
        doc_entry,draft_orderno,creation_date,ods_createdon,order_canceled,ods_status1,ods_insurance_order,cust_vendor_code, 
        order_branch,ods_orderprocess_branch,ods_creator,user_name,ods_ordercriteriastatus,ods_localcollection,ods_brandnm, 
        strt_stts,strt_tmstmp,order_hours,delivery_date,ods_urgent,awtng_cllctn_tmstmp
    from mabawa_mviews.v_promised_collectiondate
    where delivery_date::date >= '2024-01-01'
    """

    df = pd.read_sql(df_q,con=engine)
    
    wrkng_hrs_q = """    
    SELECT warehouse_code, warehouse_name, docnum, days, start_time, end_time, auto_time
    FROM reports_tables.working_hours
    """

    wrkng_hrs = pd.read_sql(wrkng_hrs_q,engine)

    branch_data = {}
    branch_data2 = {}

    for _, row in wrkng_hrs.iterrows():
        branch = row['warehouse_code']
        day = row['days']
        start_time = row['start_time']
        end_time = row['end_time']
        auto_time = row['auto_time']

        start_time = datetime.strptime(start_time,'%H:%M').time()
        end_time = datetime.strptime(end_time,'%H:%M').time()
        auto_time = datetime.strptime(auto_time,'%H:%M').time()

        if branch not in branch_data:

            branch_data[branch] = {
                'working_hours': {},
                }
            branch_data2[branch] = {
            'working_hours': {},
        }
    
        branch_data[branch]['working_hours'][day] = (start_time, end_time)
        branch_data2[branch]['working_hours'][day] = (start_time, auto_time)
    
    df['time_promised'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'order_branch', 'strt_tmstmp', 'delivery_date',branch_data2,ke_holidays),axis=1)

    df.set_index('doc_entry',inplace=True) 

    upsert(engine=engine,
    df=df,
    schema='mabawa_mviews',
    table_name='promised_collectiondate',
    if_row_exists='update',
    create_table=True)





