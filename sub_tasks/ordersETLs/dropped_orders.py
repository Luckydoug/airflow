import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pandas as pd
from pandas.io.json._normalize import nested_to_record 
from sqlalchemy import create_engine
from airflow.models import Variable
from pandas.io.json._normalize import nested_to_record 
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from datetime import date
import datetime
import pytz
import businesstimedelta
import pandas as pd
import holidays as pyholidays
from workalendar.africa import Kenya

from sub_tasks.data.connect import (pg_execute, engine) 


def get_source_dropped_orders():

    data = pd.read_sql("""
            SELECT 
                s2.doc_entry,  s2.odsc_lineid,  s2.odsc_date, 
                s2.odsc_time_int,  s2.odsc_time, s2.odsc_datetime, 
                s2.odsc_status,  s2.odsc_new_status,  s2.odsc_doc_no, 
                s2.odsc_createdby,  s2.odsc_usr_dept,
                (case when  s2.odsc_status = 'HQ Item Out of Stock Getting It' then  s2.odsc_status 
                when  s2.odsc_status = 'Lenses coming from Branch Lab' then 'Lense Out of Stock'
                when  s2.odsc_status = 'Repair Order Printed' then  s2.odsc_status
                when  s2.odsc_status in ('Rejected Order Received At Control Room','Rejected Lenses sent to Lens Store','Rejected Frame sent to Frame Store',
                'Control Room Rejected','Blanks Rejected at Control Room','Surfacing Damaged By Technician','Surfacing Damage/Reject Sent to Control Room',
                'Surfacing Rejected','Requested Courier for Home Delivery', 'Sent For Home Delivery','Home Delivery Collected',
                'Damaged PF/PL Sent to Branch', 'Rejected Order Sent To Control Room','Rejected Order Sent To Control Room',
                'Damage Sent to Control Rooom')
                then 'Rejections' 
                when  s2.odsc_status like '%%Reject Received at Control Room%%' then 'Rejections'
                else 'Orders and Time Issues'
                end) as dropped_status,
                s.odsc_remarks 
            FROM mabawa_staging.source_orderscreenc1_staging4 s2 
            left join mabawa_staging.source_orderscreenc1_staging2 s on s2.doc_entry = s.doc_entry 
            and s2.odsc_lineid = s.odsc_lineid
            where s2.odsc_date::date >= '2024-01-01'
                
            """, con=engine)
    
    print("Data Fetched")
    print(data)

    drop = """drop table mabawa_staging.source_droppedc1;"""
    drop= pg_execute(drop)
    print("Dropped Table")

    data.to_sql('source_droppedc1', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    
    return 'Complete' 

def update_source_dropped_orders():

    query = """
    update mabawa_staging.source_droppedc1
    set dropped_status = 'HQ Item Out of Stock Getting It'
    where doc_entry in 
    (SELECT distinct doc_entry 
    FROM mabawa_staging.source_droppedc1
    where odsc_status = 'HQ Item Out of Stock Getting It')
    and odsc_usr_dept in ('Designer','Main Store')
    """

    query = pg_execute(query)

    query1 = """
    update mabawa_staging.source_droppedc1
    set dropped_status = 'Orders and Time Issues'
    where dropped_status = 'HQ Item Out of Stock Getting It'
    and odsc_usr_dept not in ('Designer','Main Store')
    """

    query1 = pg_execute(query1)

    query2 = """
    update mabawa_staging.source_droppedc1
    set dropped_status = 'Orders and Time Issues'
    where dropped_status = 'Rejections'
    and odsc_usr_dept in ('Designer','Main Store', 'Lens Store')
    """

    query2 = pg_execute(query2)

    query3 = """
    update mabawa_staging.source_droppedc1
    set dropped_status = 'Repair Order Printed'
    where doc_entry in 
    (SELECT 
	distinct doc_entry
	FROM mabawa_staging.source_orderscreenc1
	where odsc_status = 'Repair Order Printed')
    and odsc_usr_dept in ('Lens Store')
    """

    query3 = pg_execute(query3)

    return 'Complete' 

def get_source_dropped_orders_staging():

    data = pd.read_sql("""
        select
            doc_entry, odsc_date, 
            odsc_time_int, odsc_time, odsc_datetime, 
            odsc_status, odsc_new_status, odsc_doc_no, 
            odsc_createdby, odsc_usr_dept, dropped_status
        FROM mabawa_staging.source_droppedc1
        where odsc_date::date >= '2024-04-01';
        """, con=engine)
    print('Data Fetched')

    data = data.pivot_table(index='doc_entry', columns=['odsc_new_status'], values=['odsc_datetime','odsc_createdby', 'odsc_usr_dept', 'dropped_status'], aggfunc='min')
    # rename columns
    def rename(col):
        if isinstance(col, tuple):
            col = '_'.join(str(c) for c in col)
        return col
    data.columns = map(rename, data.columns)
    data['doc_entry'] = data.index

    print('About to Truncate')
    truncate_dropped = """drop table mabawa_staging.source_droppedc1_staging;"""
    truncate_dropped=pg_execute(truncate_dropped)

    data.to_sql('source_droppedc1_staging', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    return 'something'

def create_fact_dropped_orders():

    dfholidays = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)

    data = pd.read_sql("""
    select *
    FROM mabawa_staging.source_droppedc1_staging
    """, con=engine)

    data["odsc_datetime_Sales Order Created"] = pd.to_datetime(data["odsc_datetime_Sales Order Created"])
    data["odsc_datetime_Order Printed"] = pd.to_datetime(data["odsc_datetime_Order Printed"])
    data["odsc_datetime_Frame Sent to Lens Store"] = pd.to_datetime(data["odsc_datetime_Frame Sent to Lens Store"])
    data["odsc_datetime_Frame Sent to Overseas Desk"] = pd.to_datetime(data["odsc_datetime_Frame Sent to Overseas Desk"])
    data["odsc_datetime_Sent to Control Room"] = pd.to_datetime(data["odsc_datetime_Sent to Control Room"])
    data["odsc_datetime_Issue blanks"] = pd.to_datetime(data["odsc_datetime_Issue blanks"])
    data["odsc_datetime_Issue Finished Lenses for Both Eyes"] = pd.to_datetime(data["odsc_datetime_Issue Finished Lenses for Both Eyes"])
    data["odsc_datetime_PF & PL Sent to Lens Store"] = pd.to_datetime(data["odsc_datetime_PF & PL Sent to Lens Store"])
    data["odsc_datetime_PF & PL Received at Lens Store"] = pd.to_datetime(data["odsc_datetime_PF & PL Received at Lens Store"])
    data["odsc_datetime_Repair Order Printed"] = pd.to_datetime(data["odsc_datetime_Repair Order Printed"])
    data["odsc_datetime_Sent to Surfacing"] = pd.to_datetime(data["odsc_datetime_Sent to Surfacing"])
    data["odsc_datetime_Sent to Pre Quality"] = pd.to_datetime(data["odsc_datetime_Sent to Pre Quality"])
    data["odsc_datetime_Surfacing Assigned to Technician"] = pd.to_datetime(data["odsc_datetime_Surfacing Assigned to Technician"])
    data["odsc_datetime_Assigned to Technician"] = pd.to_datetime(data["odsc_datetime_Assigned to Technician"])
    data["odsc_datetime_Sent to Packaging"] = pd.to_datetime(data["odsc_datetime_Sent to Packaging"])
    data["odsc_datetime_Branch Glazing Sent to Branch"] = pd.to_datetime(data["odsc_datetime_Branch Glazing Sent to Branch"])
    data["odsc_datetime_Sent to Branch"] = pd.to_datetime(data["odsc_datetime_Sent to Branch"])

    # All departments working hours 
    workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(19),
        working_days=[0,1, 2, 3, 4])

    saturday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(17),
        working_days=[5])

    # vic_holidays = pyholidays.KE()
    # holidays = businesstimedelta.HolidayRule(vic_holidays)
    # cal = Kenya()
    # hl = cal.holidays(2021)
    # #print(hl)
    # my_dict=dict(hl)
    # vic_holidays=vic_holidays.append(my_dict)

    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    cal = Kenya()
    hl = dfholidays.values.tolist()
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    businesshrs = businesstimedelta.Rules([workday, saturday, holidays])

    def BusHrs(start, end):
        if end>=start:
            return float(businesshrs.difference(start,end).hours)*float(60)+float(businesshrs.difference(start,end).seconds)/float(60)
        else:
            return ""   
    
    # Designer store 
    workday1 = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(18),
        working_days=[0,1, 2, 3, 4])

    saturday1 = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(16),
        working_days=[5]) 
       
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    cal = Kenya()
    hl = dfholidays.values.tolist()
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    businesshrs1 = businesstimedelta.Rules([workday1, saturday1, holidays])

    def BusHrs1(start, end):
        if end>=start:
            return float(businesshrs1.difference(start,end).hours)*float(60)+float(businesshrs1.difference(start,end).seconds)/float(60)
        else:
            return ""
        
    filtercolumns = ["odsc_createdby_Order Printed","odsc_createdby_Frame Sent to Lens Store","odsc_createdby_Frame Sent to Overseas Desk",
                     "odsc_createdby_Sent to Control Room"]

    mainstoredf = data[data[filtercolumns].apply(lambda x: x.isin(['main1', 'main2', 'main3']).any(), axis=1)]
    designerdf = data[data[filtercolumns].apply(lambda x: x.isin(['designer1', 'designer2', 'designer3']).any(), axis=1)]

    # hq items out of stock
    data['hq_sl_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_HQ Item Out of Stock Getting It"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    data['hq_sc_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_HQ Item Out of Stock Getting It"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    data['hq_ov_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_HQ Item Out of Stock Getting It"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    print('hq items out of stock done')

    # main store
    mainstoredf['so_op_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Order Printed"]), axis=1)
    mainstoredf['op_sl_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    mainstoredf['op_sc_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    mainstoredf['op_ov_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    mainstoredf['op_hq_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_HQ Item Out of Stock Getting It"]), axis=1)

    mainstoredf['so_sl_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    mainstoredf['so_sc_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    mainstoredf['so_ov_diff']=mainstoredf.apply(lambda row: BusHrs(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)

    print('mainstoredf done')

    # designer store
    designerdf['so_op_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Order Printed"]), axis=1)
    designerdf['op_sl_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    designerdf['op_sc_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Order Printed"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    designerdf['op_ov_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Order Printed"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    designerdf['op_hq_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Order Printed"], row["odsc_datetime_HQ Item Out of Stock Getting It"]), axis=1)

    designerdf['so_sl_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Lens Store"]), axis=1)
    designerdf['so_sc_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    designerdf['so_ov_diff']=designerdf.apply(lambda row: BusHrs1(row["odsc_datetime_Sales Order Created"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)

    print('designerdf done')

    maindesigner = pd.concat([mainstoredf,designerdf])
    maindesigner = maindesigner[['doc_entry','so_op_diff','op_sl_diff','op_sc_diff','op_ov_diff','op_hq_diff','so_sl_diff','so_sc_diff','so_ov_diff']]


    # lensstore
    data['sl_sc_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    data['sl_isb_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Issue blanks"]), axis=1)
    data['sl_iflb_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Issue Finished Lenses for Both Eyes"]), axis=1)
    data['sl_ov_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Lens Store"], row["odsc_datetime_Frame Sent to Overseas Desk"]), axis=1)
    data['sent_bfsl_sc_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    data['op_isb_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Issue blanks"]), axis=1)
    data['op_iflb_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["odsc_datetime_Issue Finished Lenses for Both Eyes"]), axis=1)
    
    #len store repair orders senttolenstore
    data['sent_sl_rop_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Sent to Lens Store"], row["odsc_datetime_Repair Order Printed"]), axis=1)
    data['rop_sc_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Repair Order Printed"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    data['rop_isb_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Repair Order Printed"], row["odsc_datetime_Issue blanks"]), axis=1)
    data['rop_iflb_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Repair Order Printed"], row["odsc_datetime_Issue Finished Lenses for Both Eyes"]), axis=1)
    
    #len store repair orders receivedatlenstore
    data['received_sl_rop_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Received at Lens Store"], row["odsc_datetime_Repair Order Printed"]), axis=1)
    data['sent_sl_sc_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Sent to Lens Store"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    data['received_sl_sc_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_PF & PL Received at Lens Store"], row["odsc_datetime_Sent to Control Room"]), axis=1)


    #control room
    data['sc__ssf_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Control Room"], row["odsc_datetime_Sent to Surfacing"]), axis=1)
    data['sc_sp_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Control Room"], row["odsc_datetime_Sent to Packaging"]), axis=1)
    data['sc_spqc_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Control Room"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)
    data['bsc_ssf_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Blanks Sent to Control Room"], row["odsc_datetime_Sent to Surfacing"]), axis=1)
    data['bsc_sp_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Blanks Sent to Control Room"], row["odsc_datetime_Sent to Packaging"]), axis=1)
    data['bsc_spqc_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Blanks Sent to Control Room"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)
    
    #surfacing
    data['ssf__astch_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Surfacing"], row["odsc_datetime_Surfacing Assigned to Technician"]), axis=1)
    data['astch_spqc_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Surfacing Assigned to Technician"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)
    data['ssf_spqc_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Surfacing"], row["odsc_datetime_Sent to Pre Quality"]), axis=1)

    #prequality
    data['spqc__astch_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Pre Quality"], row["odsc_datetime_Assigned to Technician"]), axis=1)
    data['spqc__stp_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Pre Quality"], row["odsc_datetime_Sent to Packaging"]), axis=1)


    #final qc
    data['astch_sp_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Assigned to Technician"], row["odsc_datetime_Sent to Packaging"]), axis=1)

    #packaging
    data['sp_stb_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Packaging"], row["odsc_datetime_Sent to Branch"]), axis=1)
    data['sp_gbstb_diff']=data.apply(lambda row: BusHrs(row["odsc_datetime_Sent to Packaging"], row["odsc_datetime_Branch Glazing Sent to Branch"]), axis=1)


    # converting to numeric
    # hq items out of stock
    data['hq_sl_diff'] = pd.to_numeric(data['hq_sl_diff'])
    data['hq_sc_diff'] = pd.to_numeric(data['hq_sc_diff'])
    data['hq_ov_diff'] = pd.to_numeric(data['hq_ov_diff'])

    # main store
    mainstoredf['so_op_diff'] = pd.to_numeric(mainstoredf['so_op_diff'])
    mainstoredf['op_sl_diff'] = pd.to_numeric(mainstoredf['op_sl_diff'])
    mainstoredf['op_sc_diff'] = pd.to_numeric(mainstoredf['op_sc_diff'])
    mainstoredf['op_ov_diff'] = pd.to_numeric(mainstoredf['op_ov_diff'])
    mainstoredf['op_hq_diff'] = pd.to_numeric(mainstoredf['op_hq_diff'])
    mainstoredf['so_sl_diff'] = pd.to_numeric(mainstoredf['so_sl_diff'])
    mainstoredf['so_sc_diff'] = pd.to_numeric(mainstoredf['so_sc_diff'])
    mainstoredf['so_ov_diff'] = pd.to_numeric(mainstoredf['so_ov_diff'])

    #designer store
    designerdf['so_op_diff'] = pd.to_numeric(designerdf['so_op_diff'])
    designerdf['op_sl_diff'] = pd.to_numeric(designerdf['op_sl_diff'])
    designerdf['op_sc_diff'] = pd.to_numeric(designerdf['op_sc_diff'])
    designerdf['op_ov_diff'] = pd.to_numeric(designerdf['op_ov_diff'])
    designerdf['op_hq_diff'] = pd.to_numeric(designerdf['op_hq_diff'])
    designerdf['so_sl_diff'] = pd.to_numeric(designerdf['so_sl_diff'])
    designerdf['so_sc_diff'] = pd.to_numeric(designerdf['so_sc_diff'])
    designerdf['so_ov_diff'] = pd.to_numeric(designerdf['so_ov_diff'])

    # lensstore
    data['sl_sc_diff'] = pd.to_numeric(data['sl_sc_diff'])
    data['sl_isb_diff'] = pd.to_numeric(data['sl_isb_diff'])
    data['sl_iflb_diff'] = pd.to_numeric(data['sl_iflb_diff'])
    data['sl_ov_diff'] = pd.to_numeric(data['sl_ov_diff'])
    data['sent_bfsl_sc_diff'] = pd.to_numeric(data['sent_bfsl_sc_diff'])
    data['op_isb_diff'] = pd.to_numeric(data['op_isb_diff'])
    data['op_iflb_diff'] = pd.to_numeric(data['op_iflb_diff'])

    #lensstorerepairs
    data['sent_sl_rop_diff'] = pd.to_numeric(data['sent_sl_rop_diff'])
    data['rop_sc_diff'] = pd.to_numeric(data['rop_sc_diff'])
    data['rop_isb_diff'] = pd.to_numeric(data['rop_isb_diff'])
    data['rop_iflb_diff'] = pd.to_numeric(data['rop_iflb_diff'])
    data['received_sl_rop_diff'] = pd.to_numeric(data['received_sl_rop_diff'])
    data['sent_sl_sc_diff'] = pd.to_numeric(data['sent_sl_sc_diff'])
    data['received_sl_sc_diff'] = pd.to_numeric(data['received_sl_sc_diff'])

    # controlroom
    data['sc__ssf_diff'] = pd.to_numeric(data['sc__ssf_diff'])
    data['sc_sp_diff'] = pd.to_numeric(data['sc_sp_diff'])
    data['sc_spqc_diff'] = pd.to_numeric(data['sc_spqc_diff'])
    data['bsc_ssf_diff'] = pd.to_numeric(data['bsc_ssf_diff'])
    data['bsc_sp_diff'] = pd.to_numeric(data['bsc_sp_diff'])
    data['bsc_spqc_diff'] = pd.to_numeric(data['bsc_spqc_diff'])

    #surfacing
    data['ssf__astch_diff'] = pd.to_numeric(data['ssf__astch_diff'])
    data['astch_spqc_diff'] = pd.to_numeric(data['astch_spqc_diff'])
    data['ssf_spqc_diff'] = pd.to_numeric(data['ssf_spqc_diff'])

    #prequality
    data['spqc__astch_diff'] = pd.to_numeric(data['spqc__astch_diff'])
    data['spqc__stp_diff'] = pd.to_numeric(data['spqc__stp_diff'])

    #packaging
    data['sp_stb_diff'] = pd.to_numeric(data['sp_stb_diff'])
    data['sp_gbstb_diff'] = pd.to_numeric(data['sp_gbstb_diff'])

    data = pd.merge(data,maindesigner,on ='doc_entry',how = 'outer')

    print(data.columns.to_list())
    print(data)

    truncate_fact = """truncate mabawa_dw.fact_dropped_orderscreenc1;"""
    truncate_fact = pg_execute(truncate_fact)
    ("Table Dropped")

    data.to_sql('fact_dropped_orderscreenc1', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    
    return 'Completed'


# get_source_dropped_orders()
# # update_source_dropped_orders()
# get_source_dropped_orders_staging()
# create_fact_dropped_orders()





# def update_source_dropped_orders_staging():

#     update_status = """
#     alter table mabawa_staging.source_droppedc1_staging add column dropped_status varchar(100);
#     """
#     update_status = pg_execute(update_status)

#     update_status1 = """
#     update mabawa_staging.source_droppedc1_staging
#     set dropped_status = 'Orders and Time Issues'
#     where "odsc_datetime_HQ Item Out of Stock Getting It" is null
#     and "odsc_usr_dept_Lenses coming from Branch Lab" is null
#     and "odsc_datetime_Repair Order Printed"  is null
#     """
#     update_status1 = pg_execute(update_status1)
    
#     update_status2 = """
#     update mabawa_staging.source_droppedc1_staging
#     set dropped_status = 'HQ Items Out of Stock'
#     where "odsc_datetime_HQ Item Out of Stock Getting It" is not null
#     """
#     update_status2 = pg_execute(update_status2)
    
#     update_status3 = """
#     update mabawa_staging.source_droppedc1_staging
#     set dropped_status = 'Lenses Out of Stock'
#     where "odsc_usr_dept_Lenses coming from Branch Lab" is not null
#     """
#     update_status3 = pg_execute(update_status3)

#     update_status4 = """
#     update mabawa_staging.source_droppedc1_staging
#     set dropped_status = 'Repair Order'
#     where "odsc_datetime_Repair Order Printed" is not null
#     """
#     update_status4 = pg_execute(update_status4)

#     update_status5 = """
#     update mabawa_staging.source_droppedc1_staging
#     set dropped_status = 'Rejections'
#     where "odsc_datetime_Rejected Order Received At Control Room" is not null
#     """
#     update_status5 = pg_execute(update_status5)

#     update_status6 = """
#     update mabawa_staging.source_droppedc1_staging
#     set dropped_status = 'Rejections'
#     where "odsc_datetime_Rejected Lenses sent to Lens Store" is not null
#     """
#     update_status6 = pg_execute(update_status6)

#     update_status7 = """
#     update mabawa_staging.source_droppedc1_staging
#     set dropped_status = 'Rejections'
#     where "odsc_datetime_Rejected Frame sent to Frame Store" is not null
#     """
#     update_status7 = pg_execute(update_status7)

#     update_status8 = """
#     update mabawa_staging.source_droppedc1_staging
#     set dropped_status = 'Rejections'
#     where "odsc_usr_dept_Rejected Order Sent To Control Room" is not null
#     """
#     update_status8 = pg_execute(update_status8)

#     update_status9 = """
#     update mabawa_staging.source_droppedc1_staging
#     set dropped_status = 'Rejections'
#     where "odsc_usr_dept_Damage Sent to Control Rooom" is not null
#     """
#     update_status9 = pg_execute(update_status9)

#     return 'something'