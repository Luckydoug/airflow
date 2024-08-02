import sys
sys.path.append(".")
import psycopg2
import datetime
import pandas as pd
from airflow.models import Variable
import datetime
import pandas as pd
import businesstimedelta
import holidays as pyholidays
from workalendar.africa import Kenya
from sub_tasks.data.connect import (pg_execute, engine) 

conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")


def create_source_orderscreenc1_overseas():
    query = """
    truncate mabawa_staging.source_orderscreenc1_overseas;
    insert into mabawa_staging.source_orderscreenc1_overseas
    SELECT 
        s.doc_entry, so.doc_no,
        odsc_date, 
        odsc_time_int, odsc_time, 
        odsc_datetime, 
        extract(dow from odsc_datetime::date) as day_of_week,
        (case when extract(dow from odsc_datetime::date) in (1,2,3, 4, 5)
        and odsc_time > '17:30:00'
        then (date_trunc('day', odsc_datetime)+ interval '1 day')::date + '09:00:00'::time
        when extract(dow from odsc_datetime::date) in (6)
        and odsc_time > '15:30:00'
        then (date_trunc('day', odsc_datetime)+ interval '2 day')::date + '09:00:00'::time
        when extract(dow from odsc_datetime::date) in (0)
        then (date_trunc('day', odsc_datetime)+ interval '1 day')::date + '09:00:00'::time
        else date_trunc('day', odsc_datetime)::date + '09:00:00'::time
        end) as check_datetime,
        odsc_status, odsc_new_status, rownum, 
        odsc_doc_no, odsc_createdby, odsc_usr_dept, 
        is_dropped
    FROM mabawa_staging.source_orderscreenc1_staging4 s 
    left join mabawa_staging.source_orderscreen so on s.doc_entry = so.doc_entry    
    where so.ods_ordercriteriastatus not in ('Contact Lens from Overseas') 
    """
    query = pg_execute(query)

    return "something"
    print(query)
    

def update_source_orderscreenc1_overseas():

    query = """
    update mabawa_staging.source_orderscreenc1_overseas o 
    set odsc_new_status = 'Order Printed'
    where odsc_status = 'Order Printed at Overseas'
    """
    query = pg_execute(query)

def transpose_overseas():
    
    data = pd.read_sql("""
                select v.doc_entry,v.doc_no,v.odsc_date,v.odsc_time_int,v.odsc_time,
                v.odsc_datetime,v.check_datetime,v.order_date,v.check_order_date,
                v.branch_name,v.delivery_date,v.odsc_status,v.odsc_new_status,
                v.rownum,v.odsc_doc_no,v.odsc_createdby,odsc_usr_dept,v.is_dropped,
                so.ods_ordercriteriastatus,so.ods_ordertype  from mabawa_staging.v_transpose_query v
                left join mabawa_staging.source_orderscreen so on so.doc_no = v.doc_no 
                where so.ods_ordercriteriastatus not in ('Contact Lens from Overseas') 
                and v.doc_entry not in 
                (select doc_entry  from mabawa_staging.source_orderscreenc1 so2 
                where odsc_date::date >= '2024-03-01'
                and odsc_status = 'Overseas Damages Transferred To Damage Store')           
    """, con=engine)

    print("Data Fetched")
    print(data)

    data = data.pivot_table(index='doc_entry', columns=['odsc_new_status'], values=['odsc_datetime', 'check_datetime','order_date', 'check_order_date', 'branch_name', 'delivery_date', 'odsc_createdby', 'odsc_usr_dept', 'is_dropped'], aggfunc='min')
    
    print("Pivoting Complete")

    # rename columns
    def rename(col):
        if isinstance(col, tuple):
            col = '_'.join(str(c) for c in col)
        return col
    
    data.columns = map(rename, data.columns)

    print("Columns Renamed")

    data['doc_entry'] = data.index
    print("Indexed")

    drop_table = """truncate mabawa_staging.source_orderscreenc1_overseas_trans;"""
    drop_table = pg_execute(drop_table)
    print("Dropped")

    data.to_sql('source_orderscreenc1_overseas_trans', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)

    return "Data Transposed"
    print(data)

def create_fact_orderscreenc1_overseas():

    data = pd.read_sql("""
    SELECT holiday_date, holiday_name
    FROM mabawa_dw.dim_holidays;
    """, con=engine)

    df = pd.read_sql("""
    SELECT *
    FROM mabawa_staging.source_orderscreenc1_overseas_trans;
    """, con=engine)

    print(df.columns)


    workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(18),
        working_days=[0,1, 2, 3, 4])

    saturday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(16),
        working_days=[5])
    
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    cal = Kenya()
    #hl = cal.holidays(2021)
    hl = data.values.tolist()
    #print(hl)
    my_dict=dict(hl)
    vic_holidays=vic_holidays.append(my_dict)

    businesshrs = businesstimedelta.Rules([workday, saturday, holidays])

    def BusHrs(start, end):
        if end>=start:
            return float(businesshrs.difference(start,end).hours)*float(60)+float(businesshrs.difference(start,end).seconds)/float(60)
        else:
            return ""

    print ("Data Fetched")

    df["odsc_datetime_Frame Sent to Overseas Desk"] = pd.to_datetime(df["odsc_datetime_Frame Sent to Overseas Desk"])
    df["odsc_datetime_Branch Frame Sent to Overseas Desk"] = pd.to_datetime(df["odsc_datetime_Branch Frame Sent to Overseas Desk"])
    df["odsc_datetime_PF Sent to Overseas Desk"] = pd.to_datetime(df["odsc_datetime_PF Sent to Overseas Desk"])
    df["odsc_datetime_Order Printed"] = pd.to_datetime(df["odsc_datetime_Order Printed"])
    df["odsc_datetime_Sent to Control Room"] = pd.to_datetime(df["odsc_datetime_Sent to Control Room"])
    df["odsc_datetime_Product Return Received at Overseas"] = pd.to_datetime(df["odsc_datetime_Product Return Received at Overseas"])
    
    
    df["order_date_Frame Sent to Overseas Desk"] = pd.to_datetime(df["order_date_Frame Sent to Overseas Desk"])
    df["order_date_Branch Frame Sent to Overseas Desk"] = pd.to_datetime(df["order_date_Branch Frame Sent to Overseas Desk"])
    df["order_date_PF Sent to Overseas Desk"] = pd.to_datetime(df["order_date_PF Sent to Overseas Desk"])
    df["order_date_Order Printed"] = pd.to_datetime(df["order_date_Order Printed"])
    

    df["check_datetime_Frame Sent to Overseas Desk"] = pd.to_datetime(df["check_datetime_Frame Sent to Overseas Desk"])
    df["check_datetime_Branch Frame Sent to Overseas Desk"] = pd.to_datetime(df["check_datetime_Branch Frame Sent to Overseas Desk"])
    df["check_datetime_PF Sent to Overseas Desk"] = pd.to_datetime(df["check_datetime_PF Sent to Overseas Desk"])
    df["check_datetime_Order Printed"] = pd.to_datetime(df["check_datetime_Order Printed"])
    
    df["check_order_date_Frame Sent to Overseas Desk"] = pd.to_datetime(df["check_order_date_Frame Sent to Overseas Desk"])
    df["check_order_date_Branch Frame Sent to Overseas Desk"] = pd.to_datetime(df["check_order_date_Branch Frame Sent to Overseas Desk"])
    df["check_order_date_PF Sent to Overseas Desk"] = pd.to_datetime(df["check_order_date_PF Sent to Overseas Desk"])
    df["check_order_date_Order Printed"] = pd.to_datetime(df["check_order_date_Order Printed"])
    df["delivery_date_Sent to Control Room"] = pd.to_datetime(df["delivery_date_Sent to Control Room"])
    

    df['odsc_datetime_Generated PO'] = pd.to_datetime(df['odsc_datetime_Generated PO'])
    
    print("Identified Dates")

    # overseas novax 
    df['op_po_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["order_date_Order Printed"]), axis=1)
    df['fsod_po_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Overseas Desk"], row["order_date_Frame Sent to Overseas Desk"]), axis=1)
    df['bfsod_po_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Branch Frame Sent to Overseas Desk"], row["order_date_Branch Frame Sent to Overseas Desk"]), axis=1)
    df['pfsod_po_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF Sent to Overseas Desk"], row["order_date_PF Sent to Overseas Desk"]), axis=1)


    """Rejected Jobs - Product Return Received at Overseas """
    df['op_prrod_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Product Return Received at Overseas"], row["order_date_Order Printed"]), axis=1)
    df['f_prrod_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Product Return Received at Overseas"], row["order_date_Frame Sent to Overseas Desk"]), axis=1)
    df['bf_prrod_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Product Return Received at Overseas"], row["order_date_Branch Frame Sent to Overseas Desk"]), axis=1)
    df['pf_prrod_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Product Return Received at Overseas"], row["order_date_PF Sent to Overseas Desk"]), axis=1)
    df['prrod_diff'] = df['op_prrod_diff'].combine_first(df['f_prrod_diff']).combine_first(df['bf_prrod_diff']).combine_first(df['pf_prrod_diff'])


 
    df['check_op_po_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Order Printed"], row["order_date_Order Printed"]), axis=1)
    df['check_fsod_po_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Frame Sent to Overseas Desk"], row["order_date_Frame Sent to Overseas Desk"]), axis=1)
    df['check_bfsod_po_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Branch Frame Sent to Overseas Desk"], row["order_date_Branch Frame Sent to Overseas Desk"]), axis=1)
    df['check_pfsod_po_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_PF Sent to Overseas Desk"], row["order_date_PF Sent to Overseas Desk"]), axis=1)


    """Rejected Jobs - Product Return Received at Overseas """
    df['check_op_prrod_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Product Return Received at Overseas"], row["order_date_Order Printed"]), axis=1)
    df['check_fprrod_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Product Return Received at Overseas"], row["order_date_Frame Sent to Overseas Desk"]), axis=1)
    df['check_bfprrod_bfdiff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Product Return Received at Overseas"], row["order_date_Branch Frame Sent to Overseas Desk"]), axis=1)
    df['check_pfsod_prrod_diff']=df.apply(lambda row: BusHrs(row["odsc_datetime_Product Return Received at Overseas"], row["order_date_PF Sent to Overseas Desk"]), axis=1)
    df['check_prrod_po_diff'] = df['check_op_po_diff'].combine_first(df['check_fsod_po_diff']).combine_first(df['check_bfsod_po_diff']).combine_first(df['check_pfsod_po_diff'])

    print("Time Calculation Completed - Novax")

    ##Replace odsc_datetime_Sent to Control Room with "odsc_datetime_PF to Follow Sent to Overseas Desk" since PF to follow takes time 
    df["delivery_date_Sent to Control Room"] = df.apply(lambda row: row['odsc_datetime_PF to Follow Sent to Overseas Desk'] 
                                                            if pd.notna(row['odsc_datetime_PF to Follow Sent to Overseas Desk']) else
                                                            row['delivery_date_Sent to Control Room'], axis=1)
    
    df["delivery_date_Sent to Control Room"] = df.apply(lambda row: row["odsc_datetime_Generated GRPO"] 
                                                        if (row["delivery_date_Sent to Control Room"] > 
                                                        row["odsc_datetime_Sent to Control Room"]) else 
                                                        row["delivery_date_Sent to Control Room"],axis = 1)


    # dhl to sent to control room
    df['dhl_sc']=df.apply(lambda row: BusHrs(row["delivery_date_Sent to Control Room"], row["odsc_datetime_Sent to Control Room"]), axis=1)
    
    print ("Time Difference - DHL")

    # overseas novax
    df['op_po_diff'] = pd.to_numeric(df['op_po_diff'])
    df['fsod_po_diff'] = pd.to_numeric(df['fsod_po_diff'])
    df['bfsod_po_diff'] = pd.to_numeric(df['bfsod_po_diff'])
    df['pfsod_po_diff'] = pd.to_numeric(df['pfsod_po_diff'])
    df['op_prrod_diff']=pd.to_numeric(df['op_prrod_diff'])
    df['f_prrod_diff']=pd.to_numeric(df['f_prrod_diff'])
    df['bf_prrod_diff']=pd.to_numeric(df['bf_prrod_diff'])
    df['pf_prrod_diff']=pd.to_numeric(df['pf_prrod_diff'])
    df['prrod_diff'] = pd.to_numeric(df['prrod_diff'])

    df['check_op_po_diff'] = pd.to_numeric(df['check_op_po_diff'])
    df['check_fsod_po_diff'] = pd.to_numeric(df['check_fsod_po_diff'])
    df['check_bfsod_po_diff'] = pd.to_numeric(df['check_bfsod_po_diff'])
    df['check_pfsod_po_diff'] = pd.to_numeric(df['check_pfsod_po_diff'])
    df['check_op_prrod_diff']=pd.to_numeric(df['check_op_prrod_diff'])
    df['check_fprrod_diff']=pd.to_numeric(df['check_fprrod_diff'])
    df['check_bfprrod_bfdiff']=pd.to_numeric(df['check_bfprrod_bfdiff'])
    df['check_pfsod_prrod_diff']=pd.to_numeric(df['check_pfsod_prrod_diff'])
    df['check_prrod_po_diff'] = pd.to_numeric(df['check_prrod_po_diff'])

    # dhl to sent to control room
    df['dhl_sc'] = pd.to_numeric(df['dhl_sc'])

    def ontime(row):
        if row["order_date_Branch Frame Sent to Overseas Desk"] <= row["check_order_date_Branch Frame Sent to Overseas Desk"]:
            return 0
        elif row["order_date_Frame Sent to Overseas Desk"] <= row["check_order_date_Frame Sent to Overseas Desk"]:
            return 0
        elif row["order_date_PF Sent to Overseas Desk"] <= row["check_order_date_PF Sent to Overseas Desk"]:
            return 0
        elif row["order_date_Order Printed"] <= row["check_order_date_Order Printed"]:
            return 0
        else:
            return 1
    df['new_diff'] = df.apply(ontime,axis=1)    
            
    print("Converted Numerics")

    query = """truncate table mabawa_dw.fact_orderscreenc1_overseas;"""
    query = pg_execute(query)
    
    df.to_sql('fact_orderscreenc1_overseas', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)
    
    return "something"


def index_fact_orderscreenc1_approvals():

    add_key = """alter table mabawa_dw.fact_orderscreenc1_approvals add PRIMARY KEY (doc_entry)"""
    add_key = pg_execute(add_key)

    return "something"

def oldupdate():

    query = """
    update mabawa_staging.source_orderscreenc1_overseas o 
    set check_datetime = (date_trunc('day', check_datetime)+ interval '1 day')::date + '09:00:00'::time
    from mabawa_dw.dim_holidays h 
    where o.check_datetime::date = h.holiday_date::date
    and extract(dow from odsc_datetime::date) in (1,2,3,4,5)
    """
    query = pg_execute(query)

    query2 = """
     update mabawa_staging.source_orderscreenc1_overseas o 
    set check_datetime = (date_trunc('day', check_datetime)+ interval '2 day')::date + '09:00:00'::time
    from mabawa_dw.dim_holidays h 
    where o.check_datetime::date = h.holiday_date::date
    and extract(dow from odsc_datetime::date) in (6)
    """
    query2 = pg_execute(query2)

    return "something"

