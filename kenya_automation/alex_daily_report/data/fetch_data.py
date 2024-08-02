from airflow.models import Variable
from sqlalchemy import create_engine
import psycopg2
from datetime import date
import datetime
import pytz
import businesstimedelta
import pandas as pd
import holidays as pyholidays
from workalendar.africa import Kenya
import pygsheets
import mysql.connector as database
import urllib.parse
import datetime
from sub_tasks.libraries.utils import createe_engine
from reports.draft_to_upload.utils.utils import return_report_daterange
from reports.draft_to_upload.utils.utils import get_report_frequency
from sub_tasks.libraries.utils import fourth_week_start, fourth_week_end

conn = createe_engine()

selection = get_report_frequency()

if selection == 'Weekly':
    start_date = fourth_week_start
    yesterday = fourth_week_end
elif selection == 'Daily':
    start_date = return_report_daterange(selection)
    yesterday = start_date

print(start_date)
print(yesterday)

# today = datetime.date.today()
# start_date = today - datetime.timedelta(days=1)
# yesterday = today - datetime.timedelta(days=1)
# formatted_date = yesterday.strftime('%Y-%m-%d')
# print(start_date)
# print(yesterday)

from sub_tasks.data.connect import (pg_execute, engine) 

def repdata(database, engine):
    query = f"""
    with replacement as (
    select sid.replaced_itemcode as "Item No.",i.item_desc as "Item/Service Description",sid.itr__status as "ITR Status",si.doc_no as "ITR Number",si.internal_no as "Internal Number",si.post_date as "ITR Date",
    si.exchange_type as "Exchange Type",sid.sales_orderno as "Sales Order number",sid.sales_order_entry as "Sales Order entry",
    sid.sales_order_branch as "Sales Branch",sid.draft_order_entry as "Draft order entry",sid.draft_orderno as "Order Number",si.createdon as "Creation Date",
    si.creationtime_incl_secs as "Creatn Time - Incl. Secs",sid.picker_name as "Picker Name"
    from {database}_staging.source_itr_details sid
    inner join {database}_staging.source_itr si on si.internal_no = sid.doc_internal_id 
    left join {database}_staging.source_items i on i.item_code = sid.replaced_itemcode
    where si.exchange_type = 'Replacement') 
    select * from replacement 
    where "ITR Date"::date between '{yesterday}' and '{yesterday}'
    and "Sales Branch" <> ''
    """
    Repdata =  pd.read_sql_query(query, con=engine) 
    return Repdata

def to_warehouse(database, engine):
    query1 =  f""" 
    select doc_no as "Document Number",si.internal_no as "Internal Number",doc_status as "Document Status", post_date as "Posting Date", 
    exchange_type as "Exchange Type", remarks as "Remarks", filler as "Filter",to_warehouse_code as "To Warehouse Code",
    createdon as "Creation Date", to_char(creationtime_incl_secs, 'FM999:09:99'::text) AS "Creatn Time - Incl. Secs"
    from {database}_staging.source_itr si  
    inner join {database}_staging.source_users su on su.user_signature = si.user_signature 
    where createdon::date between '{yesterday}' and '{yesterday}'
    and su.user_code in ('BRS','manager')
    """
    return pd.read_sql_query(query1,con=engine)

def stb_rab_data(database, engine):
    delays = f"""
            with orders as 
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_a, odsc_lineid, odsc_date::date, odsc_time, odsc_createdby as Desk,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as draft_dt,
                odsc_status as draft_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('Draft Order Created')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                preauth_intiated as
                (select *
                from
                (select row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_b, odsc_lineid, odsc_date::date, odsc_time, odsc_createdby as Desk,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as preauth_dt,
                odsc_status as preauth_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('Pre-Auth Initiated For Optica Insurance')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                smartforwarded as 
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_c, odsc_lineid, odsc_date::date, odsc_time, odsc_createdby as Desk,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as smart_dt,
                odsc_status as smart_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('SMART Forwarded to Approvals Team','Customer Confirmed Order')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                saleorders as 
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_d, odsc_lineid, odsc_date::date, odsc_time, odsc_createdby as Desk,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as so_dt,
                odsc_status as so_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('Sales Order Created','Confirmed by Approvals Team')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                printedpf as 
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_e, odsc_lineid, odsc_date::date, odsc_time, odsc_createdby as Desk,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as printedpf_dt,
                odsc_status as printedpf_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('Printed PF Identifier','Printed Lens Identifier','Printed Frame and Lens Identifier')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                pfsenttohq as 
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_f, odsc_lineid, odsc_date::date, odsc_time, odsc_createdby as Desk,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as pfsenttohq_dt,
                odsc_status as pfsenttohq_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('PF Sent to HQ','PL Sent to HQ','PF & PL Sent to HQ')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                pfreceivedathq as 
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_g, odsc_lineid, odsc_date::date, odsc_time, odsc_createdby as Desk,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as frameatlenstore_dt,
                odsc_status as frameatlenstore_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('PF Sent to Lens Store','PL Sent to Lens Store','PF & PL Sent to Lens Store','Frame Sent to Lens Store','PF Sent to Overseas Desk','Frame Sent to Overseas Desk')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                pfreceivedatlenstore as 
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_h, odsc_lineid, odsc_date::date, odsc_time, odsc_createdby as Desk,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as pfreceivedatlenstore_dt,
                odsc_status as pfreceivedatlenstore_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('PF Received from Receiver','PF & PL Received at Lens Store')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                orderprinted_main as
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_i, odsc_lineid, odsc_date::date, odsc_time, odsc_createdby as Desk,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as opmain_dt,
                odsc_status as opmain_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('Order Printed')
                and odsc_createdby in ('main1','main2','main3')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                orderprinted_designer as 
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_j, odsc_lineid, odsc_date::date, odsc_time, odsc_createdby as Desk,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as opdesigner_dt,
                odsc_status as opdesigner_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('Order Printed')
                and odsc_createdby in ('designer1','designer2','designer3')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                orderprinted_lens as 
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_k, odsc_lineid, odsc_date::date, odsc_time, odsc_createdby as Desk,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as oplens_dt,
                odsc_status as oplens_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('Order Printed')
                and odsc_createdby in ('lens1','lens2','lens3','overseas1','overseas2','overseas3')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                grpo as
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_l, odsc_lineid, odsc_date::date, odsc_time,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as grpo_dt,
                odsc_status as grpo_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('Generated GRPO')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                gpo as 
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_m, odsc_lineid, odsc_date::date, odsc_time,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as gpo_dt,
                odsc_status as gpo_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('Generated PO to KE','Generated PO')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                stc as 
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_n, odsc_lineid, odsc_date::date, odsc_time,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as stc_dt,
                odsc_status as stc_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('Sent to Control Room')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                senttopreqc as 
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_o, odsc_lineid, odsc_date::date, odsc_time,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as senttopreqc_dt,     
                odsc_status as senttopreqc_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('Sent to Pre Quality')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                assignedtotech as 
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_p, odsc_lineid, odsc_date::date, odsc_time,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as assignedtotech_dt,
                odsc_status as assignedtotech_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('Assigned to Technician')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ), 
                stp as
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_q, odsc_lineid, odsc_date::date, odsc_time,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as stp_dt,
                odsc_status as stp_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('Sent to Packaging')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                stb as 
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_r, odsc_lineid, odsc_date::date, odsc_time,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as stb_dt,
                odsc_status as stb_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('Sent to Branch')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                rab as 
                (select * 
                from
                (select 
                row_number() over (partition by doc_entry order by odsc_lineid asc) as r,
                doc_entry as doc_entry_s, odsc_lineid, odsc_date::date, odsc_time,
                odsc_date::date + (lpad(odsc_time::text, 4, '0')::time) as rab_dt,
                odsc_status as rab_status
                from {database}_staging.source_orderscreenc1
                where odsc_status in ('Received at Branch','Damaged PF/PL Received at Branch','Sent For Home Delivery')
                order by odsc_lineid asc
                ) a
                where a.r = 1
                ),
                allorders as 
                (select * from orders o 
                full outer join preauth_intiated on preauth_intiated.doc_entry_b = o.doc_entry_a
                full outer join smartforwarded on smartforwarded.doc_entry_c = o.doc_entry_a
                full outer join saleorders on saleorders.doc_entry_d = o.doc_entry_a    
                full outer join printedpf on printedpf.doc_entry_e = o.doc_entry_a
                full outer join pfsenttohq on pfsenttohq.doc_entry_f = o.doc_entry_a
                full outer join pfreceivedathq on pfreceivedathq.doc_entry_g = o.doc_entry_a
                full outer join pfreceivedatlenstore on pfreceivedatlenstore.doc_entry_h = o.doc_entry_a
                full outer join orderprinted_main on orderprinted_main.doc_entry_i = o.doc_entry_a
                full outer join orderprinted_designer on orderprinted_designer.doc_entry_j = o.doc_entry_a
                full outer join orderprinted_lens on orderprinted_lens.doc_entry_k = o.doc_entry_a
                full outer join grpo on grpo.doc_entry_l = o.doc_entry_a
                full outer join gpo on gpo.doc_entry_m = o.doc_entry_a
                full outer join stc on stc.doc_entry_n = o.doc_entry_a
                full outer join senttopreqc on senttopreqc.doc_entry_o = o.doc_entry_a
                full outer join assignedtotech on assignedtotech.doc_entry_p = o.doc_entry_a   
                full outer join stp on stp.doc_entry_q = o.doc_entry_a
                full outer join stb on stb.doc_entry_r = o.doc_entry_a
                full outer join rab on rab.doc_entry_s = o.doc_entry_a
                ),
                summary as 
                (select * 
                from 
                (select o.doc_entry_a,
                        so.ods_outlet,
                        so.ods_orderprocess_branch,
                        so.doc_no,
                        so.ods_creator,
                        su.user_name,
                        so.ods_status,
                        SO.ods_normal_repair_order,
                        so.ods_ordercriteriastatus,
                        so.ods_insurance_order,
                        case when so.ods_insurance_order = 'No' then 'Cash' else 'Insurance' end as "Mode of Pay",
                        so.ods_delivery_date::date + interval '1 day' as ods_delivery_date,
                        so.ods_delivery_time,
                        draft_dt,
                        draft_status,
                        preauth_dt,
                        preauth_status,
                        smart_dt,
                        smart_status,
            --            case when smart_dt > so_dt then draft_dt else smart_dt end as smart_dt1,
                        so_dt,
                        so_status,
                        printedpf_dt,
                        printedpf_status,
                        pfsenttohq_dt,
                        pfsenttohq_status,
                        frameatlenstore_dt,
                        frameatlenstore_status,
                        pfreceivedatlenstore_dt,
                        pfreceivedatlenstore_status,
                        opmain_dt,
                        opmain_status,
                        opdesigner_dt,
                        opdesigner_status,
                        oplens_dt,
                        oplens_status,
                        grpo_dt,
                        grpo_status,
                        gpo_dt,
                        gpo_status,	
                        stc_dt,
                        stc_status,
                        senttopreqc_dt,
                        senttopreqc_status,
                        assignedtotech_dt,
                        assignedtotech_status,
                        stp_dt,
                        stp_status,
                        stb_dt,
                        stb_status,
                        rab_dt,
                        rab_status,
                        so.order_hours,
                        m.riders_time,
                        m.transporter,
                        CASE
                            WHEN (m.transporter = 'Upcountry' and so.ods_ordercriteriastatus not like ('%Glazed at Branch%')) THEN DATE_TRUNC('day', stb_dt) + INTERVAL '18 hours'
                            ELSE stb_dt
                        END AS adjusted_stb
                from allorders o
                left join {database}_staging.source_orderscreen so on so.doc_entry = o.doc_entry_a
                left join {database}_staging.source_users su on su.user_code = so.ods_creator
                left join {database}_staging.source_transporters_matrix m on so.ods_outlet = m.branch_code) d
                left join 
                (SELECT
                    orders.doc_no::INT AS "DocNum",
                    insurance_company.insurance_name AS "Insurance Company",
                    plan.plan_scheme_name AS "Insurance Scheme",
                    plan.plan_scheme_type AS "Scheme Type",
                    ods_insurance_feedback1 AS "Feedback 1",
                    ods_insurance_feedback2 AS "Feedback 2"
                FROM
                    {database}_staging.source_orderscreen AS orders
                    LEFT JOIN {database}_staging.source_users AS users ON CAST(orders.ods_creator AS TEXT) = CAST(users.se_optom AS TEXT)
                    INNER JOIN {database}_staging.source_web_payments AS payments ON orders.doc_entry::TEXT = payments.unique_id::TEXT AND payments.payment_mode = 'Insurance'
                    LEFT JOIN {database}_staging.source_insurance_company AS insurance_company ON payments.insurance_company_name::TEXT = insurance_company.insurance_code::TEXT
                    LEFT JOIN {database}_staging.source_insurance_plan AS plan ON insurance_company.id = plan.insurance_id AND plan.plan_scheme_name = payments.insurance_scheme
                ) scheme_type 
                ON scheme_type."DocNum" = d.doc_no),
            summarydata as       
                (select 
                    doc_entry_a as doc_entry,  
                    doc_no,
                    ods_outlet,
                    ods_orderprocess_branch,
                    ods_creator,
                    user_name,
                    ods_status,
                    ods_normal_repair_order,
                    ods_ordercriteriastatus,
                    ods_insurance_order,
                    "Insurance Company",
                    "Scheme Type",
                    case when smart_dt > so_dt then draft_dt else smart_dt end as smart_dt1,
                    ods_delivery_date::date + (lpad(ods_delivery_time::text, 6, '0')::time) as collection_dt,
            --    	case when "Scheme Type" = 'DIRECT ONLY' then preauth_dt else start_cal_coll_time end as start_cal_coll_time1,
                    draft_dt,
                    draft_status,
                    preauth_dt,
                    preauth_status,
                    smart_dt,
                    smart_status,
                    so_dt,
                    so_status,
                    printedpf_dt,
                    printedpf_status,
                    pfsenttohq_dt,
                    pfsenttohq_status,
                    frameatlenstore_dt,
                    frameatlenstore_status,
                    pfreceivedatlenstore_dt,
                    pfreceivedatlenstore_status,
                    opmain_dt,
                    opmain_status,
                    opdesigner_dt,
                    opdesigner_status,
                    oplens_dt,
                    oplens_status,
                    grpo_dt,
                    grpo_status,
                    gpo_dt,
                    gpo_status,	
                    stc_dt,
                    stc_status,
                    senttopreqc_dt,
                    senttopreqc_status,
                    assignedtotech_dt,
                    assignedtotech_status,
                    stp_dt,
                    stp_status,
                    stb_dt,
                    stb_status,
                    rab_dt,
                    rab_status,
                    order_hours,
                    riders_time,
                    adjusted_stb
                from summary
                where ods_status <> 'Cancel'
                and ods_normal_repair_order not in ('OTC','Repair Order')
                and ods_orderprocess_branch = 'HQWS')
                select doc_entry,	   		
                        doc_no,
                        ods_outlet,
                        case when opmain_dt is not null then '0MA' 
                            when opdesigner_dt is not null then '0DS' 
                            when oplens_dt is not null then '0LE' 
                            else ods_outlet
                        end as department,
                        ods_orderprocess_branch,
                        ods_creator,
                        user_name,
                        ods_status,
                        ods_normal_repair_order,
                        ods_ordercriteriastatus,
                        ods_insurance_order,
                        "Insurance Company",
                        "Scheme Type",
                        collection_dt,
                        case when ods_insurance_order = 'No' then draft_dt 
                            when "Scheme Type" = 'DIRECT ONLY' then preauth_dt
                                    else smart_dt1 end as "start_cal_coll_time",
                        draft_dt,
                        draft_status,
                        preauth_dt,
                        preauth_status,            
                        smart_dt,
                        smart_status,
                        smart_dt1,
                        so_dt,
                        so_status,
                        printedpf_dt,
                        printedpf_status,
                        pfsenttohq_dt,
                        pfsenttohq_status,
                        frameatlenstore_dt,
                        frameatlenstore_status,
                        pfreceivedatlenstore_dt,
                        pfreceivedatlenstore_status,
                        opmain_dt,
                        opmain_status,
                        opdesigner_dt,
                        opdesigner_status,
                        oplens_dt,
                        oplens_status,
                        grpo_dt,
                        grpo_status,
                        gpo_dt,
                        gpo_status,	
                        stc_dt,
                        stc_status,
                        senttopreqc_dt,
                        senttopreqc_status,
                        assignedtotech_dt,
                        assignedtotech_status,
                        stp_dt,
                        stp_status,
                        stb_dt,
                        stb_status,
                        rab_dt,
                        rab_status,
                        order_hours,
                        riders_time,
                        adjusted_stb,
                        case when stb_dt is null then ''
                            when stb_dt <= collection_dt then 'Yes' else 'No' end as "STB On Time",
                    case when rab_dt is null then '' 
                            when rab_dt <= collection_dt and stb_dt <= collection_dt then 'Yes' else 'No' end as "RAB On Time"
                            from summarydata
            where collection_dt::date between '{start_date}' and '{yesterday}' 
 """
    return pd.read_sql_query(delays,con=engine)


def replacement(database,engine):
    replacements = f"""
        with plp as (
            select sid.replaced_itemcode as "Item No.",i.item_desc as "Item/Service Description",sid.itr__status as "ITR Status",si.doc_no as "ITR Number",si.internal_no as "Internal Number",si.post_date as "ITR Date",
            si.exchange_type as "Exchange Type",sid.sales_orderno as "Sales Order number",sid.sales_order_entry as "Sales Order entry",
            sid.sales_order_branch as "Sales Branch",sid.draft_order_entry as "Draft order entry",sid.draft_orderno as "Order Number",si.createdon as "Creation Date",
            si.creationtime_incl_secs as "Creatn Time - Incl. Secs",sid.picker_name as "Picker Name"
            from {database}_staging.source_itr_details sid
            inner join {database}_staging.source_itr si on si.internal_no = sid.doc_internal_id 
            left join {database}_staging.source_items i on i.item_code = sid.replaced_itemcode
            where si.createdon ::date between '{yesterday}' and '{yesterday}'
            and si.exchange_type = 'Replacement'
            and sid.sales_order_branch <> ''
        ),
        stb as 
        (
            select created_user as "Created User",post_date as "STB Date",post_time,
            to_char(post_time, 'FM999:09:99'::text)::time without time zone AS "STB Time", status as "Status", item_code as "Item Code", itr_no as "ITR No" 
            from {database}_staging.source_itr_log sil 
            where sil.status in ('Rep Sent to Branch')
            order by post_date,post_time ),
        rab as 
        (
            select created_user as "Created User",post_date as "Date",post_time,
            to_char(post_time, 'FM999:09:99'::text)::time without time zone AS "Time", status as "Status", item_code as "Item Code", itr_no as "ITR No" 
            from {database}_staging.source_itr_log sil 
            where sil.status in ('Rep Received at Branch','Rep OK')
            order by post_date,post_time 
        ),
        final as 
        (
            select * from plp p
            left join rab r on  r."ITR No"::numeric = p."Internal Number"::numeric
            left join stb s on s."ITR No"::numeric = p."Internal Number"::numeric
        ),
        summary as 
        (
            select "Item No.","Item/Service Description","ITR Status","ITR Number","Internal Number","ITR Date","Exchange Type","Sales Branch","Order Number",
            "Creation Date"::date + lpad("Creatn Time - Incl. Secs"::text,6,'0')::time as "Creation Date_Time","STB Date"::date + ("STB Time")::time as "Sent to Branch Date_Time",
            "Date"::date + ("Time")::time as "Received at Branch Date_Time"
            from final
        ),
        summary1 as 
            (select *,
            (EXTRACT(epoch FROM ("Received at Branch Date_Time" - "Creation Date_Time")) / 86400) as difference,
            round((EXTRACT(epoch FROM ("Received at Branch Date_Time" - "Creation Date_Time")) / 86400)::numeric,0) AS day_difference   
            from summary
        )
        select *,
            CASE when day_difference = 0 then '0 days'
                when  day_difference between 1 AND 2 then '1 day - 2 days'
                when day_difference between 3 and 5 then '3 days - 5 days'
                when day_difference between 6 and 7 then '6 days - 7 days'
                when day_difference is null then 'Not Received at Branch'
                        ELSE 'Greater than 7 days'
            END AS duration
            from summary1
        """
    return pd.read_sql_query(replacements,con=engine)

