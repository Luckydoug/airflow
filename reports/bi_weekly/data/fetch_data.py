import pandas as pd
from datetime import datetime, timedelta


def fetch_biweekly_kpis(start_date, end_date, engine, database, view):
    query = f"""
    with opening_time as (
    select shb.branch_code as "Branch", 
    'OVERALL' as "Payroll Number",
    count(*) as "Times Opened Late(Thresh = 0)"
    from {database}.source_opening_time sot 
    left join {database}.source_hrms_branch shb 
    on shb.branch_name::text = sot.branch::text
    where lost_time > 0
    and date between '{start_date}' and '{end_date}'
    group by 1
    ),
    active_days as (
    select al.branch as "Branch", coalesce(al.user_code,'OVERALL') as "Payroll Number",
    count(distinct(al.date)) as "Active Days"
    from {view}.all_activity al
    where "date"::date between '{start_date}' and '{end_date}'
    and al.user_code is not null
    group by grouping sets((al.branch), (al.branch, al.user_code))
    order by al.branch
    ),
    optom_conversion as (
    select a.branch_code as "Branch",
    coalesce(a.optom, 'OVERALL') as "Payroll Number",
    round((sum(case when days <= 7 then 1 else 0 end))::decimal / count(a.code) * 100, 0)::decimal as "Optom Low RX Conversion (Target = 65)"
    from
    (select row_number() over(partition by cust_code, create_date, code order by days, rx_type, code desc) as r, *
    from {view}.et_conv
    where status not in ('Cancel','Unstable', 'CanceledEyeTest', 'Hold')
    and (patient_to_ophth not in ('Yes') or patient_to_ophth is null)) as a 
    where a.r = 1
    and a.optom is not null
    and a.create_date::date between '{start_date}' and '{end_date}'
    AND "RX" = 'Low Rx'
    group by grouping sets((a.branch_code), (a.branch_code, a.optom))
    ),
    eyetest_time as (
    select eqt.outlet_id as "Branch",
    coalesce(eqt.optom_id, 'OVERALL') as "Payroll Number",
    round(avg(eqt.eye_testtime)) as "Average Eye Test Time"
    from {view}.eyetest_queue_time eqt 
    where eqt.added_to_queue2::date between '{start_date}' and '{end_date}'
    group by grouping sets ((eqt.outlet_id), (eqt.outlet_id, eqt.optom_id))
    ),
    eyetest_to_order as (
    select eto.order_branch as "Branch",
    coalesce(eto.ods_creator, 'OVERALL') as "Payroll Number",
    round((sum(case when eto.time_taken <= 45 then 1 else 0 end)::decimal / count(eto.visit_id)::decimal)::decimal * 100,0) as "Eye Test to Order Efficiency (Target = 90%% in 45 minutes)"
    from report_views.eyetest_to_order eto
    where eto.order_date::date between '{start_date}' and '{end_date}'
    group by grouping sets ((eto.order_branch), (eto.order_branch, eto.ods_creator))
    ),
    printing_identifier as (
    select pie.ods_outlet as "Branch",
    coalesce(pie.ods_creator, 'OVERALL') as "Payroll Number",
    round((sum(case when pie.time_taken <= 5 then 1 else 0 end)::decimal / count(pie.doc_entry)::decimal)::decimal * 100,0) as "Printing Identifier Efficiency (Target = 5 Mins)"
    from {view}.printing_identifier_efficiency pie
    where printed_identifier::date between '{start_date}' and '{end_date}'
    group by grouping sets ((pie.ods_outlet), (pie.ods_outlet, pie.ods_creator))
    ),
    staff_conversion as (
    SELECT 
    a.branch_code AS "Branch",
    coalesce(COALESCE(orders.ods_creator, a.view_creator, a.sales_employees, optom), 'OVERALL') 
    AS "Payroll Number",
    round((sum(case when a.days <= 7 then 1 else 0 end))::decimal / count(a.code) * 100, 0)::decimal 
    as "EWC Low RX Conversion (Target = 65)"
    FROM (
    SELECT 
    ROW_NUMBER() OVER(PARTITION BY cust_code, create_date, code ORDER BY days, rx_type, code DESC) AS r, 
    ec.branch_code, ec.view_creator, 
    ec.sales_employees, ec.optom, 
    ec.order_converted, ec.days, ec.code,
    ec.create_date
    FROM {view}.et_conv ec 
    WHERE status NOT IN ('Cancel','Unstable', 'CanceledEyeTest', 'Hold')
    AND (patient_to_ophth NOT IN ('Yes') OR patient_to_ophth IS NULL)
    AND "RX" = 'Low Rx'
    ) AS a 
    LEFT JOIN {database}.source_orderscreen orders 
    ON a.order_converted::TEXT = orders.doc_no::TEXT
    WHERE a.r = 1
    AND a.optom IS NOT NULL
    AND a.branch_code IS NOT NULL
    and a.create_date::date between '{start_date}' and '{end_date}'
    GROUP BY GROUPING SETS (
    (a.branch_code),
    (a.branch_code, COALESCE(orders.ods_creator, a.view_creator, a.sales_employees, optom)))
    ),
    nps as ( 
    select ns.branch as "Branch", 
    coalesce(ns.user_code, 'OVERALL') as "Payroll Number",
    round((((sum(case when ns.nps_rating::int in (9, 10) then 1 else 0 end)::decimal) - 
    (sum(case when ns.nps_rating::int < 7 then 1 else 0 end)::decimal)) / count(*)::decimal) * 100 ) as "NPS Score(Target = 90)"
    from {view}.nps_surveys ns
    WHERE ns.survey_id <> ''::text 
    AND ns.nps_rating ~ '^[0-9]+$'::text 
    AND ns.survey_id <> 'nan'::text 
    AND ns.response = 'Yes'::text 
    AND ns.drop = 'No'::text
    and ns.trigger_date::date between '{start_date}' and '{end_date}'
    and ns.user_code is not null
    group by grouping sets((ns.branch), (ns.branch, ns.user_code))
    ),
    google_reviews as (
    SELECT store_code AS "Branch",
    'OVERALL' as "Payroll Number",
    ROUND(SUM(case WHEN star_rating = 'THREE' THEN 3
    WHEN star_rating = 'ONE' THEN 1
    WHEN star_rating = 'FOUR' THEN 4
    WHEN star_rating = 'FIVE' THEN 5
    WHEN star_rating = 'TWO' THEN 2
    ELSE 0END)::DECIMAL / COUNT(star_rating), 2) AS "Average Rating"
    FROM {database}.google_reviews
    where createdat::date between '{start_date}' and '{end_date}'
    GROUP BY 1, 2
    ),
    sops as (
    select ods_outlet as "Branch",
    'OVERALL' as "Payroll Number",
    round(((nullif(sum(sop_count),0) / sum(cust_count))::decimal * 100)) as "SOPs/ Customers"
    FROM {view}.sop_summary
    where pk_date::date between '{start_date}' and '{end_date}'
    group by 1
    ),																		
    quality_rejected as (
    SELECT 
    orders.ods_outlet as "Branch",
    coalesce(orders.ods_creator, 'OVERALL') as "Payroll Number",
    count(a.doc_entry) as "Quality Rejected at Branch"
    FROM (
    SELECT 
    ors.odsc_date,
    ors.doc_entry,
    ors.odsc_time,
    ors.odsc_remarks,
    row_number() OVER (PARTITION BY ors.odsc_time, ors.odsc_date, ors.doc_entry) AS row_num
    FROM {database}.source_orderscreenc1 ors
    WHERE ors.odsc_status = 'Quality Rejected at Branch'
    ) a
    LEFT JOIN {database}.source_orderscreen orders 
    ON a.doc_entry::text = orders.doc_entry::text
    WHERE a.row_num = 1
    and a.odsc_date::date between '{start_date}' and '{end_date}'
    GROUP BY grouping sets((orders.ods_outlet), (orders.ods_outlet, orders.ods_creator))
    ),
    customer_issue as ( 
    SELECT 
    orders.ods_outlet as "Branch",
    coalesce(orders.ods_creator, 'OVERALL') as "Payroll Number",
    count(a.doc_entry) as "Customer Issue"
    FROM (
    SELECT 
    ors.odsc_date,
    ors.doc_entry,
    ors.odsc_time,
    ors.odsc_remarks,
    row_number() OVER (PARTITION BY ors.odsc_time, ors.odsc_date, ors.doc_entry) AS row_num
    FROM {database}.source_orderscreenc1 ors
    WHERE ors.odsc_status = 'Customer Issue During Collection'
    ) a
    LEFT JOIN {database}.source_orderscreen orders 
    ON a.doc_entry::text = orders.doc_entry::text
    WHERE a.row_num = 1
    and a.odsc_date::date between '{start_date}' and '{end_date}'
    GROUP BY grouping sets((orders.ods_outlet), (orders.ods_outlet, orders.ods_creator))
    ),
    discounts as (
    SELECT 
    sd.branch as "Branch",
    coalesce(orders.ods_creator, 'OVERALL') as "Payroll Number",
    count(a.doc_entry) as "Orders Collected and Discounted Same Day"
    FROM (
    SELECT 
    ors.odsc_date,
    ors.doc_entry,
    ors.odsc_time,
    ors.odsc_remarks,
    row_number() OVER (PARTITION BY ors.doc_entry) AS row_num
    FROM {database}.source_orderscreenc1 ors
    WHERE ors.odsc_status = 'Collected') as  a 
    left join (select doc_no, doc_entry, ods_creator  from {database}.source_orderscreen so) as orders 
    on a.doc_entry = orders.doc_entry
    inner join {database}.source_discounts sd 
    on sd.old_order_no::text = orders.doc_no::text and a.odsc_date::date = sd.create_date::date 
    where a.row_num = 1
    and a.odsc_date::date between '{start_date}' and '{end_date}'
    group by grouping sets((sd.branch),(sd.branch, ods_creator))
    ),
    insurance_conversion as (
    select 
    ifc.ods_outlet as "Branch",
    coalesce(ifc.ods_creator, 'OVERALL') as "Payroll Number",
    round((
    sum(case when ifc.fdbck_type = 'Use Available Amount on SMART' and 
    ifc.cnvrtd = 1 then 1 else 0 end)::decimal / 
    nullif(sum(case when ifc.fdbck_type = 'Use Available Amount on SMART' 
    then 1 else 0 end)::decimal,0))::decimal * 100, 0) as "Use Available Amount Conversion",
    round((
    nullif(sum(case when ifc.fdbck_type = 'Declined by Insurance Company' and 
    ifc.cnvrtd = 1 then 1 else 0 end)::decimal,0) / 
    nullif(sum(case when ifc.fdbck_type = 'Declined by Insurance Company' 
    then 1 else 0 end)::decimal,0))::decimal * 100, 0) as "Declined Conversion"
    from 
    {view}.insurance_feedback_conversion ifc 
    where fdbck_date::date between '{start_date}' and '{end_date}'
    group by grouping sets((ifc.ods_outlet),(ifc.ods_outlet,ifc.ods_creator))
    ),
    aprvl_upd_eff as (
    select ieaf.ods_outlet as "Branch",
    coalesce(ieaf.ods_creator, 'OVERALL') as "Payroll Number",
    round((sum(case when ieaf.rmrk_to_updt <= 5 then 1 else 0 end)::decimal / nullif(count(ieaf.rmrk_to_updt),0)::decimal) * 100,0) as "ApprovalReceivedInsuranceUpdateSAP"
    from {view}.insurance_efficiency_after_feedback ieaf
    where ieaf.cstmr_cntctd_tm is not null 
    and ieaf.cstmr_cntctd_tm::date between '{start_date}' and '{end_date}'
    and ieaf.ods_outlet in (select branch_code::text from reports_tables.branch_data where sends_own_insurance = 'Yes')
    group by grouping sets((ieaf.ods_outlet),(ieaf.ods_outlet, ieaf.ods_creator))
    ),
    client_concated as (
    select af.ods_outlet as "Branch",
    coalesce(af.ods_creator, 'OVERALL') as "Payroll Number",
    round((sum(case when af.apprvl_to_cstmr_cntctd <= 10 then 1 else 0 end)::decimal / nullif(count(af.apprvl_to_cstmr_cntctd)::decimal, 0)) * 100,0) as "InsuranceFeedbackToCustomerTime"
    from {view}.insurance_efficiency_after_feedback af
    where af.cstmr_cntctd_tm is not null 
    and af.cstmr_cntctd_tm::date between '{start_date}' and '{end_date}'
    group by grouping sets((af.ods_outlet),(af.ods_outlet, af.ods_creator))
    ),
    warranties as (
    select
    upper(left(trim(remarks),3)) as "Branch",
    'OVERALL' as "Payroll Number",
    sum(qty) as "Warranties Not Returned"
    from {database}.source_itr si 
    left join {database}.source_itr_details sid on si.internal_no = sid.doc_internal_id
    left join {database}.source_items si2 on sid.item_no = si2.item_code 
    where si.exchange_type = 'Warranty Return'
    and itr__status in (
    'Warranty Return List Created',
    'Warranty Return List Printed',
    'Warranty Return Sent to HQ')
    and doc_status = 'O'
    and createdon between '{start_date}' and '{end_date}'
    group by 1
    ),
    error_deductable as (
    select b."Branch",
    coalesce(b."Payroll Number", 'OVERALL') as "Payroll Number",
    count(b.*) as "Error Deductable"
    from (
    select 
    case when order_discount_category = 'SALESPERSON ERROR DEDUCTABLE' 
    then so.ods_outlet else sp.branch_code
    end as "Branch",
    case when order_discount_category = 'SALESPERSON ERROR DEDUCTABLE' 
    then so.ods_creator else sp.latest_updated_optomid
    end as "Payroll Number"
    from {database}.source_discounts sd 
    left join {database}.source_orderscreen so on trim(sd.old_order_no) = so.doc_no::text 
    left join {database}.source_users su on so.ods_creator = su.user_code
    left join {database}.source_prescriptions sp on so.presctiption_no::text = sp.code
    left join {database}.source_users su2 on sp.latest_updated_optomid = su2.user_code
    where order_discount_category in ('SALESPERSON ERROR DEDUCTABLE','OPTOM ERROR DEDUCTABLE')
    and sd.create_date between '{start_date}' and '{end_date}'
    and (case when order_discount_category = 'SALESPERSON ERROR DEDUCTABLE' 
    then so.ods_outlet else sp.branch_code end) is not null
    ) as b
    group by grouping sets((b."Branch"), (b."Branch", b."Payroll Number"))
    ),
    corrected_forms as ( 
    select cfr.order_branch as "Branch",
    coalesce(cfr.creator, 'OVERALL') as "Payroll Number",
    round((sum(case when cfr.time_taken::int <= 5 then 1 else 0 end) / nullif(count(*),0)::decimal) * 100, 0) as "CorrectedFormsResentEfficiency"
    from {database}.corrected_forms_resent cfr
    where cfr.corrected_date::date between '{start_date}' and '{end_date}'
    group by grouping sets ((cfr.order_branch), (cfr.order_branch, cfr.creator))
    ),
    smart_forwarded as (
    select sf.branch as "Branch",
	coalesce(sf.ods_creator, 'OVERALL') as "Payroll Number",
	round((sum(case when sf.time_taken <= 60 then 1 else 0 end)::decimal / count(sf.order_number))::decimal * 100, 0) as "Approval Received to SMART Forwarded Efficiency"
	from report_views.smart_forwarded sf 
	where sf.smart_time::date between '{start_date}' and '{end_date}'
	group by grouping sets ((sf.branch), (sf.branch, sf.ods_creator))
	order by 1
    ),
    overall as (
    select
    coalesce(nps."Branch", opening_time."Branch", sops."Branch", 
    quality_rejected."Branch", customer_issue."Branch", discounts."Branch", 
    insurance_conversion."Branch",aprvl_upd_eff."Branch", optom_conversion."Branch",
    staff_conversion."Branch",eyetest_time."Branch", eyetest_to_order."Branch", pie."Branch", 
    gr."Branch", ad."Branch", war."Branch", cl."Branch", ed."Branch", cf."Branch", sff."Branch"
    ) as "Branch",
    coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", 
    quality_rejected."Payroll Number", customer_issue."Payroll Number", discounts."Payroll Number", 
    insurance_conversion."Payroll Number",aprvl_upd_eff."Payroll Number", optom_conversion."Payroll Number",
    staff_conversion."Payroll Number", eyetest_time."Payroll Number", eyetest_to_order."Payroll Number", 
    pie."Payroll Number", gr."Payroll Number", ad."Payroll Number", war."Payroll Number", cl."Payroll Number",
    ed."Payroll Number", cf."Payroll Number",sff."Payroll Number"
    ) as "Payroll Number",
    coalesce(ad."Active Days", 0) as "Active Days",
    coalesce(opening_time."Times Opened Late(Thresh = 0)") as "Times Opened Late(Thresh = 0)",
    coalesce(sops."SOPs/ Customers") as "SOPs/ Customers",
    war."Warranties Not Returned",
    optom_conversion."Optom Low RX Conversion (Target = 65)",
    staff_conversion."EWC Low RX Conversion (Target = 65)",
    ed."Error Deductable",
    eyetest_time."Average Eye Test Time",
    eyetest_to_order."Eye Test to Order Efficiency (Target = 90%% in 45 minutes)",
    pie."Printing Identifier Efficiency (Target = 5 Mins)",
    cf."CorrectedFormsResentEfficiency",
    coalesce(quality_rejected."Quality Rejected at Branch", 0) as "Quality Rejected at Branch",
    sff."Approval Received to SMART Forwarded Efficiency",
    nps."NPS Score(Target = 90)",
    gr."Average Rating",
    coalesce(customer_issue."Customer Issue", 0) as "Customer Issue During Collection",
    coalesce(discounts."Orders Collected and Discounted Same Day", 0) as "Orders Collected and Discounted Same Day",
    insurance_conversion."Use Available Amount Conversion",
    insurance_conversion."Declined Conversion",
    aprvl_upd_eff."ApprovalReceivedInsuranceUpdateSAP",
    cl."InsuranceFeedbackToCustomerTime"
    from nps
    full outer join opening_time 
    on opening_time."Branch" = nps."Branch" and opening_time."Payroll Number" =  nps."Payroll Number" 
    full outer join sops 
    on sops."Branch" = coalesce(nps."Branch", opening_time."Branch") 
    and sops."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number")
    full outer join quality_rejected 
    on quality_rejected."Branch"=  coalesce(nps."Branch", opening_time."Branch", sops."Branch") 
    and quality_rejected."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number")
    full outer join customer_issue 
    on customer_issue."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch") 
    and customer_issue."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number")
    full outer join discounts 
    on discounts."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch", customer_issue."Branch") 
    and discounts."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number", customer_issue."Payroll Number")
    full outer join insurance_conversion 
    on insurance_conversion."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch", customer_issue."Branch", discounts."Branch") 
    and insurance_conversion."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number", customer_issue."Payroll Number", discounts."Payroll Number")
    full outer join aprvl_upd_eff 
    on aprvl_upd_eff."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch", customer_issue."Branch", discounts."Branch", insurance_conversion."Branch") 
    and aprvl_upd_eff."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number", customer_issue."Payroll Number", discounts."Payroll Number", insurance_conversion."Payroll Number")
    full outer join optom_conversion 
    on optom_conversion."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch", customer_issue."Branch", discounts."Branch", insurance_conversion."Branch",aprvl_upd_eff."Branch") 
    and optom_conversion."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number", customer_issue."Payroll Number", discounts."Payroll Number", insurance_conversion."Payroll Number", aprvl_upd_eff."Payroll Number")
    full outer join staff_conversion 
    on staff_conversion."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch", customer_issue."Branch", discounts."Branch", insurance_conversion."Branch",aprvl_upd_eff."Branch", optom_conversion."Branch")
    and staff_conversion."Payroll Number" =  coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number", customer_issue."Payroll Number", discounts."Payroll Number", insurance_conversion."Payroll Number", aprvl_upd_eff."Payroll Number", optom_conversion."Payroll Number")
    full outer join eyetest_time 
    on eyetest_time."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch", customer_issue."Branch", discounts."Branch", insurance_conversion."Branch",aprvl_upd_eff."Branch", optom_conversion."Branch", staff_conversion."Branch")
    and eyetest_time."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number", customer_issue."Payroll Number", discounts."Payroll Number", insurance_conversion."Payroll Number", aprvl_upd_eff."Payroll Number", optom_conversion."Payroll Number", staff_conversion."Payroll Number")
    full outer join eyetest_to_order 
    on eyetest_to_order."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch", customer_issue."Branch", discounts."Branch", insurance_conversion."Branch",aprvl_upd_eff."Branch", optom_conversion."Branch", staff_conversion."Branch", eyetest_time."Branch")
    and eyetest_to_order."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number", customer_issue."Payroll Number", discounts."Payroll Number", insurance_conversion."Payroll Number", aprvl_upd_eff."Payroll Number", optom_conversion."Payroll Number", staff_conversion."Payroll Number", eyetest_time."Payroll Number")
    full outer join printing_identifier pie 
    on pie."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch", customer_issue."Branch", discounts."Branch", insurance_conversion."Branch",aprvl_upd_eff."Branch", optom_conversion."Branch", staff_conversion."Branch", eyetest_time."Branch", eyetest_to_order."Branch")
    and pie."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number", customer_issue."Payroll Number", discounts."Payroll Number", insurance_conversion."Payroll Number", aprvl_upd_eff."Payroll Number", optom_conversion."Payroll Number", staff_conversion."Payroll Number", eyetest_time."Payroll Number", eyetest_to_order."Payroll Number")
    full outer join google_reviews gr
    on gr."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch", customer_issue."Branch", discounts."Branch", insurance_conversion."Branch",aprvl_upd_eff."Branch", optom_conversion."Branch", staff_conversion."Branch", eyetest_time."Branch", eyetest_to_order."Branch", pie."Branch")
    and gr."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number", customer_issue."Payroll Number", discounts."Payroll Number", insurance_conversion."Payroll Number", aprvl_upd_eff."Payroll Number", optom_conversion."Payroll Number", staff_conversion."Payroll Number", eyetest_time."Payroll Number", eyetest_to_order."Payroll Number", pie."Payroll Number")
    full outer join active_days ad 
    on ad."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch", customer_issue."Branch", discounts."Branch", insurance_conversion."Branch",aprvl_upd_eff."Branch", optom_conversion."Branch", staff_conversion."Branch", eyetest_time."Branch", eyetest_to_order."Branch", pie."Branch", gr."Branch")
    and ad."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number", customer_issue."Payroll Number", discounts."Payroll Number", insurance_conversion."Payroll Number", aprvl_upd_eff."Payroll Number", optom_conversion."Payroll Number", staff_conversion."Payroll Number", eyetest_time."Payroll Number", eyetest_to_order."Payroll Number", pie."Payroll Number", gr."Payroll Number")
    full outer join warranties war 
    on war."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch", customer_issue."Branch", discounts."Branch", insurance_conversion."Branch",aprvl_upd_eff."Branch", optom_conversion."Branch", staff_conversion."Branch", eyetest_time."Branch", eyetest_to_order."Branch", pie."Branch", gr."Branch", ad."Branch")
    and war."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number", customer_issue."Payroll Number", discounts."Payroll Number", insurance_conversion."Payroll Number", aprvl_upd_eff."Payroll Number", optom_conversion."Payroll Number", staff_conversion."Payroll Number", eyetest_time."Payroll Number", eyetest_to_order."Payroll Number", pie."Payroll Number", gr."Payroll Number", ad."Payroll Number")
    full outer join client_concated cl 
    on cl."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch", customer_issue."Branch", discounts."Branch", insurance_conversion."Branch",aprvl_upd_eff."Branch", optom_conversion."Branch", staff_conversion."Branch", eyetest_time."Branch", eyetest_to_order."Branch", pie."Branch", gr."Branch", ad."Branch", war."Branch")
    and cl."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number", customer_issue."Payroll Number", discounts."Payroll Number", insurance_conversion."Payroll Number", aprvl_upd_eff."Payroll Number", optom_conversion."Payroll Number", staff_conversion."Payroll Number", eyetest_time."Payroll Number", eyetest_to_order."Payroll Number", pie."Payroll Number", gr."Payroll Number", ad."Payroll Number", war."Payroll Number")
    full outer join error_deductable ed 
    on ed."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch", customer_issue."Branch", discounts."Branch", insurance_conversion."Branch",aprvl_upd_eff."Branch", optom_conversion."Branch", staff_conversion."Branch", eyetest_time."Branch", eyetest_to_order."Branch", pie."Branch", gr."Branch", ad."Branch", war."Branch", cl."Branch")
    and ed."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number", customer_issue."Payroll Number", discounts."Payroll Number", insurance_conversion."Payroll Number", aprvl_upd_eff."Payroll Number", optom_conversion."Payroll Number", staff_conversion."Payroll Number", eyetest_time."Payroll Number", eyetest_to_order."Payroll Number", pie."Payroll Number", gr."Payroll Number", ad."Payroll Number", war."Payroll Number", cl."Payroll Number")
    full outer join corrected_forms cf 
    on cf."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch", customer_issue."Branch", discounts."Branch", insurance_conversion."Branch",aprvl_upd_eff."Branch", optom_conversion."Branch", staff_conversion."Branch", eyetest_time."Branch", eyetest_to_order."Branch", pie."Branch", gr."Branch", ad."Branch", war."Branch", cl."Branch", ed."Branch")
    and cf."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number", customer_issue."Payroll Number", discounts."Payroll Number", insurance_conversion."Payroll Number", aprvl_upd_eff."Payroll Number", optom_conversion."Payroll Number", staff_conversion."Payroll Number", eyetest_time."Payroll Number", eyetest_to_order."Payroll Number", pie."Payroll Number", gr."Payroll Number", ad."Payroll Number", war."Payroll Number", cl."Payroll Number", ed."Payroll Number")
    full outer join smart_forwarded sff 
    on sff."Branch" = coalesce(nps."Branch", opening_time."Branch", sops."Branch", quality_rejected."Branch", customer_issue."Branch", discounts."Branch", insurance_conversion."Branch",aprvl_upd_eff."Branch", optom_conversion."Branch", staff_conversion."Branch", eyetest_time."Branch", eyetest_to_order."Branch", pie."Branch", gr."Branch", ad."Branch", war."Branch", cl."Branch", ed."Branch", cf."Branch")
    and sff."Payroll Number" = coalesce(nps."Payroll Number", opening_time."Payroll Number", sops."Payroll Number", quality_rejected."Payroll Number", customer_issue."Payroll Number", discounts."Payroll Number", insurance_conversion."Payroll Number", aprvl_upd_eff."Payroll Number", optom_conversion."Payroll Number", staff_conversion."Payroll Number", eyetest_time."Payroll Number", eyetest_to_order."Payroll Number", pie."Payroll Number", gr."Payroll Number", ad."Payroll Number", war."Payroll Number", cl."Payroll Number", ed."Payroll Number", cf."Payroll Number")
    )
    select 
    bd.branch_code as "Branch",
    bd.srm as "SRM",
    bd.rm as "RM",
    users.user_name as "Staff Name",
    users.user_department_name as "Designation",
    overall."Payroll Number",
    overall."Active Days",
    overall."Times Opened Late(Thresh = 0)",
    overall."SOPs/ Customers",
    overall."Warranties Not Returned",
    overall."NPS Score(Target = 90)",
    overall."Average Rating" as "Google Reviews Average Rating",
    overall."Optom Low RX Conversion (Target = 65)",
    overall."EWC Low RX Conversion (Target = 65)",
    overall."Error Deductable",
    overall."Eye Test to Order Efficiency (Target = 90%% in 45 minutes)",
    overall."Approval Received to SMART Forwarded Efficiency",
    overall."Printing Identifier Efficiency (Target = 5 Mins)",
    overall."CorrectedFormsResentEfficiency",
    overall."Average Eye Test Time",
    overall."Quality Rejected at Branch",
    overall."Customer Issue During Collection",
    overall."Orders Collected and Discounted Same Day",
    overall."Use Available Amount Conversion",
    overall."Declined Conversion",
    overall."ApprovalReceivedInsuranceUpdateSAP",
    overall."InsuranceFeedbackToCustomerTime"
    from overall
    inner join reports_tables.branch_data bd 
    on overall."Branch" = bd."branch_code"
    left join {database}.source_users users 
    on overall."Payroll Number"::text = users.user_code::text
    order by overall."Branch", overall."Payroll Number"
    """

    return pd.read_sql_query(query, con=engine)


class KPIsData:
    def __init__(self, engine, start_date, end_date, view, database) -> None:
        self.engine = engine
        self.start_date = start_date
        self.end_date = end_date
        self.view = view
        self.database = database



        """
        Private Class Method
        """
    def _validate_dates(self, start_date: str, end_date: str) -> None:
        try:
            pd.to_datetime(start_date)
            pd.to_datetime(end_date)
        except ValueError:
            raise ValueError(
                "start_date and end_date must be in a valid date format (YYYY-MM-DD)."
            )

    def _execute_query(self, query: str) -> pd.DataFrame:
        try:
            return pd.read_sql_query(query, con=self.engine)
        except Exception as e:
            print(f"An error occured: {e}")
            return pd.DataFrame()

    def _get_max_date(self, table: str, date_column: str) -> datetime:
        query = f"SELECT MAX({date_column}) AS max_date FROM {table}"
        result = self._execute_query(query)
        if not result.empty and result["max_date"].iloc[0]:
            return pd.to_datetime(result["max_date"]).iloc[0]
        return None

    def fetch_opening_time(self) -> pd.DataFrame:
        query = f"""
        select sot.date as "Date",
        shb.branch_code as "Branch",
        sot.opening_time as "Opening Time",
        sot.time_opened as "Time Opened",
        sot.lost_time as "Lost Time",
        ' ' as "Comments"
        from {self.database}.source_opening_time sot 
        left join {self.database}.source_hrms_branch shb
        on shb.branch_name::text = sot.branch::text
        where sot.date::date between '{self.start_date}' and '{self.end_date}'
        and sot.lost_time > 0
        """

        return self._execute_query(query)

    def fetch_eyetest_not_converted(self) -> pd.DataFrame:
        query = f"""
        select create_date as "Eye Test Date",
        code as "Prescription Number",
        status as "Status",
        cust_code as "Customer Code",
        mode_of_pay as "Customer Type",
        branch_code as "Branch",
        optom_name as "Optom Name",
        handed_over_to  as "Handed Over To",
        last_viewed_by as "Last self.viewed By",
        on_after as "Order Placed",
        on_after_mode as "Mode of Payment",
        on_after_status as "Order Status",
        days as "Days Taken to Convert",
        conversion_remarks as "Conversion Remarks"
        from
        (select row_number() over(partition by cust_code, create_date, code order by days, rx_type, code desc) as r, *
        from {self.view}.et_conv
        where status not in ('Cancel','Unstable', 'CanceledEyeTest', 'Hold')
        and (patient_to_ophth not in ('Yes') or patient_to_ophth is null)) as a 
        where a.r = 1 
        and a.optom is not null 
        and a.create_date::date between '{self.start_date}' and '{self.end_date}'
        and (a.days is null or a.days > 7)
        and "RX" = 'Low Rx'
        """

        return self._execute_query(query)

    def fetch_eytest_to_order_delays(self) -> pd.DataFrame:
        query = f"""
        SELECT 
        visit_id AS "Prescription Number",
        cust_code AS "Customer Code",
        branch AS "Branch",
        TO_CHAR(et_completed_time, 'YYYY-MM-DD HH24:MI') AS "Eye Test Completed Time",
        order_number AS "Order Number",
        TO_CHAR(order_date, 'YYYY-MM-DD HH24:MI') AS "Draft Order Time",
        ods_creator as "Order Creator",
        time_taken as "Time Taken"
        FROM report_views.eyetest_to_order eto
        where time_taken > 45
        and order_date::date between '{self.start_date}' and '{self.end_date}';
        """

        return self._execute_query(query)

    def fetch_identifier_printing_delays(self) -> pd.DataFrame:
        query = f"""
        select doc_no as "Order Number",
        user_name as "Order Creator",
        ods_outlet as "Branch",
        case when ods_insurance_order = 'Yes' then 'Insurance' else 'Cash' end as "Order Type",
        identifier as "Identifier Printed",
        TO_CHAR(sales_order_created, 'YYYY-MM-DD HH24:MI') AS "Sale Order Time",
        TO_CHAR(printed_identifier, 'YYYY-MM-DD HH24:MI') AS "Printed Identifier Time",
        time_taken as "Time Taken",
        ' ' as "Comments"
        from {self.view}.printing_identifier_efficiency pie 
        where time_taken > 5 
        and printed_identifier::date between '{self.start_date}' and '{self.end_date}';
        """

        return self._execute_query(query)

    def fetch_passive_comments(self) -> pd.DataFrame:
        query = f"""
        select sap_internal_number as "SAP Internal Number",
        trigger_date as "Trigger Date",
        cstmr as "Customer Code",
        branch as "Branch",
        user_name as "Staff Name",
        nps_rating as "NPS Rating",
        long_feedback as "Long Comment"
        from {self.view}.nps_surveys ns 
        where nps_rating ~ '^[0-9]+$'::text 
        AND survey_id <> 'nan'::text 
        AND response = 'Yes'::text 
        AND drop = 'No'::text
        and trigger_date::date between '{self.start_date}' and '{self.end_date}'
        and long_feedback <> ''
        and long_feedback is not null 
        and long_feedback <> '0'
        and nps_rating::int in (7,8)
        """

        return self._execute_query(query)

    def fetch_poor_google_reviews(self) -> pd.DataFrame:
        query = f"""
        select createdat::date as "Google Review Date",
        store_code as "Branch",
        reviewer as "Customer Name",
        case when star_rating in ('FIVE') then 5
        when star_rating in ('FOUR') then 4 
        when star_rating in ('THREE') then 3 
        when star_rating in ('TWO') then 2 
        when star_rating in ('ONE') then 1 
        end as "Rating (Out of 5)",
        review_comment as "Customer Comment"
        from {self.database}.google_reviews gr 
        where createdat::date between '{self.start_date}' and '{self.end_date}'
        and star_rating in ('ONE', 'TWO', 'THREE')
        """

        return self._execute_query(query)

    def fetch_sop_non_compliance(self) -> pd.DataFrame:
        query = f"""
        select sop_date "SOP Date",
        branch_code as "Branch",
        sop as "SOP Missed",
        classification
        from {self.view}.sop 
        where sop_date::date between '{self.start_date}' and '{self.end_date}'
        and branch_code is not null
        """

        return self._execute_query(query)

    def fetch_frame_only_orders(self) -> pd.DataFrame:
        query = f"""
        select "OrderNumber" as "Order Number",
        "CustomerCode" as "Customer Code",
        "EyeTestDate" as "Eye Test Date",
        "EyeTestCode" as "Prescription Number",
        "OptomName" as "Optom Name",
        "OrderBranch" as "Branch",
        "DraftOrderCreateDate" as "Order Date",
        "OrderCreator" as "Order Creator",
        "InsuranceOrder" as "Insurance Order?",
        "ItemName" as "Frame Name"
        from {self.view}.frame_only_orders foo 
        where "SalesOrderCreateDate"::date between '{self.start_date}' and '{self.end_date}'
        """

        return self._execute_query(query)

    def fetch_insurance_feedback_not_converted(self) -> pd.DataFrame:
        query = f"""  
        select doc_no as "Order Number",
        ods_outlet as "Branch",
        user_name as "Order Creator",
        ods_status as "Status",
        ods_status1 as "Current Status",
        cust_code as "Customer Code",
        fdbck_type as "Insurance Feedback",
        TO_CHAR(fdbck_tmstmp, 'YYYY-MM-DD HH24:MI') AS "Feedback Time"
        from {self.view}.insurance_feedback_conversion ifc 
        where cnvrtd = 0 
        and fdbck_type in ('Declined by Insurance Company', 'Use Available Amount on SMART')
        and fdbck_date::date between '{self.start_date}' and '{self.end_date}'
        """

        return self._execute_query(query)

    def fetch_anomalous_eyetest_times(self) -> pd.DataFrame:
        query = f"""
        select eqt.visit_id as "Visit Id",
        eqt.outlet_id as "Branch",
        eqt.cust_id as "Customer Code",
        eqt.optom_name as "Optom Name",
        to_char(eqt.added_to_queue2,'YYYY-MM-DD HH24:MI') as "Added to Queue",
        to_char(eqt.et_start, 'YYYY-MM-DD HH24:MI') as "ET Start Time",
        to_char(eqt.eyetest_complete, 'YYYY-MM-DD HH24:MI') as "ET End Time",
        eye_testtime as "Eye Test Time"
        from {self.view}.eyetest_queue_time eqt 
        where (eqt.eye_testtime > 12 or eqt.eye_testtime < 5)
        and eqt.added_to_queue2::date between '{self.start_date}' and '{self.end_date}'
        """

        return self._execute_query(query)

    def fetch_conversion_trend(self):
        query = f"""
        select 
        trim(a.branch_code) as "branch",
        to_char(date_trunc('week', a.create_date::timestamp), 'yyyy-mm-dd') as "week start",
        round((sum(case when a.days <= 7 then 1 else 0 end)::decimal / count(a.code) * 100), 0)::decimal 
        as "overall conversion"
        from (select row_number() over(partition by cust_code, create_date, code order by days, rx_type, code desc) as r, *
        from {self.view}.et_conv
        where status not in ('cancel', 'unstable', 'canceledeyetest', 'hold')
        and (patient_to_ophth not in ('yes') or patient_to_ophth is null)) as a 
        where a.r = 1
        and a.optom is not null
        and a.create_date::date between date_trunc('week', current_date) - interval '12 weeks' 
        and date_trunc('week', current_date) - interval '1 day'
        group by 1, 2
        order by 2 asc;
        """
        return self._execute_query(query)
    

    def fetch_lowrx_conversion_trend(self):
        query = f"""
        select 
        trim(a.branch_code) as "Branch",
        to_char(date_trunc('week', a.create_date::timestamp), 'yyyy-mm-dd') as "week start",
        round((sum(case when a.days <= 7 then 1 else 0 end)::decimal / count(a.code) * 100), 0)::decimal 
        as "Overall Conversion"
        from (select row_number() over(partition by cust_code, create_date, code order by days, rx_type, code desc) as r, *
        from {self.view}.et_conv
        where status not in ('cancel', 'unstable', 'canceledeyetest', 'hold')
        and (patient_to_ophth not in ('yes') or patient_to_ophth is null)) as a 
        where a.r = 1
        and a.optom is not null
        and a.create_date::date between date_trunc('week', current_date) - interval '12 weeks' 
        and date_trunc('week', current_date) - interval '1 day'
        and "RX" = 'low rx'
        group by 1, 2
        order by 2 asc;
        """
        return self._execute_query(query)

    @property
    def is_up_to_date(self) -> tuple:
        tables = {
            f"{self.database}.source_opening_time": "date",
            f"{self.view}.et_conv": "create_date",
            "report_views.eyetest_to_order": "order_date",
            f"{self.view}.printing_identifier_efficiency": "printed_identifier",
            f"{self.view}.nps_surveys": "trigger_date",
            f"{self.database}.google_reviews": "createdat",
            f"{self.view}.sop": "sop_date",
            # f"{self.view}.frame_only_orders": "SalesOrderCreateDate",
            f"{self.view}.insurance_feedback_conversion": "fdbck_date",
        }

        today = datetime.today().date()
        yesterday = today - timedelta(days=1)
        not_up_to_date_tables = []

        for table, date_column in tables.items():
            max_date = self._get_max_date(table, date_column)
            if max_date is None or max_date.date() < yesterday:
                not_up_to_date_tables.append(table)

        if not_up_to_date_tables:
            return False, f"Tables not up to date: {', '.join(not_up_to_date_tables)}"
        return True, "All tables are up to date"


"""
fetch_opening_time : Opening Time
fetch_eyetest_not_converted: Eye Tests not Converted
fetch_eytest_to_order_delays: Eye Test to Draft Order Delays
fetch_identifier_printing_delays: Printing Identifier Delays
fetch_passive_comments: Passive Comments
fetch_poor_google_reviews: Poor Google Reviews
fetch_sop_non_compliance: SOP not Complied
fetch_frame_only_orders: Frame only Orders
fetch_insurance_feedback_not_converted: Insurance Feedback not Converted
fetch_conversion_trend
"""
