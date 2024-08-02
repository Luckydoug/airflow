import pandas as pd
from airflow.models import variable
from reports.draft_to_upload.utils.utils import today


def fetch_orderscreen(database, engine, start_date="2024-01-01"):
    orderscreen_query = f"""
    select orderscreen.doc_entry as "DocEntry", odsc_date::date  as "Date",
    case
        when length(odsc_time::text) in (1,2) then null
        else
            (left(odsc_time::text,(length(odsc_time::text)-2))||':'||right(odsc_time::text, 2))::time 
        end 
            as "Time",
    odsc_status as "Status", odsc_createdby as "Created User", 
    odsc_remarks as "Remarks", orders.doc_no::int as "Order Number" 
    from {database}.source_orderscreenc1 orderscreen
    left join {database}.source_orderscreen as orders 
    on orderscreen.doc_entry = orders.doc_entry
    where odsc_date::date between '{start_date}' and '{today}'
    """

    orderscreen = pd.read_sql_query(orderscreen_query, con=engine)
    return orderscreen


def fetch_orders(database, engine, start_date="2024-01-01"):
    orders_query = f"""
    SELECT CAST(orders.doc_entry AS INT) AS "DocEntry", 
        CAST(orders.doc_no AS INT) AS "DocNum", 
        orders.presctiption_no::text as "Code",
        orders.ods_createdon::date AS "CreateDate",
        CASE
            WHEN length(ods_createdat::text) in (1,2) THEN null
            ELSE (left(ods_createdat::text,(length(ods_createdat::text)-2))||':'||right(ods_createdat::text, 2))::time 
        END AS "CreateTime",
        ods_status as "Status",
        CAST(orders.cust_code AS TEXT) AS "Customer Code", 
        orders.ods_outlet AS "Outlet", 
        orders.ods_insurance_order as "Insurance Order",
        orders.ods_creator AS "Creator", 
        users.user_name AS "Order Creator"
    FROM {database}.source_orderscreen orders
    LEFT JOIN {database}.source_users AS users ON CAST(orders.ods_creator AS TEXT) = CAST(users.user_code AS TEXT)
    WHERE orders.ods_createdon::date BETWEEN '{start_date}' AND '{today}'
    AND CAST(orders.cust_code AS TEXT) <> ''
    """

    all_orders = pd.read_sql_query(orders_query, con=engine)
    return all_orders


def fetch_views(database, engine, start_date="2024-01-01"):
    views_query = f"""
    SELECT visit_id::text  as "Code", create_date::date as "Create Date", 
    case
    when length(create_time::text) in (1,2) then null
    else
        (left(create_time::text,(length(create_time::text)-2))||':'||right(create_time::text, 2))::time 
    end 
        as "Create Time"
    FROM {database}.source_order_checking_details
    where create_date::date BETWEEN '{start_date}' and '{today}'
    """
    all_views = pd.read_sql_query(views_query, con=engine)
    all_views = all_views.sort_values(by=["Create Date", "Create Time"], ascending=True)
    all_views = all_views.drop_duplicates(subset=["Code"], keep="last")
    all_views.loc[:, "Last View Date"] = pd.to_datetime(
        all_views["Create Date"].astype(str)
        + " "
        + all_views["Create Time"].astype(str),
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )
    return all_views


def fetch_insurance_companies(database, engine, start_date="2024-01-01"):
    insurance_companies_query = f"""
    select orders.doc_no::int as "DocNum",
        insurance_company.insurance_name as "Insurance Company",
        plan.plan_scheme_name as "Insurance Scheme", plan.plan_scheme_type as "Scheme Type",
        ods_insurance_feedback1 as "Feedback 1", ods_insurance_feedback2 as "Feedback 2"
    from {database}.source_orderscreen as orders
    left join {database}.source_users  AS users 
    ON CAST(orders.ods_creator AS TEXT) = CAST(users.se_optom AS TEXT)
    inner JOIN {database}.source_web_payments as payments 
    on orders.doc_entry::text = payments.unique_id::text and payments.payment_mode = 'Insurance'
    left join {database}.source_insurance_company as insurance_company 
    on payments.insurance_company_name::text = insurance_company.insurance_code::text 
    left join {database}.source_insurance_plan as plan 
    on insurance_company.id = plan.insurance_id and plan.plan_scheme_name = payments.insurance_scheme
    where orders.ods_createdon::date between '{start_date}' and '{today}'
    """

    insurance_companies = pd.read_sql_query(insurance_companies_query, con=engine)
    return insurance_companies


def fetch_eyetests(database, engine, start_date="2024-01-01"):
    eyetests_query = f"""
        SELECT branch_code as "Outlet", tests.doc_entry AS "DocEntry", 
        tests.code::int AS "Eyetest Code", tests.status AS "Status",
        tests.create_date AS "Eyetest Date",
        tests.cust_code AS "Customer Code", tests.optom AS "Creator",
        users.user_name AS "Optom Name"
        FROM {database}.source_prescriptions tests
        LEFT JOIN {database}.source_users  AS users
        ON CAST(tests.optom AS TEXT) = CAST(users.se_optom AS TEXT)
        WHERE create_date::date BETWEEN '{start_date}' and '{today}'
    """

    eyetests = pd.read_sql_query(eyetests_query, con=engine)
    return eyetests


def fetch_salesorders(database, engine, start_date="2024-01-01"):
    sales_orders_query = f"""
    select draft_orderno::int as "Order Number" from {database}.source_orders_header
    where creation_date::date between '{start_date}' and '{today}'
    """
    sales_orders = pd.read_sql_query(sales_orders_query, con=engine)
    return sales_orders


def fetch_sops_branch_info(engine):
    branch_info_query = """
    SELECT branch as "Branch", branch_code as "Outlet"
    FROM mabawa_staging.sop_branch_info;
    """
    sop_branch_info = pd.read_sql_query(branch_info_query, con=engine)
    return sop_branch_info


def fetch_registrations(engine, database, table, table2, start_date="2024-01-01"):
    registrations_query = f"""
    SELECT cust_outlet as "Outlet", CAST(customers.cust_code AS TEXT) AS "Customer Code", 
    customers.cust_createdon AS "Registration Date", 
    customers.cust_gender AS "Gender", cust_type as "Customer Type",
    customers.cust_sales_employeecode AS "Creator",
    users.user_name AS "User Name"
    FROM {database}.{table} customers
    LEFT JOIN {database}.{table2} AS users 
    ON CAST(customers.cust_sales_employeecode AS TEXT) = CAST(users.se_optom AS TEXT)
    WHERE customers.cust_createdon::date BETWEEN '{start_date}' AND '{today}'
    """.format(
        today=today, start_date=start_date
    )

    registrations = pd.read_sql_query(registrations_query, con=engine)
    return registrations


def fetch_payments(engine, database, start_date="2024-01-01"):
    payments_query = f"""
    select payments.doc_entry as "PyDocEntry",payments.full_doc_type as "DocType", 
    payments.mode_of_pay as "Mode of Pay", payments.draft_orderno as "Order Number", 
    payments.createdon::date as "CreateDate", payments.branch_code as "Outlet", 
    orders.ods_normal_repair_order as "Order Type", 
    payments.cust_code as "Customer Code"
    from {database}.source_payment as payments
    left join {database}.source_orderscreen as orders 
    on payments.draft_orderno::text = orders.doc_no::text
    where payments.full_doc_type = 'Order Payment' and
    payments.status <> 'Cancel' and
    payments.status <> 'cancel' and
    mode_of_pay = 'Insurance' and
    createdon::date  between '{start_date}' and '{today}'
    """

    payments = pd.read_sql_query(payments_query, con=engine)
    return payments


def fetch_planos(
    database, engine, schema, users, customers, table, views, start_date="2024-01-01"
):
    planos_query = f"""
        SELECT
            a.code AS "Code",
            a.create_date AS "Create Date",
            a.status AS "Status",
            a.rx_type AS "RX Type",
            a.cust_code::text AS "Customer Code",
            insurance.insurance_name AS "Insurance Company",
            customers.cust_insurance_scheme AS "Scheme Name",
            a.branch_code AS "Branch",
            a.optom_name AS "Opthom Name",
            a.handed_over_to AS "EWC Handover",
            a.last_viewed_by as "Who Viewed RX",
            "RX",
            a.plano_rx AS "Plano RX",
            CASE WHEN a.days is not null THEN 1 ELSE 0 END AS "Conversion"
        FROM
            (
                SELECT
                    row_number() OVER (
                        PARTITION BY cust_code, create_date
                        ORDER BY days, rx_type, code DESC
                    ) AS r,
                    *
                FROM
                    {views}.{table}
                WHERE
                    status NOT IN ('Cancel', 'Unstable', 'Hold', 'CanceledEyeTest', 'Unconfirm')
                    AND (patient_to_ophth NOT IN ('Yes') OR patient_to_ophth IS NULL)
            ) AS a
            LEFT JOIN {schema}.{users} b ON a.optom::text = b.se_optom::text
            LEFT JOIN {schema}.{customers} AS customers ON a.cust_code::text = customers.cust_code::text
            LEFT JOIN {database}.source_insurance_company AS insurance ON customers.cust_insurance_company::text = insurance.insurance_code::text
        WHERE
            a.r = 1
            and a.create_date::date between '{start_date}' and '{today}'
            AND a.cust_code <> '10026902'
            and (insurance.insurance_name <> 'KENYA REVENUE AUTHORITY (KRA)')
            and (insurance.insurance_name <> 'CORPORATE OUTREACH')
            and activity_no is null
    """

    all_planos = pd.read_sql_query(planos_query, con=engine)
    all_planos["Scheme Name"] = all_planos["Scheme Name"].fillna(" ")
    all_planos["Insurance Company"] = all_planos["Insurance Company"].fillna(" ")

    all_planos = all_planos[
        (all_planos["Plano RX"] != "Y")
        & (~all_planos["Insurance Company"].isin(["AAR M-TIBA", "AAR INSURANCE KENYA"]))
        & (~all_planos["Scheme Name"].str.contains("DIRECT"))
    ]

    return all_planos


def fetch_planorderscreen(database, engine, start_date="2024-01-01"):
    plano_orderscreen_query = f"""
    SELECT 
        orderscreen.doc_entry AS "DocEntry", 
        orders.doc_no::int AS "Order Number", 
        orders.ods_status1 as "Current Status",
        orderscreen.odsc_date::date AS "Date",
        CASE 
            WHEN LENGTH(odsc_time::text) IN (1,2) THEN NULL
            ELSE 
                (LEFT(odsc_time::text,(LENGTH(odsc_time::text)-2))||':'||RIGHT(odsc_time::text, 2))::time 
        END AS "Time",
        orders.cust_code AS "Customer Code",
        orders.presctiption_no AS "Code",
        orderscreen.odsc_status AS "Request",
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM {database}.source_orderscreenc1 
                WHERE doc_entry = orderscreen.doc_entry AND odsc_status IN 
                ('Insurance Fully Approved', 'Insurance Partially Approved', 'Use Available Amount on SMART')
            ) THEN 'Approved'
            WHEN EXISTS (
                SELECT 1 
                FROM {database}.source_orderscreenc1 
                WHERE doc_entry = orderscreen.doc_entry AND odsc_status = 'Declined by Insurance Company'
            ) THEN 'Declined'
            ELSE 'No Feedback'
        END AS "Feedback"
    FROM 
        {database}.source_orderscreenc1 orderscreen
        LEFT JOIN {database}.source_orderscreen AS orders ON orderscreen.doc_entry = orders.doc_entry
    WHERE 
        odsc_date::date BETWEEN '{start_date}' AND '{today}'
        AND orderscreen.odsc_status in 
        ('Resent Pre-Auth to Insurance Company',
        'Sent Pre-Auth to Insurance Company', 'Upload Attachment')
    """

    plano_orderscreen = pd.read_sql_query(plano_orderscreen_query, con=engine)
    return plano_orderscreen


def fetch_detractors(database, engine, table, start_date="2024-01-01"):
    detractors_query = f"""
    select sap_internal_number as "SAP Internal Number", branch as "Branch",
    trigger_date::date as "Trigger Date", nps_rating::int as "NPS Rating", long_feedback as "Long Remarks"
    from {database}.{table}
    where trigger_date::date between '{start_date}' and '{today}'
    and response = 'Yes'
    and drop = 'No'
    and nps_rating::int < 7
    """
    surveys = pd.read_sql_query(detractors_query, con=engine)
    return surveys


def fetch_opening_time(database, engine, start_date="2024-01-01"):
    opening_time_query = f"""
    select date::date as "Date", "day" as "Day", "branch" as "Branch", 
    "opening_time" as "Opening Time", time_opened as "Time Opened", "lost_time" as "Lost Time"
    from {database}.source_opening_time
    where lost_time::int > 0::int and date::date between '{start_date}' and '{today}'
    """

    opening_time = pd.read_sql_query(opening_time_query, con=engine)
    return opening_time


def fetch_insurance_efficiency(database, engine, start_date, dw):
    query = f"""
    SELECT
        order_number AS "Order Number",
        customer_code AS "Customer Code",
        outlet AS "Outlet",
        front_desk AS "Front Desk",
        creator AS "Creator",
        order_creator AS "Order Creator",
        draft_time AS "Draft Time",
        preauth_time AS "Preauth Time",
        upload_time AS "Upload Time",
        draft_to_preauth AS "Draft to Preauth",
        preauth_to_upload AS "Preuth to Upload",
        draft_to_upload AS "Draft to Upload",
        insurance_company AS "Insurance Company",
        slade AS "Slade",
        insurance_scheme AS "Insurance Scheme",
        scheme_type AS "Scheme Type",
        feedback_1 AS "Feedback 1",
        feedback_2 AS "Feedback 2"
    FROM
        {database}.source_insurance_efficiency
    where upload_time::date between '{start_date}' and '{today}'
    and order_number::text not in (select order_number::text from {dw}.dim_draft_drop);
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_daywise_efficiency(database, engine, start_date, end_date, dw):
    daywise_query = f"""
        SELECT
        outlet AS "Outlet",
        upload_time::date AS "Date",
        ROUND((SUM(CASE WHEN draft_to_upload::int <= 8 THEN 1 ELSE 0 END)::DECIMAL 
        / COUNT(upload_time))::DECIMAL * 100) AS "Efficiency"
        FROM
            {database}.source_insurance_efficiency sie
            where upload_time::date between '{start_date}' and '{end_date}'
            and order_number::text not in (select order_number::text from {dw}.dim_draft_drop)
        GROUP BY
        outlet, upload_time::date
        order by upload_time::date;
    """
    data = pd.read_sql_query(daywise_query, con=engine)
    return data


def fetch_mtd_efficiency(database, engine, start_date, end_date, dw):
    mtd_query = f"""
    SELECT
        outlet AS "Outlet",
        ROUND(
            (SUM(CASE WHEN draft_to_upload::int <= 8 THEN 1 ELSE 0 END)::DECIMAL 
            / COUNT(upload_time))::DECIMAL * 100
        ) AS "Grand Total"
    FROM
        {database}.source_insurance_efficiency sie
    WHERE
        upload_time::date BETWEEN '{start_date}' AND '{end_date}'
        and order_number::text not in (select order_number::text from {dw}.dim_draft_drop)
    GROUP BY
        outlet
    """
    data = pd.read_sql_query(mtd_query, con=engine)
    return data


def fetch_daywise_rejections(view, database, engine, start_date, end_date):
    daywise_query = f"""
    WITH rej AS (
    SELECT
        b."Outlet",
        b.date AS date,
        COUNT(b.doc_no) AS "Rejections"
    FROM (
        SELECT
            a.ods_outlet AS "Outlet",
            a.doc_no,
            a.odsc_date AS date
        FROM (
            SELECT
                rejections_view.ods_outlet,
                rejections_view.doc_no,
                rejections_view.odsc_date
            FROM {view}.rejections_view
        ) a
    ) b
    GROUP BY b."Outlet", b.date
    ),
    orders AS (
        SELECT
            a."Outlet",
            a.date,
            COUNT(a.doc_entry) AS "Orders"
        FROM (
            SELECT
                so.doc_entry,
                so.ods_createdon AS date,
                so.ods_outlet AS "Outlet"
            FROM {database}.source_orderscreen so
            WHERE so.ods_insurance_order = 'Yes'::text
        ) a
        GROUP BY a."Outlet", date
    )

    SELECT
        rej."Outlet",
        rej.date,
        orders."Orders",
        rej."Rejections"
    FROM rej
    LEFT JOIN orders ON rej."Outlet" = orders."Outlet" AND rej.date::date = orders.date::date
    WHERE rej.date::date BETWEEN '{start_date}' AND '{end_date}'
    AND orders.date::date BETWEEN '{start_date}' AND '{end_date}';
    """

    data = pd.read_sql_query(daywise_query, con=engine)
    data["% Rejected"] = round((data["Rejections"] / data["Orders"]) * 100, 0)
    return data


def fetch_mtd_rejections(engine, database, view, start_date, end_date):
    query_two = f"""
    WITH rej AS (
        SELECT
            b."Outlet",
            COUNT(b.doc_no) AS "Rejections"
        FROM (
            SELECT
                a.ods_outlet AS "Outlet",
                a.doc_no,
                a.odsc_date AS date
            FROM (
                SELECT
                    rejections_view.ods_outlet,
                    rejections_view.doc_no,
                    rejections_view.odsc_date
                FROM {view}.rejections_view
                where odsc_date::date between '{start_date}' and '{end_date}'
            ) a
            
        ) b
        GROUP BY b."Outlet"
    ),
    orders AS (
        SELECT
            a."Outlet",
            COUNT(a.doc_entry) AS "Orders"
        FROM (
            SELECT
                so.doc_entry,
                so.ods_createdon AS date,
                so.ods_outlet AS "Outlet"
            FROM {database}.source_orderscreen so
            WHERE so.ods_insurance_order = 'Yes'::text
            and so.ods_createdon::date between '{start_date}' and '{end_date}'
        ) a
        GROUP BY a."Outlet"
    )

    SELECT
        rej."Outlet",
        orders."Orders",
        rej."Rejections"
    FROM rej
    LEFT JOIN orders ON rej."Outlet" = orders."Outlet"
    """

    data = pd.read_sql_query(query_two, con=engine)
    data["Grand Total"] = round((data["Rejections"] / data["Orders"]) * 100, 0)
    data = data[["Outlet", "Grand Total"]]
    return data


def fetch_customers(engine, database, start_date):
    query = f"""
        select outlet as "Outlet", date::date as "Date", 
        customer_count as "Customer Code"
        from {database}.sop_customers_view
        where date::date between '{start_date}' and '{today}';
    """
    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_branch_data(engine, database):
    query = f"""
    select branch_code as "Outlet",
    branch_name as "Branch",
    email as "Email",
    rm as "RM",
    rm_email as "RM Email",
    rm_group as "RM Group",
    srm as "SRM",
    srm_email as "SRM Email",
    branch_manager as "Branch Manager",
    front_desk as "Front Desk",
    zone as "Zone",
    retail_analyst as "Retail Analyst",
    analyst_email as "Analyst Email",
    escalation_email as "Escalation Email"
    from {database}.branch_data bd 
    """

    branch_data = pd.read_sql_query(query, con=engine)
    return branch_data


def fetch_non_conversion_non_viewed(engine, database, start_date):
    query = f"""
    select code as "Code",
    create_date::date as "CreateDate",
    cust_code as "Customer Code",
    branch_code as "Branch",
    CASE
        WHEN length(create_time::text) in (1,2) THEN null
        ELSE (left(create_time::text,(length(create_time::text)-2))||':'||right(create_time::text, 2))::time 
    END AS "CreateTime",
    optom_name as "Optom Name",
    case when handed_over_to is null then 'Not handed over' 
    else handed_over_to end as "Handed Over To",
    last_viewed_by as "RX Last Viewed By",
    conversion_reason as "Conversion Reason",
    conversion_remarks as "Conversion Remarks"
    from {database}.et_conv ec 
    where last_viewed_by is null
    and days is null
    and status not in ('Cancel', 'Unstable', 'CanceledEyeTest', 'Hold') 
    and branch_code not in ('HOM', 'null', '0MA')
    and (patient_to_ophth not in ('Yes') or patient_to_ophth is null)
    and create_date::date between '{start_date}' and '{today}'
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_working_hours(engine):
    query = f"""
    select warehouse_code as "Warehouse Code",
    warehouse_name as "Warehouse Name",
    docnum as "DocNum",
    days "Days",
    start_time as "Start Time",
    end_time as "End Time",
    auto_time as "Auto Time"
    from reports_tables.working_hours 
    """
    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_no_views_data(database, engine, start_date):
    query = f"""
    select code as "Code",
    create_date::date as "CreateDate",
    cust_code as "Customer Code",
    "RX",
    branch_code as "Branch",
    CASE
        WHEN length(create_time::text) in (1,2) THEN null
        ELSE (left(create_time::text,(length(create_time::text)-2))||':'||right(create_time::text, 2))::time 
    END AS "CreateTime",
    optom_name as "Optom Name",
    case when handed_over_to is null then 'Not handed over' 
    else handed_over_to end as "Handed Over To",
    last_viewed_by as "RX Last Viewed By",
    conversion_reason as "Conversion Reason",
    conversion_remarks as "Conversion Remarks",
    case when days is not null then 1 else 0 end as "Conversion",
    case when days is not null and "RX" = 'High Rx' then 1 else 0 end as "High RX Conversion",
    case when days is not null and "RX" = 'Low Rx' then 1 else 0 end as "Low RX Conversion",
    case when last_viewed_by is null and days is null then 1 else 0 end as "No Views NoN",
    case when last_viewed_by is null and days is null and "RX" = 'High Rx' then 1 else 0 end as "High_Non",
    case when last_viewed_by is null and days is null and "RX" = 'Low Rx' then 1 else 0 end as "Low_Non"
    from {database}.et_conv ec 
    where status not in ('Cancel', 'Unstable', 'CanceledEyeTest', 'Hold') 
    and branch_code not in ('HOM', 'null', '0MA')
    and (patient_to_ophth not in ('Yes') or patient_to_ophth is null)
    and create_date::date between '{start_date}' and '{today}'
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_eyetest_order(engine, start_date, database):
    query = f"""
    SELECT a.*, 
    (date_part('epoch'::text, a."Order Date" - a."ET Completed Time")::integer / 60) as "Time Taken"
    FROM (
        SELECT
            ROW_NUMBER() OVER (
                PARTITION BY so.cust_code, so.ods_createdon::DATE
                ORDER BY so.ods_createdon::DATE + LPAD(so.ods_createdat::TEXT, 4, '0')::TIME ASC
            ) AS r,
            qt.visit_id,
            qt.customer_code as "Customer Code",
            qt.completed_time as "ET Completed Time",
            qt.branch as "Outlet",
            so.ods_outlet as "Order Branch",
            so.doc_no as "Order Number",
            so.ods_createdon::DATE + LPAD(so.ods_createdat::TEXT, 4, '0')::TIME as "Order Date",
            su.user_name as "Order Creator",
            case when so.ods_insurance_order  = 'Yes' then 'Insurance' else 'Cash' end as "Order Type",
            so.ods_ordercriteriastatus as "Criteria"
        FROM
            report_views.queue_time AS qt
            LEFT JOIN {database}.source_orderscreen AS so ON qt.visit_id::TEXT = so.presctiption_no::text
            left join {database}.source_users su 
            on so.ods_creator::text = su.user_code::text
    ) AS a
    WHERE a.r = 1
    and a."ET Completed Time"::date = a."Order Date"::date
    and a."ET Completed Time"::date between '{start_date}' and '{today}'
    and a."Order Branch" = "Outlet";
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_pending_insurance(engine, start_date):
    query = f"""
        select 
        ec.code::int as "Code",
        ec.cust_code as "Customer Code",
        sic.insurance_name as "Insurance Company",
        ec.create_date::date as "ET Date",
        ec.branch_code as "Branch",
        ec.optom_name as "Optom Name",
        ec.handed_over_to as "Handed Over To",
        ec.last_viewed_by as "Last Viewed By",
        ec."RX",
        ec.mode_of_pay as "Customer Type"
        from mabawa_mviews.et_conv ec 
        left join mabawa_staging.source_customers sc
        on ec.cust_code = sc.cust_code
        left join mabawa_staging.source_insurance_company sic 
        on sc.cust_insurance_company = sic.insurance_code
        where ec.days is null 
        and ec.mode_of_pay = 'Insurance'
        and ec.create_date::date between '{start_date}' and '{today}'
        and ec.plano_rx = 'N'
        and ec.status not in ('Cancel', 'CanceledEyeTest', 'Hold', 'Unstable')
        and (ec.patient_to_ophth not in ('Yes') or ec.patient_to_ophth is null)
        and sic.insurance_name is not null;
    """

    pending_insurances = pd.read_sql_query(query, con=engine)
    return pending_insurances


def fetch_submitted_insurance(engine, start_date):
    query = f"""
    select presctiption_no as "Code",
    odsc_status as "Request"
    from report_views.pending_insurance 
    where odsc_date between '{start_date}' and '{today}'
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_rejections_drop(engine):
    query = f"""
    SELECT order_no as "Order Number" 
    FROM mabawa_staging.source_drop_insurance_errors sdie 
    where order_no <> 'nan' 
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_eff_bef_feed_desk(start_date, engine, database, view):
    query = f"""
    select
    doc_no as "Order Number",ods_outlet as "Outlet",
    user_name as "Staff Name",
    draft_rejected as "Start Status",
    draft_rejected_tm as "Start Time",
    upload_resent as "End Status",
    upload_resent_tm as "End Time",
    case when iebf.upload_resent = 'Corrected Form Resent to Optica Insurance' then null
    else sie.draft_to_preauth::text end as "Draft to Preauth",
    case when iebf.upload_resent = 'Corrected Form Resent to Optica Insurance' then null
    else sie.preauth_to_upload::text end as "Preauth to Upload",
    case when iebf.upload_resent = 'Corrected Form Resent to Optica Insurance' then iebf.drftorrjctd_to_upldorrsnt
    else sie.draft_to_preauth::int + sie.preauth_to_upload::int end as "Time Taken"
    from {view}.insurance_efficiency_before_feedback iebf
    left join {database}.source_insurance_efficiency sie 
    on iebf.doc_no::text = sie.order_number::text
    where drft_rw = 1 
    and ods_outlet not in (
    select branch_code::text from reports_tables.branch_data where sends_own_insurance = 'Yes'
    )
    and upload_resent_tm::date between '{start_date}' and '{today}'
    and case when draft_rejected = 'Draft Order Created' then sie.draft_to_upload > 8 else drftorrjctd_to_upldorrsnt > 5 end
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_eff_bef_feed_nodesk(start_date, engine, database, view):
    query = f"""
    select 
    b."Order Number",
    b."Outlet",
    b."Order Branch",
    b."Staff Name",
    b."Draft Order Created",
    b."Upload Attachment",
    b."Sent Pre-Auth",
    b."Draft to Preauth",
    b."Preauth to Upload",
    b."Draft to Upload In Mins",
    b."Upload to Sent Pre-Auth In Mins",
    b."Total Time (Target = 13 Mins)"
    from ( select iebf.doc_no as "Order Number", iebf.ods_outlet as "Order Branch", 
    iebf.sentby_branch_code as "Outlet", iebf.ods_outlet, user_name as "Staff Name",
    iebf.draft_rejected_tm as "Draft Order Created",iebf.upload_resent_tm as "Upload Attachment",
    iebf.sent_preauth_tm as "Sent Pre-Auth",
    sie.draft_to_preauth as "Draft to Preauth",
    sie."preauth_to_upload" as "Preauth to Upload",
    sie.draft_to_upload as "Draft to Upload In Mins",
    iebf.upldorrsnt_to_sntprthorrjctd as "Upload to Sent Pre-Auth In Mins",
    case when iebf.upldorrsnt_to_sntprthorrjctd > 5 and iebf.sentby_branch_code <> iebf.ods_outlet then 1 
    when iebf.sentby_branch_code = iebf.ods_outlet and sie.draft_to_upload + iebf.upldorrsnt_to_sntprthorrjctd > 13 then 1 
    when iebf.upldorrsnt_to_sntprthorrjctd < 6 and iebf.sentby_branch_code <> iebf.ods_outlet then 0 else 0
    end as check,
    sie.draft_to_upload::int + iebf.upldorrsnt_to_sntprthorrjctd as "Total Time (Target = 13 Mins)"
    from {view}.insurance_efficiency_before_feedback iebf
    left join {database}.source_insurance_efficiency sie
    on iebf.doc_no::text = sie.order_number::text
    where upld_rw = 1  
    and iebf.sentby_branch_code  in (select bd.branch_code::text from reports_tables.branch_data bd where bd.sends_own_insurance = 'Yes')
    and iebf.sent_preauth_tm::date between '{start_date}' and '{today}'
    ) b
    where b.check = 1 
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_eff_af_feed_nodesk(start_date, engine):
    query = f"""
    select 
	doc_no as "Order Number",ods_outlet as "Outlet",
    ods_creator_name as "Staff Name",approval_update_tm as "Approval Feedback Sent to Branch",
    fdbck_rcvd_tm as "Branch Received Approval Feedback",apprvl_to_rcvd as "Time Taken"
    from mabawa_mviews.insurance_efficiency_after_feedback
    where ods_outlet not in (select branch_code::text from reports_tables.branch_data where sends_own_insurance = 'Yes')
    and fdbck_rcvd_tm::date between '{start_date}' and '{today}'
    and apprvl_to_rcvd > 5 
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_eff_af_feed_desk(start_date, engine):
    query = f"""
    with df as (
    select 
	doc_no,ods_outlet,ods_creator_name,approval_remark_tm2,fdbck_rcvd_tm,
	rmrk2_to_updt, apprvl_to_rcvd,
	case 
		when rmrk2_to_updt < 0  then apprvl_to_rcvd 
		else rmrk2_to_updt + apprvl_to_rcvd
	end as tt
    from mabawa_mviews.insurance_efficiency_after_feedback
    where ods_outlet in (
        select branch_code::text from reports_tables.branch_data where sends_own_insurance = 'Yes'
    ))
    select doc_no as "Order Number",ods_outlet as "Outlet",
    ods_creator_name as "Staff Name",
    approval_remark_tm2 as "Approval Feedback Received From Insurance Company",
    fdbck_rcvd_tm as "Branch Received Approval Feedback",
    tt as "Time Taken In Minutes"
    from df 
    where fdbck_rcvd_tm::date between '{start_date}' and '{today}'
    and tt > 5 
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_direct_insurance_conversion(start_date, engine, mview):
    query = f"""
    select ifc.doc_no as "Order Number",
    ifc.ods_status1 as "Current Status",
    ifc.cust_code as "Customer Code",
    ifc.ods_outlet as "Outlet",
    ifc.user_name as "Order Creator",
    i.insurance_company as "Insurance Company"
    from {mview}.insurance_feedback_conversion ifc 
    left join report_views.insurances i 
    on i.order_number::text = ifc.doc_no::text
    where fdbck_date::date between '{start_date}' and '{today}'
    and fdbck_type = 'Direct'
    and cnvrtd = 0
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_client_contacted_time(start_date, engine, view):
    query = f"""
    SELECT ieaf.doc_no::int AS "Order Number",
    CASE WHEN ieaf.apprvl_to_cstmr_cntctd IS NULL THEN 'Not Contacted' 
    ELSE CASE WHEN ieaf.apprvl_to_cstmr_cntctd = 1 
    THEN ieaf.apprvl_to_cstmr_cntctd::text || ' Minute' 
    ELSE ieaf.apprvl_to_cstmr_cntctd::text || ' Minutes' END
    END AS "Client Contacted After (Target = 10 (Mins))"
    FROM {view}.insurance_efficiency_after_feedback ieaf
    where ieaf.apprvl_updt_tm::date between '{start_date}' and '{today}'
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_plano_revised(start_date, engine):
    query = f"""
    select 
    isb.branch_code as "Outlet",
    isb.cust_code as "Customer Code",
    isb.plano_rx as "Plano RX",
    isb.insurance_name as "Insurance Company",
    isb.optom_name as "Optom Name",
    isb.handed_over_to as "EWC Handover",
    isb.last_viewed_by as "Last Viewed By",
    isb.scheme_type as "Scheme Type"
    from report_views.insurance_submission isb
    where create_date::date between '{start_date}' and '{today}'
    and isb.branch_code not in ('null', 'HOM')
    and ((isb.insurance_name not in ('AAR M-TIBA', 'AAR INSURANCE KENYA') 
    and scheme_type not in ('DIRECT', 'DIRECT + SMART')) 
    or insurance_name is null or scheme_type is null)
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_holidays(engine, dw):
    query = f"""
    select holiday_date as "Date",
    holiday_name as "Holiday"
    from {dw}.dim_holidays;
    """
    to_drop = pd.read_sql_query(query, con=engine)

    date_objects = pd.to_datetime(to_drop["Date"], dayfirst=True).dt.date
    holiday_dict = dict(zip(date_objects, to_drop["Holiday"]))

    return holiday_dict


def efficiency_after_feedback(engine, view, start_date) -> pd.DataFrame:
    query = f"""
    select 
    ieaf.doc_no as "Order Number",
    ieaf.ods_outlet as "Outlet",
    ieaf.ods_creator_name as "Order Creator",
    ieaf.fdbk_stts as "Insurance Feedback",
    TO_CHAR(ieaf.fdbk_rmrk_tm, 'YYYY-MM-DD HH24:MI') as "Feedback Time",
    TO_CHAR(ieaf.apprvl_updt_tm, 'YYYY-MM-DD HH24:MI') as "Time Updated on SAP",
    rmrk_to_updt as "Time Taken (Target = 5 Mins)"
    from {view}.insurance_efficiency_after_feedback ieaf 
    where rmrk_to_updt > 5
    and apprvl_updt_tm::date between '{start_date}' and '{today}'
    and ieaf.ods_outlet in (select branch_code::text from reports_tables.branch_data bd where sends_own_insurance = 'Yes')
    """

    data = pd.read_sql_query(query, con=engine)
    return data
