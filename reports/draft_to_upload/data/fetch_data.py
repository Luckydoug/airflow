import pandas as pd
from airflow.models import variable
from reports.draft_to_upload.utils.utils import today


def fetch_orderscreen(database, engine, start_date='2023-01-01'):
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


def fetch_orders(database, engine, start_date='2023-01-01'):
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


def fetch_views(database, engine, start_date='2023-01-01'):
    print(start_date, today)
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
    all_views = all_views.sort_values(
        by=["Create Date", "Create Time"], ascending=True)
    all_views = all_views.drop_duplicates(subset=["Code"], keep="last")
    all_views.loc[:, "Last View Date"] = pd.to_datetime(
        all_views["Create Date"].astype(str) + " " +
        all_views["Create Time"].astype(str), format="%Y-%m-%d %H:%M:%S", errors="coerce"
    )
    return all_views


def fetch_insurance_companies(database, engine, start_date='2023-01-01'):
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

    insurance_companies = pd.read_sql_query(
        insurance_companies_query, con=engine)
    return insurance_companies


def fetch_eyetests(database, engine, start_date='2023-01-01'):
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


def fetch_salesorders(database, engine, start_date='2023-01-01'):
    sales_orders_query = f"""
    select draft_orderno::int as "Order Number" from {database}.source_orders_header
    where posting_date::date between '{start_date}' and '{today}'
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


def fetch_registrations(engine, database,  table, table2, start_date='2023-01-01'):
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
    """.format(today=today, start_date=start_date)

    registrations = pd.read_sql_query(registrations_query, con=engine)
    return registrations


def fetch_payments(engine, database, start_date='2023-01-01'):
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


def fetch_planos(database, engine, schema, users, customers, table, views, start_date='2023-01-01'):
    datest = pd.to_datetime(start_date, format="%Y-%m-%d").date()
    datend = pd.to_datetime(today, format="%Y-%m-%d").date()
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
            CASE WHEN a.days <= %s THEN 1 ELSE 0 END AS "Conversion"
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
                    status NOT IN ('Cancel', 'Unstable', 'Hold', 'CanceledEyeTest')
                    AND (patient_to_ophth NOT IN ('Yes') OR patient_to_ophth IS NULL)
            ) AS a
            LEFT JOIN {schema}.{users} b ON a.optom::text = b.se_optom::text
            LEFT JOIN {schema}.{customers} AS customers ON a.cust_code::text = customers.cust_code::text
            LEFT JOIN {database}.source_insurance_company AS insurance ON customers.cust_insurance_company::text = insurance.insurance_code::text
        WHERE
            a.r = 1
            AND a.plano_rx = 'Y'
            AND a.create_date::date >= %s
            AND a.create_date::date <= %s
            AND a.cust_code <> '10026902'
    """

    all_planos = pd.read_sql_query(
        planos_query, con=engine, params=(14, datest, datend)
    )

    return all_planos


def fetch_planorderscreen(database, engine, start_date='2023-01-01'):
    plano_orderscreen_query = f"""
    SELECT 
        orderscreen.doc_entry AS "DocEntry", 
        orders.doc_no::int AS "Order Number", 
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
        AND (orderscreen.odsc_status = 'Resent Pre-Auth to Insurance Company' 
        OR orderscreen.odsc_status = 'Sent Pre-Auth to Insurance Company')
    """

    plano_orderscreen = pd.read_sql_query(plano_orderscreen_query, con=engine)
    return plano_orderscreen


def fetch_detractors(database, engine, start_date='2023-01-01'):
    detractors_query = f"""
    select sap_internal_number as "SAP Internal Number", branch as "Branch",
    trigger_date::date as "Trigger Date", nps_rating::int as "NPS Rating", long_feedback as "Long Remarks"
    from {database}.source_ajua_info
    where trigger_date::date between '{start_date}' and '{today}'
    and ajua_response != '0'
    """
    surveys = pd.read_sql_query(detractors_query, con=engine)
    return surveys


def fetch_opening_time(database, engine, start_date='2023-01-01'):
    opening_time_query = f"""
    select date::date as "Date", "day" as "Day", "branch" as "Branch", 
    "opening_time" as "Opening Time", time_opened as "Time Opened", "lost_time" as "Lost Time"
    from {database}.source_opening_time
    where lost_time::int > 0::int and date::date between '{start_date}' and '{today}'
    """

    opening_time = pd.read_sql_query(opening_time_query, con=engine)
    return opening_time


def fetch_insurance_efficiency(database, engine, start_date):
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
    where upload_time::date between '{start_date}' and '{today}';
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_daywise_efficiency(database, engine, start_date, end_date):
    daywise_query = f"""
        SELECT
        outlet AS "Outlet",
        upload_time::date AS "Date",
        ROUND((SUM(CASE WHEN draft_to_upload::int <= 8 THEN 1 ELSE 0 END)::DECIMAL 
        / COUNT(upload_time))::DECIMAL * 100) AS "Efficiency"
        FROM
            {database}.source_insurance_efficiency sie
            where upload_time::date between '{start_date}' and '{end_date}'
        GROUP BY
        outlet, upload_time::date
        order by upload_time::date;
    """
    data = pd.read_sql_query(daywise_query, con=engine)
    return data


def fetch_mtd_efficiency(database, engine, start_date, end_date):
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
