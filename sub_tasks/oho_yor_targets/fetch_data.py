import pandas as pd
from airflow.models import variable
from sub_tasks.libraries.utils import (createe_engine, return_incentives_daterange)
import datetime


engine = createe_engine()
start_date, end_date = return_incentives_daterange()
today = datetime.date.today()

def fetch_orders():
    orders_query = """
        SELECT CAST(orders.doc_entry AS INT) AS "DocEntry",
        CAST(orders.doc_no AS INT) AS "DocNum",
        orders.ods_createdon::date AS "CreateDate",
        CASE
            WHEN length(ods_createdat::text) in (1,2) THEN null
            ELSE (left(ods_createdat::text,(length(ods_createdat::text)-2))||':'||right(ods_createdat::text, 2))::time
        END AS "CreateTime",
        ods_status as "Status",
        CAST(orders.cust_code AS INT) AS "Customer Code",
        orders.ods_outlet AS "Outlet",
        orders.ods_insurance_order as "Insurance Order",
        orders.ods_creator AS "Creator",
        users.user_name AS "Order Creator"
        FROM mabawa_staging.source_orderscreen orders
        LEFT JOIN mabawa_dw.dim_users AS users ON CAST(orders.ods_creator AS TEXT) = CAST(users.user_code AS TEXT)
        WHERE orders.ods_createdon::date BETWEEN '2019-01-01' AND '{today}'
        AND CAST(orders.cust_code AS TEXT) <> ''
        AND NOT EXISTS (SELECT 1 FROM mabawa_staging.source_exec_users exempt WHERE CAST(orders.ods_creator AS TEXT) = user);
    """.format(today=today)

    orders = pd.read_sql_query(orders_query, con=engine)
    return orders

def fetch_journals():
    journals_query = """
    SELECT posting_date::date as "Posting Date", je_type as "JE_Type",
    draft_order_no as "Order Number"
    FROM mabawa_staging.source_ojdt
    WHERE posting_date::date BETWEEN '2019-01-01' and '{end_date}'
    """.format(end_date=end_date)

    all_journals = pd.read_sql_query(journals_query, con=engine)
    return all_journals

def fetch_payments():
    payments_query = """
    select payments.doc_entry as "PyDocEntry",payments.full_doc_type as "DocType",
    payments.mode_of_pay as "Mode of Pay", payments.draft_orderno::int as "Order Number",
    payments.createdon::date as "CreateDate", payments.branch_code as "Branch",
    orders.ods_normal_repair_order as "Order Type",
    payments.cust_code as "Customer Code", total_amt as "Total Amount",
    payments.advance_amt as "Advance Amount", remaining_amt as "Remaining Amount"
    from mabawa_staging.source_payment as payments
    left join mabawa_staging.source_orderscreen as orders
    on payments.draft_orderno::text = orders.doc_no::text
    where payments.full_doc_type = 'Order Payment' and
    payments.status <> 'Cancel' and
    payments.status <> 'cancel' and
    payments.draft_orderno <> 'Close' and
    createdon::date  between '2019-01-01' and '{end_date}'
    """.format(end_date=end_date)

    all_payments = pd.read_sql_query(payments_query, con=engine)
    return all_payments

def fetch_targets():
    today = datetime.date.today()
    year = today.year
    month = today.strftime("%B")
    targets_query = """
    select warehouse_code::text as "Outlet", user_code::text as "Payroll Number",
    user_name as "Name",target_branch_amount as "Cash Branch Target", target_insurance_branch as "Insurance Branch Target",
    target_individual_cash as "Cash Target",
    target_individual_insurance as "Insurance Target"
    from mabawa_mviews.v_staff_targets_2
    where budget_year = '{year}' and
    budget_month = '{month}'
    """.format(month=month, year=year)

    targets = pd.read_sql_query(targets_query, con=engine)
    return targets

def fetch_cash_payments():
    incentive_query = """
    select incentive.doc_no as "Order Number", final_date::date as "Date",
    incentive.amount_paid::int as "Full Amount", incentive.ods_outlet as "Outlet",
    incentive.ods_creator as "Creator",incentive.user_name as "Order Creator"
    from mabawa_mviews.incentive_cash incentive
    where final_date::date between '{start_date}' and '{end_date}'
    and final_date is not null
    """.format(start_date=start_date, end_date=end_date)

    cash_payments = pd.read_sql_query(incentive_query, con=engine)
    return cash_payments
