import pandas as pd
from airflow.models import variable
from sub_tasks.libraries.utils import (
    createe_engine, 
    return_incentives_daterange
)
import datetime
from datetime import timedelta


engine = createe_engine()
start_date, end_date = return_incentives_daterange()
today = datetime.date.today()

def fetch_insurance_payments():
    query = f"""
    select 
    ii.doc_no as "DocNum",
    ii.ods_creator as "Creator",
    ii.user_name as "Order Creator",
    ii.ods_createdon as "CreateDate",
    ii.ods_outlet as "Outlet"
    from mabawa_mviews.incentive_insurance2 ii 
    where ii.creation_date::date between '{start_date}' and '{end_date}'
    and ods_outlet in ('OHO', 'YOR')
    """

    return pd.read_sql_query(query, con = engine)

def fetch_targets():
    today = datetime.date.today()
    if today.day == 1:
        today = today - timedelta(days=1)
    year = today.year
    month = today.strftime("%B")

    targets_query = f"""
    select warehouse_code::text as "Outlet", user_code::text as "Payroll Number",
    user_name as "Name",target_branch_amount as "Cash Branch Target", 
    target_insurance_branch as "Insurance Branch Target",
    target_individual_cash as "Cash Target",
    target_individual_insurance as "Insurance Target"
    from mabawa_mviews.v_staff_targets_2
    where budget_year = '{year}' and
    budget_month = '{month}'
    """

    targets = pd.read_sql_query(targets_query, con=engine)
    return targets

def fetch_cash_payments():
    incentive_query = f"""
    select incentive.doc_no as "Order Number", final_date::date as "Date",
    incentive.amount_paid::int as "Full Amount", incentive.ods_outlet as "Outlet",
    incentive.ods_creator as "Creator",incentive.user_name as "Order Creator"
    from mabawa_mviews.incentive_cash incentive
    where final_date::date between '{start_date}' and '{end_date}'
    and final_date is not null
    """

    cash_payments = pd.read_sql_query(incentive_query, con=engine)
    return cash_payments
