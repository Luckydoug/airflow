import pandas as pd
from pangres import upsert
from airflow.models import variable
from reports.draft_to_upload.utils.utils import today
from reports.draft_to_upload.utils.utils import return_report_daterange
from sub_tasks.libraries.utils import create_unganda_engine
from reports.draft_to_upload.data.fetch_data import fetch_holidays
from sub_tasks.libraries.time_diff import calculate_time_difference
from sub_tasks.libraries.time_diff import working_hours_dictionary
from reports.draft_to_upload.data.fetch_data import fetch_working_hours


def fetch_corrected_forms(engine):
    start_date = return_report_daterange(selection="Daily")
    query = f"""
    with corrected_forms as (
    select
    *
    FROM (
    select
    row_number() OVER (PARTITION BY ors.doc_entry order by ors.odsc_date::date, ors.odsc_time asc) AS row_num,
    *
    FROM mawingu_staging.source_orderscreenc1 ors
    WHERE ors.odsc_status in ('Corrected Form Resent to Approvals Team', 'Corrected Form Resent to Optica Insurance')) a
    WHERE a.row_num = 1
    ),
    rejected as (
    SELECT
    *
    FROM (
    select
    row_number() OVER (PARTITION by ors.doc_entry order by ors.odsc_date::date, ors.odsc_time asc) AS row_num,
    *
    FROM mawingu_staging.source_orderscreenc1 ors
    WHERE ors.odsc_status in ('Rejected by Approvals Team'::text,'Rejected by Optica Insurance'::text)) a
    WHERE a.row_num = 1
    )
    select so.doc_no as order_number,
    so.ods_status as status,
    so.ods_createdon::date as create_date,
    so.ods_outlet as order_branch,
    su.user_name as order_creator,
    so.ods_creator as creator,
    rj.odsc_date::DATE + LPAD(rj.odsc_time::TEXT, 4, '0')::TIME as rejected_date,
    rj.odsc_remarks as rejection_rmks,
    co.odsc_date::DATE + LPAD(co.odsc_time::TEXT, 4, '0')::TIME as corrected_date,
    co.odsc_remarks as corrected_rmks
    from corrected_forms co
    inner join rejected rj on co.doc_entry = rj.doc_entry
    inner join mawingu_staging.source_orderscreen so
    on co.doc_entry = so.doc_entry
    inner join mawingu_staging.source_users su 
    on su.user_code::text = so.ods_creator::text
    where co.odsc_date::date between '{start_date}' and '{today}'
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def upsert_corrected_forms_data():
    engine = create_unganda_engine()
    holidays = fetch_holidays(engine=engine, dw = "mawingu_dw")
    working_hours = fetch_working_hours(engine=engine)
    work_hours = working_hours_dictionary(working_hours=working_hours)
    data = fetch_corrected_forms(engine=engine)

    data["time_taken"] = data.apply(
        lambda row: calculate_time_difference(
            row=row,
            x="rejected_date",
            y="corrected_date",
            working_hours=work_hours,
            outlet="order_branch",
            holiday_dict=holidays,
        ),
        axis=1,
    )

    upsert(
        engine=engine,
        df=data.set_index("order_number"),
        schema="mawingu_staging",
        table_name="corrected_forms_resent",
        if_row_exists="update",
        create_table=True,
    )




