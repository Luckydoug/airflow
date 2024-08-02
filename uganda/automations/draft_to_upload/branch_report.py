from airflow.models import variable
import pandas as pd
from sub_tasks.libraries.utils import (
    uganda_path,
    target,
    create_unganda_engine,
    fetch_gsheet_data,
)

from reports.draft_to_upload.reports.draft import create_draft_upload_report
from reports.draft_to_upload.data.push_data import push_insurance_efficiency_data
from reports.draft_to_upload.utils.utils import (
    return_report_daterange,
    get_report_frequency,
    get_start_end_dates,
)
from reports.draft_to_upload.smtp.branches import send_branches_efficiency
from reports.draft_to_upload.smtp.smtp import clean_folders
from reports.draft_to_upload.data.fetch_data import (
    fetch_orders,
    fetch_orderscreen,
    fetch_insurance_companies,
    fetch_views,
    fetch_insurance_efficiency,
    fetch_daywise_efficiency,
    fetch_mtd_efficiency,
    fetch_branch_data,
)


engine = create_unganda_engine()
database = "mawingu_staging"
selection = get_report_frequency()
start_date = return_report_daterange(selection=selection)
start_date = pd.to_datetime(start_date, format="%Y-%m-%d").date()


orders = fetch_orders(engine=engine, database=database)

orderscreen = fetch_orderscreen(engine=engine, database=database)

all_views = fetch_views(database=database, engine=engine)

insurance_companies = fetch_insurance_companies(database=database, engine=engine)

orders = pd.merge(orders, all_views[["Code", "Last View Date"]], on="Code", how="left")

orders = pd.merge(
    orders,
    insurance_companies[
        [
            "DocNum",
            "Insurance Company",
            "Scheme Type",
            "Insurance Scheme",
            "Feedback 1",
            "Feedback 2",
        ]
    ],
    on="DocNum",
    how="left",
)

branch_data = fetch_branch_data(engine=engine, database="reports_tables")


def push_uganda_efficiency_data():
    working_hours = fetch_gsheet_data()["ug_working_hours"]
    date = return_report_daterange(selection="Daily")
    date = "2024-02-01"
    date = pd.to_datetime(date, format="%Y-%m-%d").date()
    push_insurance_efficiency_data(
        engine=engine,
        orderscreen=orderscreen,
        all_orders=orders,
        start_date=date,
        branch_data=branch_data,
        working_hours=working_hours,
        database=database,
    )


data_orders = fetch_insurance_efficiency(
    database=database, engine=engine, start_date=start_date, dw="mawingu_dw"
)

pstart_date, pend_date = get_start_end_dates(selection=selection)

daywise_efficiency = fetch_daywise_efficiency(
    database=database,
    engine=engine,
    start_date=pstart_date,
    end_date=pend_date,
    dw="mawingu_dw",
)

mtd_efficiency = fetch_mtd_efficiency(
    database=database,
    engine=engine,
    start_date=pstart_date,
    end_date=pend_date,
    dw="mawingu_dw",
)


def build_branches_efficiency():
    create_draft_upload_report(
        data_orders=data_orders,
        mtd_data=mtd_efficiency,
        daywise_data=daywise_efficiency,
        selection=selection,
        start_date=start_date,
        target=target,
        branch_data=branch_data,
        path=uganda_path,
        drop="",
    )


def trigger_efficiency_smtp():
    send_branches_efficiency(
        path=uganda_path,
        selection=selection,
        target=target,
        branch_data=branch_data,
        country="Uganda",
        log_file=f"{uganda_path}draft_upload/branch_log.txt",
    )


def clean_uganda_folder():
    clean_folders(path=uganda_path)


