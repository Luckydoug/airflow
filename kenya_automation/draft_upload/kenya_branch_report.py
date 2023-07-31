from airflow.models import variable
import pandas as pd
from sub_tasks.libraries.utils import (
    path, 
    target, 
    createe_engine,
    fetch_gsheet_data
)
from reports.draft_to_upload.reports.draft import create_draft_upload_report
from reports.draft_to_upload.data.push_data import push_insurance_efficiency_data
from reports.draft_to_upload.utils.utils import return_report_daterange, get_report_frequency
from reports.draft_to_upload.smtp.branches import send_branches_efficiency
from reports.draft_to_upload.smtp.smtp import clean_folders
from reports.draft_to_upload.data.fetch_data import (
    fetch_orders,
    fetch_orderscreen,
    fetch_insurance_companies,
    fetch_views,
    fetch_insurance_efficiency,
    fetch_daywise_efficiency,
    fetch_mtd_efficiency
)


engine =createe_engine()
database = "mabawa_staging"
selection = get_report_frequency()
start_date = return_report_daterange(selection=selection)
start_date = pd.to_datetime(start_date, format="%Y-%m-%d").date()

orders = fetch_orders(
    engine=engine,
    database=database
)

orderscreen = fetch_orderscreen(
    engine=engine,
    database=database
)

all_views = fetch_views(
    database=database,
    engine=engine
)

insurance_companies = fetch_insurance_companies(
    database=database,
    engine=engine
)

orders = pd.merge(
    orders,
    all_views[["Code", "Last View Date"]],
    on="Code",
    how="left"
)

orders = pd.merge(
    orders,
    insurance_companies[[
        "DocNum", "Insurance Company",
        "Scheme Type", "Insurance Scheme",
        "Feedback 1", "Feedback 2"
    ]],
    on="DocNum",
    how="left"
)


def push_kenya_efficiency_data():
    branch_data = fetch_gsheet_data()["branch_data"]
    working_hours = fetch_gsheet_data()["working_hours"]
    date = return_report_daterange(selection="Daily")
    date = pd.to_datetime(date, format="%Y-%m-%d").date()
    push_insurance_efficiency_data(
        engine=engine,
        orderscreen=orderscreen,
        all_orders=orders,
        start_date=date,
        branch_data=branch_data,
        working_hours=working_hours,
        database=database
    )


data_orders = fetch_insurance_efficiency(
    database=database,
    engine=engine,
    start_date=start_date
)

daywise_efficiency = fetch_daywise_efficiency(
    database=database,
    engine=engine
)

mtd_efficiency = fetch_mtd_efficiency(
    database=database,
    engine=engine
)


def build_branches_efficiency():
    branch_data = fetch_gsheet_data()["branch_data"]
    orders = fetch_gsheet_data()["orders_drop"]
    create_draft_upload_report(
        data_orders=data_orders,
        mtd_data=mtd_efficiency,
        daywise_data=daywise_efficiency,
        selection=selection,
        start_date=start_date,
        target=target,
        branch_data=branch_data,
        path=path,
        orders_drop=orders,
        drop="KENYA PIPELINE COMPANY"
    )

def trigger_efficiency_smtp():
    branch_data = fetch_gsheet_data()["branch_data"]
    send_branches_efficiency(
        path = path,
        selection=selection,
        target= target,
        branch_data=branch_data,
        log_file=f"{path}draft_upload/branch_log.txt"
    )

def clean_kenya_folder():
    clean_folders(path=path)

