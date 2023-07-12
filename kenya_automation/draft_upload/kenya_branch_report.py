from airflow.models import variable
import pandas as pd
from sub_tasks.libraries.utils import (
    path, 
    target, 
    createe_engine,
    fetch_gsheet_data
)
from reports.draft_to_upload.reports.draft import create_draft_upload_report
from reports.draft_to_upload.utils.utils import return_report_daterange, get_report_frequency
from reports.draft_to_upload.smtp.branches import send_branches_efficiency
from reports.draft_to_upload.smtp.smtp import clean_folders
from reports.draft_to_upload.data.fetch_data import (
    fetch_orders,
    fetch_orderscreen,
    fetch_insurance_companies,
    fetch_views
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


def build_branches_efficiency():
    working_hours = fetch_gsheet_data()["working_hours"]
    branch_data = fetch_gsheet_data()["branch_data"]
    create_draft_upload_report(
        selection=selection,
        orderscreen=orderscreen,
        all_orders=orders,
        start_date=start_date,
        target=target,
        branch_data=branch_data,
        path=path,
        working_hours=working_hours,
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

