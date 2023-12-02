import pandas as pd
import numpy as np 
from airflow.models import variable
from reports.draft_to_upload.reports.pending_insurance import create_pending_insurance
from reports.draft_to_upload.data.fetch_data import fetch_pending_insurance
from reports.draft_to_upload.utils.utils import get_report_frequency
from reports.draft_to_upload.utils.utils import return_report_daterange
from sub_tasks.libraries.utils import createe_engine
from reports.draft_to_upload.data.fetch_data import fetch_planorderscreen
from sub_tasks.libraries.utils import path
from reports.draft_to_upload.smtp.pending_insurance import send_pending_insurance
from reports.draft_to_upload.data.fetch_data import fetch_branch_data
from reports.draft_to_upload.data.fetch_data import fetch_submitted_insurance

engine = createe_engine()
selection = get_report_frequency()
selection = "Daily"
start_date = return_report_daterange(selection)


def pending_insurances():
    pending_insurances = fetch_pending_insurance(
        engine=engine,
        start_date=start_date
    )

    return pending_insurances


def submitted_insurance():
    submitted_insurance = fetch_submitted_insurance(
        start_date=start_date,
        engine=engine
    )

    return submitted_insurance


def branch_data():
    branch_data = fetch_branch_data(
        engine=engine,
        database="reports_tables"
    )

    return branch_data


def build_pending_insurances():
    create_pending_insurance(
        non_conversions=pending_insurances(),
        order_screen=submitted_insurance(),
        path=path,
        selection=selection
    )

def send_to_branches():
    send_pending_insurance(
        path=path,
        branch_data=branch_data(),
        log_file=f"{path}draft_upload/ped_log.txt"
    )






