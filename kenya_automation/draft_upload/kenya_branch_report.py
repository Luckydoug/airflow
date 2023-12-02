from airflow.models import variable
import pandas as pd
from sub_tasks.libraries.utils import (
    path,
    target,
    createe_engine
)
from reports.draft_to_upload.reports.draft import create_draft_upload_report
from reports.draft_to_upload.data.push_data import push_insurance_efficiency_data
from reports.draft_to_upload.utils.utils import return_report_daterange, get_report_frequency, get_start_end_dates
from reports.draft_to_upload.smtp.branches import send_branches_efficiency
from reports.draft_to_upload.smtp.smtp import clean_folders
from reports.draft_to_upload.reports.et_to_order import eyetest_order_time
from reports.draft_to_upload.data.fetch_data import (
    fetch_orders,
    fetch_orderscreen,
    fetch_insurance_companies,
    fetch_views,
    fetch_insurance_efficiency,
    fetch_daywise_efficiency,
    fetch_mtd_efficiency,
    fetch_branch_data,
    fetch_working_hours,
    fetch_eyetest_order
)


engine = createe_engine()
database = "mabawa_staging"
selection = get_report_frequency()
start_date = return_report_daterange(selection=selection)
start_date = pd.to_datetime(start_date, format="%Y-%m-%d").date()


def orderscreen():
    orderscreen = fetch_orderscreen(
        engine=engine,
        database=database
    )

    return orderscreen


def branch_data():
    branch_data = fetch_branch_data(
        engine=engine,
        database='reports_tables'
    )

    return branch_data


def working_hours() -> pd.DataFrame:
    working_hours = fetch_working_hours(
        engine=engine
    )
    return working_hours


def orders():
    all_views = fetch_views(
        database=database,
        engine=engine
    )

    insurance_companies = fetch_insurance_companies(
        database=database,
        engine=engine
    )

    orders = fetch_orders(
        engine=engine,
        database=database
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

    return orders


def push_kenya_efficiency_data():
    date = return_report_daterange(selection="Daily")
    date = pd.to_datetime(date, format="%Y-%m-%d").date()
    push_insurance_efficiency_data(
        engine=engine,
        orderscreen=orderscreen(),
        all_orders=orders(),
        start_date=date,
        branch_data=branch_data(),
        working_hours=working_hours(),
        database=database
    )


def data_orders():
    data_orders = fetch_insurance_efficiency(
        database=database,
        engine=engine,
        start_date=start_date,
        dw="mabawa_dw"
    )

    return data_orders

pstart_date, pend_date = get_start_end_dates(
    selection=selection
)

def daywise_efficiency():
    daywise_efficiency = fetch_daywise_efficiency(
        database=database,
        engine=engine,
        start_date=pstart_date,
        end_date=pend_date,
        dw="mabawa_dw"
    )

    return daywise_efficiency

def mtd_efficiency():
    mtd_efficiency = fetch_mtd_efficiency(
        database=database,
        engine=engine,
        start_date=pstart_date,
        end_date=pend_date,
        dw="mabawa_dw"
    )

    return mtd_efficiency



def eyetest_order():
    eyetest_order = fetch_eyetest_order(
        engine=engine,
        start_date=start_date
    )

    return eyetest_order


def build_branches_efficiency():
    create_draft_upload_report(
        data_orders=data_orders(),
        mtd_data=mtd_efficiency(),
        daywise_data=daywise_efficiency(),
        selection=selection,
        start_date=start_date,
        target=target,
        branch_data=branch_data(),
        path=path,
        drop="KENYA PIPELINE COMPANY"
    )

def build_eyetest_order() -> None:
    eyetest_order_time(
        data=eyetest_order(),
        path=path,
        selection=selection
    )



def trigger_efficiency_smtp():
    send_branches_efficiency(
        path=path,
        selection=selection,
        target=target,
        branch_data=branch_data(),
        country="Kenya",
        log_file=f"{path}draft_upload/branch_log.txt"
    )


def clean_kenya_folder():
    clean_folders(path=path)




