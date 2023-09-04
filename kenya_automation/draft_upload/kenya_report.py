import pandas as pd
from airflow.models import variable
from sub_tasks.libraries.utils import (
    path,
    createe_engine,
    fourth_week_start,
    first_week_start,
    fetch_gsheet_data,
    target
)

from reports.draft_to_upload.smtp.smtp import (
    send_draft_upload_report,
    clean_folders,
    send_to_branches
)

from reports.draft_to_upload.reports.draft import (
    create_draft_upload_report
)
from reports.draft_to_upload.utils.utils import get_report_frequency, return_report_daterange, get_start_end_dates
from reports.draft_to_upload.reports.rejections import create_rejection_report
from reports.draft_to_upload.reports.plano import (
    create_plano_report
)
from reports.draft_to_upload.reports.opening_time import (
    push_branch_opening_time_data,
    create_opening_time_report
)
from reports.insurance_conversion.data.fetch_data import FetchData
from reports.insurance_conversion.reports.conversion import create_insurance_conversion
from reports.draft_to_upload.reports.sops import create_ug_sops_report
from reports.draft_to_upload.reports.ratings import create_ratings_report
from reports.draft_to_upload.data.push_data import push_insurance_efficiency_data
from reports.draft_to_upload.data.fetch_data import (
    fetch_views,
    fetch_orders,
    fetch_eyetests,
    fetch_salesorders,
    fetch_payments,
    fetch_orderscreen,
    fetch_sops_branch_info,
    fetch_insurance_companies,
    fetch_planos,
    fetch_planorderscreen,
    fetch_registrations,
    fetch_detractors,
    fetch_opening_time,
    fetch_insurance_efficiency,
    fetch_mtd_efficiency,
    fetch_daywise_efficiency,
    fetch_daywise_rejections,
    fetch_mtd_rejections,
    fetch_customers
)


database = "mabawa_staging"
engine = createe_engine()
selection = get_report_frequency()
start_date = return_report_daterange(
    selection=selection
)
start_date = pd.to_datetime(start_date, format="%Y-%m-%d").date()

all_views = fetch_views(
    database=database,
    engine=engine
)

all_orders = fetch_orders(
    database=database,
    engine=engine
)

orders = all_orders.copy()

eyetests = fetch_eyetests(
    database=database,
    engine=engine
)

orderscreen = fetch_orderscreen(
    database=database,
    engine=engine
)

insurance_companies = fetch_insurance_companies(
    database=database,
    engine=engine
)

all_orders = pd.merge(
    all_orders,
    all_views[["Code", "Last View Date"]],
    on="Code",
    how="left"
)

all_orders = pd.merge(
    all_orders,
    insurance_companies[[
        "DocNum", "Insurance Company",
        "Scheme Type", "Insurance Scheme",
        "Feedback 1", "Feedback 2"
    ]],
    on="DocNum",
    how="left"
)

sales_orders = fetch_salesorders(
    database=database,
    engine=engine
)


payments = fetch_payments(
    database=database,
    engine=engine,
    start_date='2019-01-01'
)

planos = fetch_planos(
    engine=engine,
    database=database,
    schema="mabawa_dw",
    customers="dim_customers",
    users="dim_users",
    table="et_conv",
    views="mabawa_mviews",
    start_date=str(start_date)
)

plano_orderscreen = fetch_planorderscreen(
    database=database,
    engine=engine,
    start_date=str(start_date)
)

sops_info = fetch_sops_branch_info(engine=engine)

registrations = fetch_registrations(
    engine=engine,
    database="mabawa_dw",
    table="dim_customers",
    table2="dim_users",
    start_date='2019-01-01'
)

surveys = fetch_detractors(
    database=database,
    engine=engine
)

date = ''
if selection == "Daily":
    date = str(start_date)
if selection == "Weekly":
    date = fourth_week_start
if selection == "Monthly":
    date = '2023-08-01'

pstart_date, pend_date = get_start_end_dates(
    selection=selection
)


opening_data = fetch_opening_time(
    database=database,
    engine=engine,
    start_date=date
)


def push_kenya_efficiency_data():
    branch_data = fetch_gsheet_data()["branch_data"]
    working_hours = fetch_gsheet_data()["working_hours"]
    date = return_report_daterange(selection="Daily")
    date = pd.to_datetime(date, format="%Y-%m-%d").date()
    push_insurance_efficiency_data(
        engine=engine,
        orderscreen=orderscreen,
        all_orders=all_orders,
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
    engine=engine,
    start_date=pstart_date,
    end_date=pend_date
)

mtd_efficiency = fetch_mtd_efficiency(
    database=database,
    engine=engine,
    start_date=pstart_date,
    end_date=pend_date
)


def build_kenya_draft_upload():
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


daywise_rejections = fetch_daywise_rejections(
    database=database,
    view="mabawa_mviews",
    engine=engine,
    start_date=pstart_date,
    end_date=pend_date
)

mtd_rejections = fetch_mtd_rejections(
    database=database,
    view="mabawa_mviews",
    engine=engine,
    start_date=pstart_date,
    end_date=pend_date
)


def build_kenya_rejections():
    branch_data = fetch_gsheet_data()["branch_data"]
    to_drop = fetch_gsheet_data()["rejections_drop"]
    create_rejection_report(
        orderscreen=orderscreen,
        all_orders=all_orders,
        branch_data=branch_data,
        path=path,
        selection=selection,
        start_date=start_date,
        sales_orders=sales_orders,
        mtd_data=mtd_rejections,
        daywise_data=daywise_rejections,
        drop = to_drop
    )

customers = fetch_customers(
    database="mabawa_mviews",
    engine=engine,
    start_date=start_date
)

def build_kenya_sops():
    branch_data = fetch_gsheet_data()["branch_data"]
    create_ug_sops_report(
        selection=selection,
        branch_data=branch_data,
        sops_info=sops_info,
        start_date=start_date,
        path=path,
        customers=customers
    )


def build_kenya_plano_report():
    branch_data = fetch_gsheet_data()["branch_data"]
    create_plano_report(
        branch_data=branch_data,
        path=path,
        registrations=registrations,
        payments=payments,
        all_planos=planos,
        plano_orderscreen=plano_orderscreen,
        all_orders=all_orders,
        selection=selection
    )


def build_kenya_ratings_report():
    branch_data = fetch_gsheet_data()["branch_data"]
    create_ratings_report(
        selection=selection,
        surveys=surveys,
        branch_data=branch_data,
        path=path
    )


def trigger_kenya_smtp():
    send_draft_upload_report(
        selection=selection,
        path=path,
        country="Test",
        target=target
    )


def trigger_kenya_branches_smtp():
    branch_data = fetch_gsheet_data()["branch_data"]
    send_to_branches(
        branch_data=branch_data,
        selection=selection,
        path=path,
        filename=f"{path}draft_upload/log.txt"
    )


def push_kenya_opening_time():
    opening_time = fetch_gsheet_data()["opening_time"]
    push_branch_opening_time_data(
        opening_time=opening_time,
        rename={
            "Date": "date",
            "Day of the week": "day",
            "Branch": "branch",
            "ReportingTime": "reporting_time",
            "OpeningTime": "opening_time",
            "Time Opened": "time_opened"
        },
        database="mabawa_staging",
        table="source_opening_time",
        engine=engine,
        form="%d-%m-%Y"
    )


def build_kenya_opening_time():
    create_opening_time_report(
        opening_data,
        path
    )


def clean_kenya_folder():
    clean_folders(path=path)


data_fetcher = FetchData(
    engine=engine,
    database="mabawa_staging"
)

orderscreen = data_fetcher.fetch_orderscreen(
    start_date=start_date
)
insurance_companies = data_fetcher.fetch_insurance_companies()
orders = data_fetcher.fetch_orders()
sales_orders = data_fetcher.fetch_sales_orders(
    start_date=first_week_start
)


def build_kenya_insurance_conversion() -> None:
    branch_data = fetch_gsheet_data()["branch_data"]
    working_hours = fetch_gsheet_data()["working_hours"]
    create_insurance_conversion(
        path=path,
        all_orders=orders,
        orderscreen=orderscreen,
        branch_data=branch_data,
        sales_orders=sales_orders,
        insurance_companies=insurance_companies,
        selection=selection,
        date = start_date,
        working_hours=working_hours
    )











