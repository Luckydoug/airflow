import pandas as pd
from airflow.models import variable
from reports.draft_to_upload.utils.utils import target
from sub_tasks.libraries.utils import (
    rwanda_path,
    create_rwanda_engine,
    createe_engine,
    fourth_week_start
)

from reports.draft_to_upload.smtp.smtp import (
    send_draft_upload_report,
    send_to_branches,
    clean_folders
)

from reports.draft_to_upload.reports.draft import (
    create_draft_upload_report
)
from reports.draft_to_upload.reports.opening_time import (
    create_opening_time_report
)
from reports.draft_to_upload.utils.utils import get_report_frequency, return_report_daterange, get_start_end_dates
from reports.draft_to_upload.data.push_data import push_insurance_efficiency_data
from reports.draft_to_upload.reports.rejections import create_rejection_report
from reports.draft_to_upload.reports.plano import (
    create_plano_report
)
from reports.draft_to_upload.reports.sops import create_ug_sops_report
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
    fetch_opening_time,
    fetch_daywise_efficiency,
    fetch_insurance_efficiency,
    fetch_mtd_efficiency,
    fetch_customers,
    fetch_daywise_rejections,
    fetch_mtd_rejections,
    fetch_branch_data,
    fetch_working_hours
)


database = "voler_staging"
engine = create_rwanda_engine()
engine2 = createe_engine()
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

eyetests = fetch_eyetests(
    database=database,
    engine=engine
)

orderscreen = fetch_orderscreen(
    database=database,
    engine=engine
)

branch_data = fetch_branch_data(
    engine=engine,
    database="reports_tables"
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
    engine=engine
)


planos = fetch_planos(
    engine=engine,
    database=database,
    schema="voler_staging",
    customers="source_customers",
    users="source_users",
    table="et_conv",
    views="voler_mviews"
)

plano_orderscreen = fetch_planorderscreen(
    database=database,
    engine=engine
)

sops_info = fetch_sops_branch_info(engine=engine2)
registrations = fetch_registrations(
    engine=engine,
    database="voler_staging",
    table="source_customers",
    table2="source_users"
)

date = ''
if selection == "Daily":
    date = str(start_date)
if selection == "Weekly":
    date = fourth_week_start
if selection == "Monthly":
    date = '2023-09-01'

opening_data = fetch_opening_time(
    database=database,
    start_date=date,
    engine=engine
)


def build_rwanda_opening_time():
    create_opening_time_report(
        opening_data,
        rwanda_path
    )

def working_hours() -> pd.DataFrame:
    working_hours = fetch_working_hours(
        engine=engine
    )
    return working_hours

def push_rwanda_efficiency_data():
    date = return_report_daterange(selection="Daily")
    date = pd.to_datetime(date, format="%Y-%m-%d").date()
    push_insurance_efficiency_data(
        engine=engine,
        orderscreen=orderscreen,
        all_orders=all_orders,
        start_date=date,
        branch_data=branch_data,
        working_hours=working_hours(),
        database=database
    )


pstart_date, pend_date = get_start_end_dates(
    selection=selection
)

data_orders = fetch_insurance_efficiency(
    database=database,
    engine=engine,
    start_date=start_date,
    dw="voler_dw"
)


daywise_efficiency = fetch_daywise_efficiency(
    database=database,
    engine=engine,
    start_date=pstart_date,
    end_date=pend_date,
    dw="voler_dw"
)

mtd_efficiency = fetch_mtd_efficiency(
    database=database,
    engine=engine,
    start_date=pstart_date,
    end_date=pend_date,
    dw="voler_dw"
)


def build_rw_draft_upload():
    create_draft_upload_report(
        mtd_data=mtd_efficiency,
        daywise_data=daywise_efficiency,
        data_orders=data_orders,
        selection=selection,
        start_date=start_date,
        target=target,
        branch_data=branch_data,
        path=rwanda_path
    )



daywise_rejections = fetch_daywise_rejections(
    database=database,
    view="voler_mviews",
    engine=engine,
    start_date=pstart_date,
    end_date=pend_date
)

mtd_rejections = fetch_mtd_rejections(
    database=database,
    view="voler_mviews",
    engine=engine,
    start_date=pstart_date,
    end_date=pend_date
)

def build_rw_rejections():
    create_rejection_report(
        orderscreen=orderscreen,
        all_orders=all_orders,
        branch_data=branch_data,
        path=rwanda_path,
        selection=selection,
        start_date=start_date,
        sales_orders=sales_orders,
        mtd_data=mtd_rejections,
        daywise_data=daywise_rejections
    )



customers = fetch_customers(
    database="voler_mviews",
    engine=engine,
    start_date=start_date
)

def build_rw_sops():
    create_ug_sops_report(
        selection=selection,
        branch_data=branch_data,
        sops_info=sops_info,
        start_date=start_date,
        path=rwanda_path,
        customers=customers
    )


def build_plano_report():
    create_plano_report(
        branch_data=branch_data,
        path=rwanda_path,
        registrations=registrations,
        payments=payments,
        all_planos=planos,
        plano_orderscreen=plano_orderscreen,
        all_orders=all_orders,
        selection=selection
    )


def trigger_rwanda_smtp():
    #This is the function to trigger SMTP.
    #When country is the name of the function the report is for.
    #If wanted to send for Rwanda then you would pass Rwanda as the argument for country parameter.
    #To Test this report. please pass Test as the argument.
    send_draft_upload_report(
        selection=selection,
        path=rwanda_path,
        country="Rwanda",
        target=target
    )

def trigger_rwanda_branches_smtp():
    send_to_branches(
        branch_data=branch_data,
        selection=selection,
        path=rwanda_path,
        country="Rwanda",
        filename=f"{rwanda_path}draft_upload/log.txt"
    )

def clean_rwanda_folder():
    clean_folders(path=rwanda_path)








