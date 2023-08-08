import pandas as pd
from airflow.models import variable
from reports.draft_to_upload.utils.utils import target
from sub_tasks.libraries.utils import (
    rwanda_path,
    fetch_gsheet_data,
    create_rwanda_engine,
    createe_engine,
    fourth_week_start
)

from reports.draft_to_upload.smtp.smtp import (
    send_draft_upload_report,
    clean_folders
)

from reports.draft_to_upload.reports.draft import (
    create_draft_upload_report
)
from reports.draft_to_upload.reports.opening_time import (
    push_branch_opening_time_data, 
    create_opening_time_report
)
from reports.draft_to_upload.utils.utils import get_report_frequency, return_report_daterange
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
    fetch_mtd_rejections
)


database = "voler_staging"
engine = create_rwanda_engine()
engine2 = createe_engine()
selection = "Weekly"
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


def push_rwanda_opening_time():
    #This function is tested and working. But it can fail at some point.
    #To Debrw this function, kindly proceed to this G-Sheet; https://docs.google.com/spreadsheets/d/1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8/edit#gid=0
    #On the sheet name called rwanda Opening Time, check if the columns are defined exactly the way the appear on the rename object below
    #The Keys for the rename object are the column names expected on the G-Sheet.
    #Modify the column names accordingly.
    rwanda_opening = fetch_gsheet_data()["rwanda_opening"]
    push_branch_opening_time_data(
        opening_time=rwanda_opening,
        rename={
            "Date": "date",
            "Day": "day",
            "Branch": "branch",
            "Reporting Time": "reporting_time",
            "Opening Time": "opening_time",
            "Time Opened": "time_opened"
        },
        database=database,
        table="source_opening_time",
        engine=engine,
        form="%d-%b-%y"
    )


date = ''
if selection == "Daily":
    date = str(start_date)
if selection == "Weekly":
    date = fourth_week_start
if selection == "Monthly":
    date = '2023-07-01'

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


def push_rwanda_efficiency_data():
    branch_data = fetch_gsheet_data()["rw_srm_rm"]
    working_hours = fetch_gsheet_data()["rw_working_hours"]
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
    engine=engine
)

mtd_efficiency = fetch_mtd_efficiency(
    database=database,
    engine=engine
)


def build_rw_draft_upload():
    branch_data = fetch_gsheet_data()["rw_srm_rm"]
    create_draft_upload_report(
        mtd_data=mtd_efficiency,
        daywise_data=daywise_efficiency,
        data_orders=data_orders,
        selection=selection,
        start_date=start_date,
        target=target,
        branch_data=branch_data,
        path=rwanda_path,
    )



daywise_rejections = fetch_daywise_rejections(
    database=database,
    view="voler_mviews",
    engine=engine
)

mtd_rejections = fetch_mtd_rejections(
    database=database,
    view="voler_mviews",
    engine=engine
)

def build_rw_rejections():
    branch_data = fetch_gsheet_data()["rw_srm_rm"]
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
    branch_data = fetch_gsheet_data()["rw_srm_rm"]
    create_ug_sops_report(
        selection=selection,
        branch_data=branch_data,
        sops_info=sops_info,
        start_date=start_date,
        path=rwanda_path,
        customers=customers
    )


def build_plano_report():
    branch_data = fetch_gsheet_data()["rw_srm_rm"]
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
        country="Test",
        target=target
    )


def clean_rwanda_folder():
    clean_folders(path=rwanda_path)






