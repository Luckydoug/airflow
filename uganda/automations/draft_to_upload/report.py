import pandas as pd
from airflow.models import variable
from reports.draft_to_upload.utils.utils import uganda_target
from sub_tasks.libraries.utils import (
    uganda_path,
    fetch_gsheet_data,
    create_unganda_engine,
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
    push_branch_opening_time_data, create_opening_time_report)
from reports.draft_to_upload.utils.utils import get_report_frequency, return_report_daterange
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
    fetch_opening_time
)


database = "mawingu_staging"
engine = create_unganda_engine()
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
    schema="mawingu_staging",
    customers="source_customers",
    users="source_users",
    table="et_conv",
    views="mawingu_mviews"
)

plano_orderscreen = fetch_planorderscreen(
    database=database,
    engine=engine
)

sops_info = fetch_sops_branch_info(engine=engine2)
registrations = fetch_registrations(
    engine=engine,
    database="mawingu_staging",
    table="source_customers",
    table2="source_users"
)


def push_uganda_opening_time():
    #This function is tested and working. But it can fail at some point.
    #To Debug this function, kindly proceed to this G-Sheet; https://docs.google.com/spreadsheets/d/1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8/edit#gid=0
    #On the sheet name called Uganda Opening Time, check if the columns are defined exactly the way the appear on the rename object below
    #The Keys for the rename object are the column names expected on the G-Sheet.
    #Modify the column names accordingly.
    uganda_opening = fetch_gsheet_data()["uganda_opening"]
    push_branch_opening_time_data(
        opening_time=uganda_opening,
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


def build_uganda_opening_time():
    create_opening_time_report(
        opening_data,
        uganda_path
    )


def build_ug_draft_upload():
    branch_data = fetch_gsheet_data()["ug_srm_rm"]
    working_hours = fetch_gsheet_data()["ug_working_hours"]
    create_draft_upload_report(
        selection=selection,
        orderscreen=orderscreen,
        all_orders=all_orders,
        start_date=start_date,
        target=uganda_target,
        branch_data=branch_data,
        path=uganda_path,
        working_hours=working_hours
    )


def build_ug_rejections():
    branch_data = fetch_gsheet_data()["ug_srm_rm"]
    create_rejection_report(
        orderscreen=orderscreen,
        all_orders=all_orders,
        branch_data=branch_data,
        path=uganda_path,
        selection=selection,
        start_date=start_date,
        sales_orders=sales_orders
    )


def build_ug_sops():
    branch_data = fetch_gsheet_data()["ug_srm_rm"]
    create_ug_sops_report(
        selection=selection,
        branch_data=branch_data,
        sops_info=sops_info,
        start_date=start_date,
        all_orders=all_orders,
        registrations=registrations,
        eyetests=eyetests,
        path=uganda_path
    )


def build_plano_report():
    branch_data = fetch_gsheet_data()["ug_srm_rm"]
    create_plano_report(
        branch_data=branch_data,
        path=uganda_path,
        registrations=registrations,
        payments=payments,
        all_planos=planos,
        plano_orderscreen=plano_orderscreen,
        all_orders=all_orders,
        selection=selection
    )


def trigger_uganda_smtp():
    #This is the function to trigger SMTP.
    #When country is the name of the function the report is for.
    #If wanted to send for Rwanda then you would pass Rwanda as the argument for country parameter.
    #To Test this report. please pass Test as the argument.
    send_draft_upload_report(
        selection=selection,
        path=uganda_path,
        country="Test",
        target=uganda_target
    )


def clean_uganda_folder():
    clean_folders(path=uganda_path)






