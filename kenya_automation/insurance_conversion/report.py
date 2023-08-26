from airflow.models import variable
from reports.insurance_conversion.reports.conversion import create_insurance_conversion
from reports.insurance_conversion.data.fetch_data import FetchData
from reports.insurance_conversion.smtp.smtp import send_to_management, mop_folder, send_to_branches
from sub_tasks.libraries.utils import (
    first_week_start, 
    createe_engine,
    fetch_gsheet_data,
    path
)
from reports.draft_to_upload.utils.utils import return_report_daterange

engine = createe_engine()
selection = "Weekly"
start_date = return_report_daterange(selection=selection)
data_fetcher = FetchData(
    engine=engine,
    database="mabawa_staging"
)

orderscreen = data_fetcher.fetch_orderscreen(
    start_date=first_week_start
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
        selection=selection,
        working_hours=working_hours,
        all_orders=orders,
        orderscreen=orderscreen,
        branch_data=branch_data,
        sales_orders=sales_orders,
        insurance_companies=insurance_companies,
        date = start_date
    )

def send_to_kenya_management() -> None:
    send_to_management(
        selection=selection,
        country = "Kenya",
        path=path
    )

def send_to_kenya_branches() -> None:
    branch_data = fetch_gsheet_data()["branch_data"]
    send_to_branches(
        path=path,
        branch_data=branch_data,
        filename=f"{path}insurance_conversion/branch_log.txt"
        
    )

def clean_kenya_folder():
    mop_folder(path=path)
