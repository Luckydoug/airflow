from airflow.models import variable
from reports.insurance_conversion.reports.conversion import create_insurance_conversion
from reports.insurance_conversion.data.fetch_data import FetchData
from reports.draft_to_upload.utils.utils import get_report_frequency
from reports.insurance_conversion.smtp.smtp import send_to_management, mop_folder, send_to_branches
from sub_tasks.libraries.utils import (
    first_week_start, 
    create_unganda_engine,
    fetch_gsheet_data,
    uganda_path
)
from reports.draft_to_upload.utils.utils import (return_report_daterange)

engine = create_unganda_engine()
selection = "Weekly"
start_date = return_report_daterange(selection = selection)
data_fetcher = FetchData(
    engine=engine,
    database="mawingu_staging"
)

orderscreen = data_fetcher.fetch_orderscreen(
    start_date='2023-01-01'
)
insurance_companies = data_fetcher.fetch_insurance_companies()
orders = data_fetcher.fetch_orders()
sales_orders = data_fetcher.fetch_sales_orders(
    start_date=start_date
)
branch_data = data_fetcher.fetch_branch_data(database="reports_tables")
no_feedbacks = data_fetcher.fetch_no_feedbacks(
    database="report_views",
    start_date=start_date
)


def build_uganda_insurance_conversion() -> None:
    branch_data = fetch_gsheet_data()["ug_srm_rm"]
    working_hours = fetch_gsheet_data()["ug_working_hours"]
    create_insurance_conversion(
        path=uganda_path,
        working_hours=working_hours,
        date = start_date,
        selection=selection,
        all_orders=orders,
        orderscreen=orderscreen,
        branch_data=branch_data,
        sales_orders=sales_orders,
        insurance_companies=insurance_companies,
        no_feedbacks=no_feedbacks,
        country="Uganda"
    )

def send_to_uganda_management() -> None:
    send_to_management(
        selection=selection,
        country = "Uganda",
        path=uganda_path
    )

def send_to_uganda_branches() -> None:
    branch_data = fetch_gsheet_data()["ug_srm_rm"]
    send_to_branches(
        path=uganda_path,
        branch_data=branch_data,
        country="Uganda",
        filename=f"{uganda_path}insurance_conversion/branch_log.txt"
        
    )

def clean_uganda_folder() -> None:
    mop_folder(path=uganda_path)








