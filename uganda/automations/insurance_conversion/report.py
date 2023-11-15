from airflow.models import variable
import pandas as pd
from reports.insurance_conversion.reports.conversion import create_insurance_conversion
from reports.insurance_conversion.data.fetch_data import FetchData
from reports.insurance_conversion.smtp.smtp import send_to_management, mop_folder, send_to_branches
from sub_tasks.libraries.utils import (
    create_unganda_engine,
    uganda_path
)
from reports.draft_to_upload.utils.utils import (return_report_daterange)
from reports.conversion.utils.utils import (get_conversion_frequency)

engine = create_unganda_engine()
selection = get_conversion_frequency(
    report="Insurance Conversion"
)
start_date = return_report_daterange(selection=selection)
data_fetcher = FetchData(
    engine=engine,
    database="mawingu_staging"
)

"""
FETCH ORDERSCREEN LOGS
"""


def orderscreen() -> pd.DataFrame:
    orderscreen = data_fetcher.fetch_orderscreen(
        start_date='2023-01-01'
    )

    return orderscreen


"""
FETCH INSURANCE COMPANIES
"""


def insurance_companies() -> pd.DataFrame:
    insurance_companies = data_fetcher.fetch_insurance_companies()

    return insurance_companies


"""
FETCH ALL ORDERS MADE OVER DATE RANGE
"""


def orders() -> pd.DataFrame:
    orders = data_fetcher.fetch_orders()

    return orders


"""
FETCH SALES ORDERS
"""


def sales_orders() -> pd.DataFrame:
    sales_orders = data_fetcher.fetch_sales_orders(
        start_date=start_date
    )

    return sales_orders


"FETCH BRANCH DATA"


def branch_data() -> pd.DataFrame:
    branch_data = data_fetcher.fetch_branch_data(database="reports_tables")

    return branch_data


"""
FETCH INSURANCE ORDERS THAT HAVE NOT RECEIVED A FEEDBACK FROM THE INSURANCE COMPANY.
"""


def no_feedbacks() -> pd.DataFrame:
    no_feedbacks = data_fetcher.fetch_no_feedbacks(
        database="report_views",
        start_date=start_date
    )

    return no_feedbacks


"""
FETCH UGANDA BRANCHES WORKING HOURS
"""


def working_hours() -> pd.DataFrame:
    working_hours = data_fetcher.fetch_working_hours()

    return working_hours


"""
BUILD INSURANCE CONVERSION REPORT
"""


def build_uganda_insurance_conversion() -> None:
    create_insurance_conversion(
        path=uganda_path,
        working_hours=working_hours(),
        date=start_date,
        selection=selection,
        all_orders=orders(),
        orderscreen=orderscreen(),
        branch_data=branch_data(),
        sales_orders=sales_orders(),
        insurance_companies=insurance_companies(),
        no_feedbacks=no_feedbacks(),
        country="Uganda"
    )


"""
SEND REPORT TO THE MANAGEMENT
"""


def send_to_uganda_management() -> None:
    send_to_management(
        selection=selection,
        country="Uganda",
        path=uganda_path
    )


"""
SEND THE REPORT TO UGANDA MANAGEMENT
"""


def send_to_uganda_branches() -> None:
    send_to_branches(
        path=uganda_path,
        branch_data=branch_data(),
        country="Uganda",
        filename=f"{uganda_path}insurance_conversion/branch_log.txt"

    )


"""
REMOVE ALL THE .xlsx FILES AFTER THE ABOVE SENDING IS DONE.
"""


def clean_uganda_folder() -> None:
    mop_folder(path=uganda_path)
