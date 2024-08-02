import pandas as pd
from airflow.models import variable
from reports.insurance_conversion.reports.conversion import create_insurance_conversion
from reports.insurance_conversion.data.fetch_data import FetchData
from sub_tasks.libraries.utils import assert_integrity
from reports.insurance_conversion.smtp.smtp import send_to_management 
from reports.insurance_conversion.smtp.smtp import mop_folder 
from reports.insurance_conversion.smtp.smtp import send_to_branches
from sub_tasks.libraries.utils import createe_engine
from sub_tasks.libraries.utils import path
from reports.draft_to_upload.utils.utils import return_report_daterange
from reports.conversion.utils.utils import get_conversion_frequency
from reports.draft_to_upload.data.fetch_data import fetch_eyetest_order
from reports.draft_to_upload.reports.et_to_order import eyetest_order_time
from reports.insurance_conversion.reports.rejections import create_rejections_report


engine = createe_engine()
selection = get_conversion_frequency(
    report="Insurance Conversion"
)
start_date = return_report_daterange(selection=selection)


selection = "Monthly"

"""
CREATE AN INSTANCE OF FetchData CLASS
"""
data_fetcher = FetchData(
    engine=engine,
    database="mabawa_staging"
)

"""
PLEASE NOTE THAT TO PREVENT PERIODIC EXECUTION OF CODE AND TO OPTMIZE THE CODE,
WE DEFINE FUNCTIONS FOR EVERY DATA WE FETCH
WITH THIS APPROACH WE REQUIRE THAT THE DATA BE FETCHED WHEN IT'S ONLY REQUIRED.
"""

"""
FETCH ORDERSCREEN LOGS
"""
def orderscreen():
    orderscreen = data_fetcher.fetch_orderscreen(
        start_date=str(start_date)
    )

    return orderscreen

"""
FETCH INSURANCE COMPANIES
"""
def insurance_companies():
    insurance_companies = data_fetcher.fetch_insurance_companies()

    return insurance_companies


"""
FETCH ORDERS MADE OVER DATE RANGE
"""
def orders():
    orders = data_fetcher.fetch_orders()
    return orders

"""
FETCH SALES ORDERS
"""
def sales_orders():
    sales_orders = data_fetcher.fetch_sales_orders(
        start_date="2024-01-01"
    )

    return sales_orders

"FETCH BRANCH, SRM, RM, AND BRANCH MAANGERS DATA"
def branch_data():
    branch_data = data_fetcher.fetch_branch_data("reports_tables")

    return branch_data


"""
FETCH NO FEEDBACKS
"""
def no_feedbacks():
    no_feedbacks = data_fetcher.fetch_no_feedbacks(
        database="report_views",
        start_date=start_date
    )

    return no_feedbacks


"""
FETCH WORKING HOURS
"""

def working_hours():
    working_hours = data_fetcher.fetch_working_hours()

    return working_hours

"""
CREATE INSURANCE CONVERSION REPORT 
"""


def holidays() -> pd.DataFrame:
    holidays = data_fetcher.fetch_holidays(dw="mabawa_dw")

    return holidays


def build_kenya_insurance_conversion() -> None:
    create_insurance_conversion(
        path=path,
        selection=selection,
        working_hours=working_hours(),
        all_orders=orders(),
        orderscreen=orderscreen(),
        branch_data=branch_data(),
        sales_orders=sales_orders(),
        insurance_companies=insurance_companies(),
        no_feedbacks=no_feedbacks(),
        date = start_date,
        country="Kenya",
        holidays=holidays()
    )


def eyetest_order():
    eyetest_order = fetch_eyetest_order(
        engine=engine,
        start_date=start_date
    )

    return eyetest_order

"""
BUILD EYE TEST ORDER REPORT
"""
def build_eyetest_order() -> None:
    eyetest_order_time(
        data=eyetest_order(),
        path=path,
        selection=selection
    )


def rejections() -> pd.DataFrame:
    rejections = data_fetcher.fetch_rejections(
        start_date=start_date
    )

    return rejections





def build_rejections() -> pd.DataFrame:
    create_rejections_report(
        data_rejections=rejections(),
        selection=selection,
        path=path
    )


"""
SEND REPORT TO THE MANAGEMENT
WHEN YOU WANT TO SEND A TEST EMAIL
PASS "Test" AS AN ARGUMENT TO THE country PARAMETER
"""
def send_to_kenya_management() -> None:
    # if not assert_integrity(engine=engine,database="mabawa_staging"):
    #     print("We run into an error. Ensure all the tables are updated in data warehouse and try again.")
    #     return
    send_to_management(
        selection=selection,
        country = "Test",
        path=path
    )

"""
SEND THE REPORT THE THE BRANCHES
"""

def send_to_kenya_branches() -> None:
    if not assert_integrity(engine=engine,database="mabawa_staging"):
        print("We run into an error. Ensure all the tables are updated in data warehouse and try again.")
        return
    send_to_branches(
        path=path,
        branch_data=branch_data(),
        country="Kenya",
        filename=f"{path}insurance_conversion/branch_log.txt"
        
    )

"""
REMOVE ALL THE .xlsx FILES AFTER SENDING THE REPORTS.
"""
def clean_kenya_folder():
    return
    mop_folder(path=path)


"""
DOUGLAS FROM
OPTICA DATA TEAM

PRINCIPLE USED: DRY(DON'T REPEAT YOURSELF)
CODE: REUSABILITY

EPHASIZING ON BEST PRACTICES TO
REDUCE LOAD ON THE SERVER
AS ACHIEVED BY OPTIMIZATION
"""


