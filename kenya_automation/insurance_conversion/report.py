from airflow.models import variable
from reports.insurance_conversion.reports.conversion import create_insurance_conversion
from reports.insurance_conversion.data.fetch_data import FetchData
from reports.insurance_conversion.smtp.smtp import (
    send_to_management, 
    mop_folder, 
    send_to_branches
)
from sub_tasks.libraries.utils import (
    createe_engine,
    path
)
from reports.draft_to_upload.utils.utils import return_report_daterange
from reports.conversion.utils.utils import (get_conversion_frequency)
engine = createe_engine()
selection = get_conversion_frequency(
    report="Insurance Conversion"
)
start_date = return_report_daterange(selection=selection)

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
        start_date="2023-04-27"
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
        country="Kenya"
    )

"""
SEND REPORT TO THE MANAGEMENT
WHEN YOU WANT TO SEND A TEST EMAIL
PASS "Test" AS AN ARGUMENT TO THE country PARAMETER
"""
def send_to_kenya_management() -> None:
    send_to_management(
        selection=selection,
        country = "Kenya",
        path=path
    )

"""
SEND THE REPORT THE THE BRANCHES
"""

def send_to_kenya_branches() -> None:
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



