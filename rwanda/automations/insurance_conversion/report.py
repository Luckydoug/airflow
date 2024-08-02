from airflow.models import variable
import pandas as pd
from reports.insurance_conversion.reports.conversion import create_insurance_conversion
from reports.insurance_conversion.data.fetch_data import FetchData
from reports.conversion.utils.utils import get_conversion_frequency
from reports.draft_to_upload.utils.utils import return_report_daterange
from reports.draft_to_upload.data.fetch_data import fetch_client_contacted_time
from reports.insurance_conversion.smtp.smtp import (
    send_to_management,
    mop_folder,
    send_to_branches,
)
from sub_tasks.libraries.utils import (
    create_rwanda_engine,
    fetch_gsheet_data,
    rwanda_path,
    assert_integrity,
)


engine = create_rwanda_engine()
selection = get_conversion_frequency(report="Insurance Conversion")
start_date = return_report_daterange(selection=selection)
data_fetcher = FetchData(engine=engine, database="voler_staging")


def orderscreen():
    orderscreen = data_fetcher.fetch_orderscreen(start_date="2023-01-01")

    return orderscreen


def insurance_companies():
    insurance_companies = data_fetcher.fetch_insurance_companies()

    return insurance_companies


def orders():
    orders = data_fetcher.fetch_orders()

    return orders


def sales_orders():
    sales_orders = data_fetcher.fetch_sales_orders(start_date=start_date)

    return sales_orders


def branch_data():
    branch_data = data_fetcher.fetch_branch_data(database="reports_tables")

    return branch_data


def no_feedbacks():
    no_feedbacks = data_fetcher.fetch_no_feedbacks(
        database="report_views", start_date=start_date
    )

    return no_feedbacks


def contact_time() -> pd.DataFrame:
    contact_time = fetch_client_contacted_time(
        start_date=start_date, engine=engine, view="voler_mviews"
    )

    return contact_time


def holidays() -> pd.DataFrame:
    return data_fetcher.fetch_holidays(dw="voler_dw")


def build_rwanda_insurance_conversion() -> None:
    working_hours = fetch_gsheet_data()["rw_working_hours"]
    create_insurance_conversion(
        path=rwanda_path,
        working_hours=working_hours,
        date=start_date,
        selection=selection,
        all_orders=orders(),
        orderscreen=orderscreen(),
        branch_data=branch_data(),
        sales_orders=sales_orders(),
        insurance_companies=insurance_companies(),
        no_feedbacks=no_feedbacks(),
        contact_time=contact_time(),
        holidays=holidays(),
        country="Rwanda",
    )


def send_to_rwanda_management() -> None:
    # if not assert_integrity(engine=engine,database="voler_staging"):
    #     print("We run into an error. Ensure all the tables are updated in data warehouse and try again.")
    #     return

    send_to_management(selection=selection, country="Rwanda", path=rwanda_path)


def send_to_rwanda_branches() -> None:
    if not assert_integrity(engine=engine, database="voler_staging"):
        print(
            "We run into an error. Ensure all the tables are updated in data warehouse and try again."
        )
        return

    send_to_branches(
        path=rwanda_path,
        branch_data=branch_data(),
        country="Rwanda",
        filename=f"{rwanda_path}insurance_conversion/branch_log.txt",
    )


def clean_rwanda_folder() -> None:
    mop_folder(path=rwanda_path)
