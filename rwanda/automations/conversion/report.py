import pandas as pd
from airflow.models import variable
from reports.conversion.data.fetch_data import (
    fetch_views_conversion,
    fetch_eyetests_conversion,
    fetch_registrations_conversion,
    fetch_branch_data
)

from sub_tasks.libraries.utils import create_rwanda_engine, rwanda_path, assert_integrity
from reports.conversion.utils.utils import get_conversion_frequency
from reports.conversion.reports.viewrx import create_views_conversion
from reports.conversion.utils.utils import return_conversion_daterange
from reports.conversion.reports.eyetests import create_eyetests_conversion
from reports.conversion.reports.registrations import create_registrations_conversion
from reports.conversion.smtp.smtp import (
    send_management_report, 
    send_branches_report,
    clean_registrations,
    clean_eyetests,
    clean_views
)


database = "voler_mviews"
engine = create_rwanda_engine()
selection = get_conversion_frequency(
    report="Conversion"
)
selection = "Monthly"


start_date, end_date = return_conversion_daterange(selection=selection)

def views_conv() -> pd.DataFrame:
    views_conv = fetch_views_conversion(
        database=database,
        engine=engine,
        start_date=start_date,
        end_date=end_date,
        users="voler_staging",
        users_table="source_users",
        view = "v_view_rx_conv"
    )

    return views_conv

def eyetests_conv() -> pd.DataFrame:
    eyetests_conv = fetch_eyetests_conversion(
        engine=engine,
        start_date=start_date,
        end_date=end_date,
        users="voler_staging",
        users_table="source_users"
    )

    return eyetests_conv


def registrations_conv() -> pd.DataFrame:
    registrations_conv = fetch_registrations_conversion(
        database=database,
        engine=engine,
        start_date=start_date,
        end_date=end_date,
        users="voler_staging",
        users_table="source_users",
        view="v_reg_conv"
    )

    return registrations_conv


def branch_data() -> pd.DataFrame:
    branch_data = fetch_branch_data(
        engine=engine,
        database="reports_tables"
    )

    return branch_data

def build_rwanda_et_conversion():
    create_eyetests_conversion(
        eyetests_conv(),
        country="Rwanda",
        selection=selection,
        path=f"{rwanda_path}"
    )

def build_rwanda_reg_conversion():
    create_registrations_conversion(
        registrations_conv(),
        country="Rwanda",
        path = f"{rwanda_path}",
        selection=selection
    )


def build_rwanda_viewrx_conversion():
    create_views_conversion(
        views_conv(),
        country="Rwanda",
        selection=selection,
        path=f"{rwanda_path}"
    )

def trigger_rwanda_management_smtp():
    if not assert_integrity(engine=engine,database="voler_staging"):
        print("We run into an error. Ensure all the tables are updated in data warehouse and try again.")
        return
    send_management_report(
        path=rwanda_path,
        country="Test", #Change to "Test" if you want to test the email first.
        selection=selection
    )

def trigger_rwanda_branches_smtp():
    if not assert_integrity(engine=engine,database="voler_staging"):
        print("We run into an error. Ensure all the tables are updated in data warehouse and try again.")
        return
    send_branches_report(
        path=rwanda_path,
        branch_data=branch_data(),
        selection=selection
    )

def clean_rwanda_registrations():
    clean_registrations(path=rwanda_path)

def clean_rwanda_eyetests():
    clean_eyetests(path=rwanda_path)

def clean_rwanda_views():
    clean_views(path=rwanda_path)


"""
Rwanda Conversion Report Airflow Automation
Optica Data Team
Let's keep it flowing

"""


# Written and Curated by Douglas
# We shall remember.


