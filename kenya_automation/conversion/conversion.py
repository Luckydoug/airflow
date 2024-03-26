from airflow.models import variable
from reports.conversion.data.fetch_data import (
    fetch_views_conversion,
    fetch_eyetests_conversion,
    fetch_registrations_conversion,
    fetch_branch_data,
    fetch_submitted_insurance
)

from sub_tasks.libraries.utils import (createe_engine, path, assert_integrity)
from reports.conversion.reports.viewrx import (create_views_conversion)
from reports.conversion.utils.utils import (return_conversion_daterange)
from reports.conversion.utils.utils import (get_conversion_frequency)
from reports.conversion.reports.eyetests import (create_eyetests_conversion)
from reports.conversion.reports.registrations import (create_registrations_conversion)
from reports.conversion.smtp.smtpcopy import (
    send_management_report, 
    send_branches_report,
    clean_registrations,
    clean_eyetests,
    clean_views
)

database = "mabawa_mviews"
engine = createe_engine()

selection = get_conversion_frequency(
    report="Conversion"
)

start_date, end_date = return_conversion_daterange(selection=selection)
def branch_data():
    branch_data = fetch_branch_data(
        engine=engine,
        database="reports_tables"
    )
    
    return branch_data


def views_conv():
    views_conv = fetch_views_conversion(
        database=database,
        engine=engine,
        start_date=start_date,
        end_date=end_date,
        users="mabawa_dw",
        users_table="dim_users",
        view = "view_rx_conv"
    )

    return views_conv


def eyetests_conv():
    eyetests_conv = fetch_eyetests_conversion(
        engine=engine,
        start_date=start_date,
        end_date=end_date,
        users="mabawa_dw",
        users_table="dim_users"
    )

    return eyetests_conv


def registrations_conv():
    registrations_conv = fetch_registrations_conversion(
        database=database,
        engine=engine,
        start_date=start_date,
        end_date=end_date,
        users="mabawa_dw",
        users_table="dim_users",
        view="reg_conv"
    )

    return registrations_conv

def submitted_insurance():
    submitted_insurance = fetch_submitted_insurance(
        engine=engine,
        start_date=start_date,
        end_date=end_date
    )

    return submitted_insurance


def build_kenya_et_conversion():
    create_eyetests_conversion(
        eyetests_conv(),
        country="Kenya",
        selection=selection,
        path=f"{path}",
        insurances = submitted_insurance()
    )


def build_kenya_reg_conversion():
    create_registrations_conversion(
        registrations_conv(),
        country="Kenya",
        path = f"{path}",
        selection=selection
    )


def build_kenya_viewrx_conversion():
    create_views_conversion(
        views_conv(),
        country="Kenya",
        selection=selection,
        path=f"{path}"
    )

def trigger_kenya_management_smtp():
    if not assert_integrity(engine=engine,database="mabawa_staging"):
        print("We run into an error. Ensure all the tables are updated in data warehouse and try again.")
        return
    send_management_report(
        path=path,
        country="Kenya",
        selection=selection
    )

def trigger_kenya_branches_smtp(): 
    if not assert_integrity(engine=engine,database="mabawa_staging"):
        print("We run into an error. Ensure all the tables are updated in data warehouse and try again.")
        return
    send_branches_report(
        path=path,
        branch_data=branch_data(),
        selection=selection
    )

def clean_kenya_registrations():
    clean_registrations(path=path)

def clean_kenya_eyetests():
    clean_eyetests(path=path)

def clean_kenya_views():
    clean_views(path=path)



"""
Kenya Conversion Report Airflow Automation
Optica Data Team
Let's keep it flowing

"""

