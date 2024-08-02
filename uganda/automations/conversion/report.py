from airflow.models import variable
import pandas as pd
from reports.conversion.data.fetch_data import (
    fetch_views_conversion,
    fetch_eyetests_conversion,
    fetch_registrations_conversion,
    fetch_branch_data
)

from sub_tasks.libraries.utils import (create_unganda_engine, uganda_path, assert_integrity)
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

"""
This is the Basic Configuration of the report
database variable is the name of the database
engine is returned from utils module. You can change
this function to accept just the schema and the password 
so it can create the engine for any database
selection can be either weekly or Monthly for this report.
Automatic selection is not yet implemented,
"""

database = "mawingu_mviews"
engine = create_unganda_engine()
selection = (
    get_conversion_frequency(report="Conversion")
)

start_date, end_date = return_conversion_daterange(selection=selection)

"""
For modularity we import the fetch data functions from the fetch_data
file located in reports/conversion/data/fetch_data.py
We then define functions with the name of the data we are expecting.
Change this files if only necessary.
"""

"""
Fetch branch data
"""


def branch_data() -> pd.DataFrame:
    branch_data = fetch_branch_data(
        engine=engine,
        database="reports_tables"
    )

    return branch_data


"""
Fetch view rx conversion
"""


def views_conv() -> pd.DataFrame:
    views_conv = fetch_views_conversion(
        database=database,
        engine=engine,
        start_date=start_date,
        end_date=end_date,
        users="mawingu_staging",
        users_table="source_users",
        view="v_view_rx_conv"
    )

    return views_conv


"""
Fetch eye tests conversion
"""


def eyetests_conv() -> pd.DataFrame:
    eyetests_conv = fetch_eyetests_conversion(
        engine=engine,
        start_date=start_date,
        end_date=end_date,
        users="mawingu_staging",
        users_table="source_users"
    )

    return eyetests_conv


"""
Fetch registrations conversion.
"""


def registrations_conv() -> pd.DataFrame:
    registrations_conv = fetch_registrations_conversion(
        database=database,
        engine=engine,
        start_date=start_date,
        end_date=end_date,
        users="mawingu_staging",
        users_table="source_users",
        view="v_reg_conv"
    )

    return registrations_conv


"""
Call designated functions to build the reports.
"""


def build_uganda_et_conversion():
    create_eyetests_conversion(
        eyetests_conv(),
        country="Uganda",
        selection=selection,
        path=f"{uganda_path}"
    )


def build_uganda_reg_conversion():
    create_registrations_conversion(
        registrations_conv(),
        country="Uganda",
        path=f"{uganda_path}",
        selection=selection
    )


def build_uganda_viewrx_conversion():
    create_views_conversion(
        views_conv(),
        country="Uganda",
        selection=selection,
        path=f"{uganda_path}"
    )


"""
Call the SMTP Functions for management and the branches.
If you want to test the management report
Pass "Test" as an argument to the country parameter
"""


def trigger_uganda_management_smtp():
    if not assert_integrity(engine=engine,database="mawingu_staging"):
        print("We run into an error. Ensure all the tables are updated in data warehouse and try again.")
        return
    
    """
    At this point we check the intergrity of the report before sending it.
    """
    
    send_management_report(
        path=uganda_path,
        country="Uganda",
        selection=selection
    )


def trigger_uganda_branches_smtp():
    if not assert_integrity(engine=engine,database="mawingu_staging"):
        print("We run into an error. Ensure all the tables are updated in data warehouse and try again.")
        return
    
    send_branches_report(
        path=uganda_path,
        branch_data=branch_data(),
        selection=selection
    )


"""
Remove all the .xlsx files after sending the reports.
"""


def clean_uganda_registrations():
    clean_registrations(path=uganda_path)


def clean_uganda_eyetests():
    clean_eyetests(path=uganda_path)

def clean_uganda_views():
    clean_views(path=uganda_path)


"""
Uganda Conversion Report Airflow Automation
Optica Data Team
Let's keep it flowing

"""

