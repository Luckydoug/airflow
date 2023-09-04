from airflow.models import variable
from reports.conversion.data.fetch_data import (
    fetch_views_conversion,
    fetch_eyetests_conversion,
    fetch_registrations_conversion,
)

from sub_tasks.libraries.utils import (create_rwanda_engine, rwanda_path, fetch_gsheet_data)
from reports.draft_to_upload.utils.utils import (get_report_frequency)
from reports.conversion.reports.viewrx import (create_views_conversion)
from reports.conversion.utils.utils import (return_conversion_daterange)
from reports.conversion.reports.eyetests import (create_eyetests_conversion)
from reports.conversion.reports.registrations import (create_registrations_conversion)
from reports.conversion.smtp.smtp import (
    send_management_report, 
    send_branches_report,
    clean_registrations,
    clean_eyetests,
    clean_views
)

#This is the Basic Configuration of the report
#database variable is the name of the database
#engine is returned from utils module. You can change
#this function to accept just the schema and the password 
#so it can create the engine for any database
#selection can be either weekly or Monthly for this report.
#Automatic selection is not yet implemented,
database = "voler_mviews"
engine = create_rwanda_engine()
selection = get_report_frequency()
start_date, end_date = return_conversion_daterange(selection=selection)

views_conv = fetch_views_conversion(
    #This function fetches the views conversion
    #This fuction is designed to accept any database as long as it exist 
    #and the view for the conversion also exists.
    database=database,
    engine=engine,
    start_date=start_date,
    end_date=end_date,
    users="voler_staging",
    users_table="source_users",
    view = "v_view_rx_conv"
)

eyetests_conv = fetch_eyetests_conversion(
    database=database,
    engine=engine,
    start_date=start_date,
    end_date=end_date,
    users="voler_staging",
    users_table="source_users"
)

registrations_conv = fetch_registrations_conversion(
    database=database,
    engine=engine,
    start_date=start_date,
    end_date=end_date,
    users="voler_staging",
    users_table="source_users",
    view="v_reg_conv"
)

def build_rwanda_et_conversion():
    create_eyetests_conversion(
        eyetests_conv,
        country="Rwanda",
        selection=selection,
        path=f"{rwanda_path}"
    )

def build_rwanda_reg_conversion():
    create_registrations_conversion(
        registrations_conv,
        country="Rwanda",
        path = f"{rwanda_path}",
        selection=selection
    )


def build_rwanda_viewrx_conversion():
    create_views_conversion(
        views_conv,
        country="Rwanda",
        selection=selection,
        path=f"{rwanda_path}"
    )

def trigger_rwanda_management_smtp():
    send_management_report(
        path=rwanda_path,
        country="Rwanda",
        selection=selection
    )

def trigger_rwanda_branches_smtp():
    branch_data = fetch_gsheet_data()["rw_srm_rm"]
    send_branches_report(
        path=rwanda_path,
        branch_data=branch_data,
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


