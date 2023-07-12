from airflow.models import variable
from reports.conversion.data.fetch_data import (
    fetch_views_conversion,
    fetch_eyetests_conversion,
    fetch_registrations_conversion,
)

from sub_tasks.libraries.utils import (create_unganda_engine, uganda_path, fetch_gsheet_data)
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


database = "mawingu_mviews"
engine = create_unganda_engine()
selection = get_report_frequency()
start_date, end_date = return_conversion_daterange(selection=selection)

views_conv = fetch_views_conversion(
    database=database,
    engine=engine,
    start_date=start_date,
    end_date=end_date,
    users="mawingu_staging",
    users_table="source_users",
    view = "v_view_rx_conv"
)

eyetests_conv = fetch_eyetests_conversion(
    database=database,
    engine=engine,
    start_date=start_date,
    end_date=end_date,
    users="mawingu_staging",
    users_table="source_users"
)

registrations_conv = fetch_registrations_conversion(
    database=database,
    engine=engine,
    start_date=start_date,
    end_date=end_date,
    users="mawingu_staging",
    users_table="source_users",
    view="v_reg_conv"
)

def build_uganda_et_conversion():
    create_eyetests_conversion(
        eyetests_conv,
        country="Uganda",
        selection=selection,
        path=f"{uganda_path}"
    )

def build_uganda_reg_conversion():
    create_registrations_conversion(
        registrations_conv,
        country="Uganda",
        path = f"{uganda_path}",
        selection=selection
    )


def build_uganda_viewrx_conversion():
    create_views_conversion(
        views_conv,
        country="Uganda",
        selection=selection,
        path=f"{uganda_path}"
    )

def trigger_uganda_management_smtp():
    send_management_report(
        path=uganda_path,
        country="Uganda",
        selection=selection
    )

def trigger_uganda_branches_smtp():
    branch_data = fetch_gsheet_data()["ug_srm_rm"]
    send_branches_report(
        path=uganda_path,
        branch_data=branch_data,
        selection=selection
    )

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

