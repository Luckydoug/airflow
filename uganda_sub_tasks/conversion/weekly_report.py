from airflow.models import variable
from uganda_sub_tasks.library.fetch_data import (
    fetch_views_conversion,
    fetch_eyetests_conversion,
    fetch_registrations_conversion,
)
from uganda_sub_tasks.conversion.reports.viewrx_conversion import (
    create_views_conversion
)
from uganda_sub_tasks.conversion.reports.eyetests_conversion import (
    create_eyetests_conversion
)
from uganda_sub_tasks.conversion.reports.registrations_conversion import (
    create_registrations_conversion
)


eyetests_conv = fetch_eyetests_conversion()
registrations_conv = fetch_registrations_conversion()
views_conv = fetch_views_conversion()


def build_uganda_et_conversion():
    create_eyetests_conversion(eyetests_conv)


def build_uganda_reg_conversion():
    create_registrations_conversion(registrations_conv)


def build_uganda_viewrx_conversion():
    create_views_conversion(views_conv)


"""
Uganda Conversion Report Airflow Automation
Optica Data Team
Let's keep it flowing

"""
