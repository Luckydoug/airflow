import pandas as pd
from airflow.models import variable
from sub_tasks.libraries.utils import (
    path,
    createe_engine,
    fourth_week_start,
    first_week_start,
    fetch_gsheet_data,
    target
)

from reports.draft_to_upload.smtp.smtp import (
    send_draft_upload_report,
    clean_folders,
    send_to_branches
)

from reports.draft_to_upload.reports.draft import (
    create_draft_upload_report
)
from reports.draft_to_upload.utils.utils import (
    get_report_frequency,
    return_report_daterange,
    get_start_end_dates
)

from reports.draft_to_upload.reports.rejections import create_rejection_report
from reports.draft_to_upload.reports.plano import (
    create_plano_report
)

from reports.draft_to_upload.reports.opening_time import (
    create_opening_time_report
)

from reports.insurance_conversion.data.fetch_data import FetchData
from reports.insurance_conversion.reports.conversion import create_insurance_conversion
from reports.draft_to_upload.reports.sops import create_ug_sops_report
from reports.draft_to_upload.reports.ratings import create_ratings_report
from reports.draft_to_upload.data.push_data import push_insurance_efficiency_data
from reports.draft_to_upload.reports.no_view_no_conv import create_non_conversions_non_view
from reports.draft_to_upload.reports.mtd_insurance import create_mtd_insurance_conversion
from reports.draft_to_upload.data.fetch_data import (
    fetch_views,
    fetch_orders,
    fetch_eyetests,
    fetch_salesorders,
    fetch_payments,
    fetch_orderscreen,
    fetch_sops_branch_info,
    fetch_insurance_companies,
    fetch_planos,
    fetch_planorderscreen,
    fetch_registrations,
    fetch_detractors,
    fetch_opening_time,
    fetch_insurance_efficiency,
    fetch_mtd_efficiency,
    fetch_daywise_efficiency,
    fetch_daywise_rejections,
    fetch_mtd_rejections,
    fetch_customers,
    fetch_branch_data,
    fetch_non_conversion_non_viewed,
    fetch_working_hours,
    fetch_no_views_data
)


database = "mabawa_staging"
engine = createe_engine()
selection = get_report_frequency()
start_date = return_report_daterange(
    selection=selection
)
start_date = pd.to_datetime(start_date, format="%Y-%m-%d").date()

"""
Assigning variables to fuctions that are fetching data
may have a significant impact of the dag importation and overall performance
To solve this, we create functions with name of the data we are fetching 
so that the data is only fetched when it is needed!.
"""

"""
RETURN ORDERS
"""


def return_orders():
    orders = fetch_orders(
        database=database,
        engine=engine
    )
    return orders


"""
RETURN EYE TESTS
"""


def eyetests():
    eyetests = fetch_eyetests(
        database=database,
        engine=engine
    )

    return eyetests


"""
RETURN ORDERSCREEN LOGS
"""


def orderscreen():
    orderscreen = fetch_orderscreen(
        database=database,
        engine=engine
    )

    return orderscreen


def all_orders():
    all_views = fetch_views(
        database=database,
        engine=engine
    )

    all_orders = fetch_orders(
        database=database,
        engine=engine
    )

    insurance_companies = fetch_insurance_companies(
        database=database,
        engine=engine
    )

    all_orders = pd.merge(
        all_orders,
        all_views[["Code", "Last View Date"]],
        on="Code",
        how="left"
    )

    all_orders = pd.merge(
        all_orders,
        insurance_companies[[
            "DocNum", "Insurance Company",
            "Scheme Type", "Insurance Scheme",
            "Feedback 1", "Feedback 2"
        ]],
        on="DocNum",
        how="left"
    )

    return all_orders


def sales_orders():
    sales_orders = fetch_salesorders(
        database=database,
        engine=engine
    )

    return sales_orders


def payments():
    payments = fetch_payments(
        database=database,
        engine=engine,
        start_date='2019-01-01'
    )
    return payments


def planos():
    planos = fetch_planos(
        engine=engine,
        database=database,
        schema="mabawa_dw",
        customers="dim_customers",
        users="dim_users",
        table="et_conv",
        views="mabawa_mviews",
        start_date=str(start_date)
    )

    return planos


def plano_orderscreen():
    plano_orderscreen = fetch_planorderscreen(
        database=database,
        engine=engine,
        start_date=str(start_date)
    )

    return plano_orderscreen


def sops_info():
    sops_info = fetch_sops_branch_info(engine=engine)
    return sops_info


def registrations():
    registrations = fetch_registrations(
        engine=engine,
        database="mabawa_dw",
        table="dim_customers",
        table2="dim_users",
        start_date='2019-01-01'
    )

    return registrations


def surveys():
    surveys = fetch_detractors(
        database=database,
        engine=engine
    )

    return surveys


def branch_data():
    branch_data = fetch_branch_data(
        engine=engine,
        database='reports_tables'
    )

    return branch_data


date = ''
if selection == "Daily":
    date = str(start_date)
if selection == "Weekly":
    date = fourth_week_start
if selection == "Monthly":
    date = '2023-11-01'

pstart_date, pend_date = get_start_end_dates(
    selection=selection
)


def opening_data():
    opening_data = fetch_opening_time(
        database=database,
        engine=engine,
        start_date=date
    )

    return opening_data


def working_hours() -> pd.DataFrame:
    working_hours = fetch_working_hours(
        engine=engine
    )
    return working_hours


def push_kenya_efficiency_data():
    date = return_report_daterange(selection="Daily")
    date = pd.to_datetime(date, format="%Y-%m-%d").date()
    push_insurance_efficiency_data(
        engine=engine,
        orderscreen=orderscreen(),
        all_orders=all_orders(),
        start_date=date,
        branch_data=branch_data(),
        working_hours=working_hours(),
        database=database
    )


def data_orders():
    data_orders = fetch_insurance_efficiency(
        database=database,
        engine=engine,
        start_date=start_date,
        dw="mabawa_dw"
    )
    return data_orders


def daywise_efficiency():
    daywise_efficiency = fetch_daywise_efficiency(
        database=database,
        engine=engine,
        start_date=pstart_date,
        end_date=pend_date,
        dw="mabawa_dw"
    )

    return daywise_efficiency


def mtd_efficiency():
    mtd_efficiency = fetch_mtd_efficiency(
        database=database,
        engine=engine,
        start_date=pstart_date,
        end_date=pend_date,
        dw="mabawa_dw"
    )

    return mtd_efficiency


def no_view_non_conversions():
    no_view_non_conversions = fetch_non_conversion_non_viewed(
        database="mabawa_mviews",
        engine=engine,
        start_date=start_date
    )

    return no_view_non_conversions


def build_kenya_draft_upload():
    create_draft_upload_report(
        data_orders=data_orders(),
        mtd_data=mtd_efficiency(),
        daywise_data=daywise_efficiency(),
        selection=selection,
        start_date=start_date,
        target=target,
        branch_data=branch_data(),
        path=path,
        drop="KENYA PIPELINE COMPANY"
    )


def daywise_rejections():
    daywise_rejections = fetch_daywise_rejections(
        database=database,
        view="mabawa_mviews",
        engine=engine,
        start_date=pstart_date,
        end_date=pend_date
    )

    return daywise_rejections


def mtd_rejections():
    mtd_rejections = fetch_mtd_rejections(
        database=database,
        view="mabawa_mviews",
        engine=engine,
        start_date=pstart_date,
        end_date=pend_date
    )

    return mtd_rejections


def build_kenya_rejections():
    to_drop = fetch_gsheet_data()["rejections_drop"]
    create_rejection_report(
        orderscreen=orderscreen(),
        all_orders=all_orders(),
        branch_data=branch_data(),
        path=path,
        selection=selection,
        start_date=start_date,
        sales_orders=sales_orders(),
        mtd_data=mtd_rejections(),
        daywise_data=daywise_rejections(),
        drop=to_drop
    )


def customers():
    customers = fetch_customers(
        database="mabawa_mviews",
        engine=engine,
        start_date=start_date
    )

    return customers


def build_kenya_sops():
    create_ug_sops_report(
        selection=selection,
        branch_data=branch_data(),
        sops_info=sops_info(),
        start_date=start_date,
        path=path,
        customers=customers()
    )


def build_kenya_plano_report():
    create_plano_report(
        branch_data=branch_data(),
        path=path,
        registrations=registrations(),
        payments=payments(),
        all_planos=planos(),
        plano_orderscreen=plano_orderscreen(),
        all_orders=all_orders(),
        selection=selection
    )


def build_kenya_ratings_report():
    create_ratings_report(
        selection=selection,
        surveys=surveys(),
        branch_data=branch_data(),
        path=path
    )


def build_kenya_opening_time():
    create_opening_time_report(
        opening_data(),
        path
    )


data_fetcher = FetchData(
    engine=engine,
    database="mabawa_staging"
)


def orderscreen():
    orderscreen = data_fetcher.fetch_orderscreen(
        start_date=start_date
    )

    return orderscreen


def insurance_companies():
    insurance_companies = data_fetcher.fetch_insurance_companies()
    return insurance_companies


def orders():
    orders = data_fetcher.fetch_orders()
    return orders


def sales_orders():
    sales_orders = data_fetcher.fetch_sales_orders(
        start_date=first_week_start
    )

    return sales_orders


def no_feedbacks():
    no_feedbacks = data_fetcher.fetch_no_feedbacks(
        database="report_views",
        start_date=start_date
    )

    return no_feedbacks


def build_kenya_insurance_conversion() -> None:
    if selection == "Monthly":
        return
    create_insurance_conversion(
        path=path,
        all_orders=orders(),
        orderscreen=orderscreen(),
        branch_data=branch_data(),
        sales_orders=sales_orders(),
        insurance_companies=insurance_companies(),
        selection=selection,
        date=start_date,
        working_hours=working_hours(),
        country="Kenya",
        no_feedbacks=no_feedbacks()
    )

"""
FETCH ORDERSCREEN LOGS
"""
def mtd_orderscreen():
    orderscreen = data_fetcher.fetch_orderscreen(
        start_date=str(pstart_date)
    )

    return orderscreen


"""
CREATE INSURANCE CONVERSION REPORT 
"""
def build_mtd_insurance_conversion() -> None:
    create_mtd_insurance_conversion(
        path=path,
        all_orders=orders(),
        orderscreen=mtd_orderscreen(),
        branch_data=branch_data(),
        sales_orders=sales_orders(),
        insurance_companies=insurance_companies(),
        date = pstart_date,
        selection="Monthly"
    )


def no_views_data() -> pd.DataFrame:
    no_views_data = fetch_no_views_data(
        database="mabawa_mviews",
        engine=engine,
        start_date=start_date
    )

    return no_views_data


def build_kenya_non_view_non_conversions():
    create_non_conversions_non_view(
        path=path,
        data=no_view_non_conversions(),
        selection=selection,
        start_date=fourth_week_start,
        no_views_data=no_views_data()
    )


"""
LOWER LEVEL CODE
"""

def trigger_kenya_smtp():
    send_draft_upload_report(
        selection=selection,
        path=path,
        country="Kenya",
        target=target
    )


def trigger_kenya_branches_smtp():
    send_to_branches(
        branch_data=branch_data(),
        selection=selection,
        path=path,
        country="Kenya",
        filename=f"{path}draft_upload/log.txt"
    )


def clean_kenya_folder():
    clean_folders(path=path)


"""
DATA TEAM DEVELOPMENTS
IT SHOULD BE UNIQUE THOUGH

SIGNED BY
DOUGLAS KATHURIMA
"""

