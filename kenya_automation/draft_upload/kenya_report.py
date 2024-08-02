import pandas as pd
from airflow.models import variable
from sub_tasks.libraries.utils import path
from sub_tasks.libraries.utils import createe_engine
from sub_tasks.libraries.utils import fourth_week_start
from sub_tasks.libraries.utils import first_week_start
from sub_tasks.libraries.utils import target
from sub_tasks.libraries.utils import assert_integrity
from reports.draft_to_upload.smtp.smtp import send_draft_upload_report
from reports.draft_to_upload.smtp.smtp import clean_folders
from reports.draft_to_upload.smtp.smtp import send_to_branches
from reports.draft_to_upload.reports.draft import create_draft_upload_report
from reports.draft_to_upload.utils.utils import get_start_end_dates
from reports.draft_to_upload.utils.utils import get_report_frequency
from reports.draft_to_upload.utils.utils import return_report_daterange
from reports.draft_to_upload.reports.rejections import create_rejection_report
from reports.draft_to_upload.reports.plano import create_plano_report
from reports.draft_to_upload.reports.opening_time import create_opening_time_report
from reports.draft_to_upload.reports.et_to_order import eyetest_order_time
from reports.insurance_conversion.data.fetch_data import FetchData
from reports.insurance_conversion.reports.conversion import create_insurance_conversion
from reports.draft_to_upload.reports.sops import create_ug_sops_report
from reports.draft_to_upload.reports.ratings import create_ratings_report
from reports.draft_to_upload.data.push_data import push_insurance_efficiency_data
from reports.draft_to_upload.reports.no_view_no_conv import create_non_conversions_non_view
from reports.draft_to_upload.reports.mtd_insurance import create_mtd_insurance_conversion
from reports.draft_to_upload.data.fetch_data import fetch_views
from reports.draft_to_upload.data.fetch_data import fetch_orders
from reports.draft_to_upload.data.fetch_data import fetch_eyetests
from reports.draft_to_upload.data.fetch_data import fetch_salesorders
from reports.draft_to_upload.data.fetch_data import fetch_payments
from reports.draft_to_upload.data.fetch_data import fetch_orderscreen
from reports.draft_to_upload.data.fetch_data import fetch_sops_branch_info
from reports.draft_to_upload.data.fetch_data import fetch_insurance_companies
from reports.draft_to_upload.data.fetch_data import fetch_planos
from reports.draft_to_upload.data.fetch_data import fetch_planorderscreen
from reports.draft_to_upload.data.fetch_data import fetch_registrations
from reports.draft_to_upload.data.fetch_data import fetch_detractors
from reports.draft_to_upload.data.fetch_data import fetch_opening_time
from reports.draft_to_upload.data.fetch_data import fetch_insurance_efficiency
from reports.draft_to_upload.data.fetch_data import fetch_mtd_efficiency
from reports.draft_to_upload.data.fetch_data import fetch_daywise_efficiency
from reports.draft_to_upload.data.fetch_data import fetch_daywise_rejections
from reports.draft_to_upload.data.fetch_data import fetch_mtd_rejections
from reports.draft_to_upload.data.fetch_data import fetch_customers
from reports.draft_to_upload.data.fetch_data import fetch_branch_data
from reports.draft_to_upload.data.fetch_data import fetch_non_conversion_non_viewed
from reports.draft_to_upload.data.fetch_data import fetch_working_hours
from reports.draft_to_upload.data.fetch_data import fetch_no_views_data
from reports.draft_to_upload.data.fetch_data import fetch_rejections_drop
from reports.draft_to_upload.data.fetch_data import fetch_eyetest_order
from reports.draft_to_upload.data.fetch_data import fetch_eff_bef_feed_desk
from reports.draft_to_upload.data.fetch_data import fetch_eff_af_feed_nodesk
from reports.draft_to_upload.data.fetch_data import fetch_eff_af_feed_desk
from reports.draft_to_upload.data.fetch_data import fetch_eff_bef_feed_nodesk
from reports.draft_to_upload.data.fetch_data import fetch_client_contacted_time
from reports.draft_to_upload.data.fetch_data import efficiency_after_feedback
from reports.draft_to_upload.data.fetch_data import fetch_holidays
from reports.draft_to_upload.data.fetch_data import fetch_direct_insurance_conversion
from reports.draft_to_upload.reports.insurance_desk import efficiencyAfterFeedback
from reports.draft_to_upload.reports.insurance_desk import efficiencyBeforeFeedback
from reports.draft_to_upload.reports.upload_to_preauth import sap_update_efficieny
from reports.draft_to_upload.reports.direct_insurance import (
    createDirectInsuranceConversion,
)


database = "mabawa_staging"
engine = createe_engine()
selection = get_report_frequency()
selection = "Daily"
start_date = return_report_daterange(selection=selection)
start_date = pd.to_datetime(start_date, format="%Y-%m-%d").date()


"""
Assigning variables to fuctions that are fetching data
may have a significant impact on the dag importation and overall performance
To solve this, we create functions with name of the data we are fetching 
so that the data is only fetched when it is needed!.
"""

"""
RETURN ORDERS
"""


def return_orders():
    orders = fetch_orders(database=database, engine=engine)
    return orders


"""
RETURN EYE TESTS
"""


def eyetests():
    eyetests = fetch_eyetests(database=database, engine=engine)

    return eyetests


"""
RETURN ORDERSCREEN LOGS
"""


def orderscreen():
    orderscreen = fetch_orderscreen(database=database, engine=engine)

    return orderscreen


def all_orders():
    all_views = fetch_views(database=database, engine=engine)

    all_orders = fetch_orders(database=database, engine=engine)

    insurance_companies = fetch_insurance_companies(database=database, engine=engine)

    all_orders = pd.merge(
        all_orders, all_views[["Code", "Last View Date"]], on="Code", how="left"
    )

    all_orders = pd.merge(
        all_orders,
        insurance_companies[
            [
                "DocNum",
                "Insurance Company",
                "Scheme Type",
                "Insurance Scheme",
                "Feedback 1",
                "Feedback 2",
            ]
        ],
        on="DocNum",
        how="left",
    )

    return all_orders


def sales_orders():
    sales_orders = fetch_salesorders(database=database, engine=engine)

    return sales_orders


def payments():
    payments = fetch_payments(database=database, engine=engine, start_date="2019-01-01")
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
        start_date=str(start_date),
    )

    return planos


def plano_orderscreen():
    plano_orderscreen = fetch_planorderscreen(
        database=database, engine=engine, start_date=str(start_date)
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
        start_date="2019-01-01",
    )

    return registrations


def surveys():
    surveys = fetch_detractors(
        database="mabawa_mviews", engine=engine, table="nps_surveys"
    )

    return surveys


def branch_data():
    branch_data = fetch_branch_data(engine=engine, database="reports_tables")

    return branch_data


date = ""
if selection == "Daily":
    date = str(start_date)
if selection == "Weekly":
    date = fourth_week_start
if selection == "Monthly":
    date = "2024-02-01"

pstart_date, pend_date = get_start_end_dates(selection=selection)


def opening_data():
    opening_data = fetch_opening_time(database=database, engine=engine, start_date=date)

    return opening_data


def working_hours() -> pd.DataFrame:
    working_hours = fetch_working_hours(engine=engine)
    return working_hours


def holidays() -> pd.DataFrame:
    holidays = fetch_holidays(engine=engine, dw="mabawa_dw")
    return holidays


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
        database=database,
        holidays=holidays(),
    )


def data_orders():
    data_orders = fetch_insurance_efficiency(
        database=database, engine=engine, start_date=start_date, dw="mabawa_dw"
    )
    return data_orders


def daywise_efficiency():
    daywise_efficiency = fetch_daywise_efficiency(
        database=database,
        engine=engine,
        start_date=pstart_date,
        end_date=pend_date,
        dw="mabawa_dw",
    )

    return daywise_efficiency


def mtd_efficiency():
    mtd_efficiency = fetch_mtd_efficiency(
        database=database,
        engine=engine,
        start_date=pstart_date,
        end_date=pend_date,
        dw="mabawa_dw",
    )

    return mtd_efficiency


def no_view_non_conversions():
    no_view_non_conversions = fetch_non_conversion_non_viewed(
        database="mabawa_mviews", engine=engine, start_date=start_date
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
        drop="KENYA PIPELINE COMPANY",
    )


def daywise_rejections():
    daywise_rejections = fetch_daywise_rejections(
        database=database,
        view="mabawa_mviews",
        engine=engine,
        start_date=pstart_date,
        end_date=pend_date,
    )

    return daywise_rejections


def mtd_rejections():
    mtd_rejections = fetch_mtd_rejections(
        database=database,
        view="mabawa_mviews",
        engine=engine,
        start_date=pstart_date,
        end_date=pend_date,
    )

    return mtd_rejections


def rejections_drop():
    drop = fetch_rejections_drop(engine=engine)
    return drop


def build_kenya_rejections():
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
        drop=rejections_drop(),
    )


def customers():
    customers = fetch_customers(
        database="mabawa_mviews", engine=engine, start_date=start_date
    )

    return customers


def build_kenya_sops():
    return
    create_ug_sops_report(
        selection=selection,
        branch_data=branch_data(),
        sops_info=sops_info(),
        start_date=start_date,
        path=path,
        customers=customers(),
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
        selection=selection,
    )


def build_kenya_ratings_report():
    return
    create_ratings_report(
        selection=selection, surveys=surveys(), branch_data=branch_data(), path=path
    )


def build_kenya_opening_time():
    return
    create_opening_time_report(opening_data(), path)


data_fetcher = FetchData(engine=engine, database="mabawa_staging")


def orderscreenc1():
    orderscreen = data_fetcher.fetch_orderscreen(start_date=fourth_week_start)

    return orderscreen


"""
FETCH INSURANCE COMPANIES
"""


def insurance_companies():
    insurance_companies = data_fetcher.fetch_insurance_companies()
    return insurance_companies


"""
FETCH ORDERS DATA
"""


def orders():
    orders = data_fetcher.fetch_orders()
    return orders


"""
FETCH SALES ORDERS DATA
"""


def sales_orders():
    sales_orders = data_fetcher.fetch_sales_orders(start_date=first_week_start)

    return sales_orders


"""
FETCH NO FEEDBACKS DATA
"""


def no_feedbacks():
    no_feedbacks = data_fetcher.fetch_no_feedbacks(
        database="report_views", start_date=start_date
    )

    return no_feedbacks


"""
FETCH THE TIME TAKEN TO CONTACT THE 
CLIENT AFTER THE INSURANCE APPROVAL FEEDBACK
"""


def contact_time() -> pd.DataFrame:
    contact_time = fetch_client_contacted_time(
        start_date=start_date, engine=engine, view="mabawa_mviews"
    )

    return contact_time


"""
BUILD INSURANCE CONVERSION 
"""


def build_kenya_insurance_conversion() -> None:
    if selection == "Monthly":
        return
    create_insurance_conversion(
        path=path,
        all_orders=orders(),
        orderscreen=orderscreenc1(),
        branch_data=branch_data(),
        sales_orders=sales_orders(),
        insurance_companies=insurance_companies(),
        selection=selection,
        date=start_date,
        working_hours=working_hours(),
        country="Kenya",
        no_feedbacks=no_feedbacks(),
        contact_time=contact_time(),
        holidays=holidays(),
    )


"""
FETCH ORDERSCREEN LOGS
"""


def mtd_orderscreen():
    orderscreen = data_fetcher.fetch_orderscreen(start_date=str(pstart_date))

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
        date=pstart_date,
        selection="Monthly",
    )


"""
FETCH NON VIEW NON CONVERSION DATA
"""


def no_views_data() -> pd.DataFrame:
    no_views_data = fetch_no_views_data(
        database="mabawa_mviews", engine=engine, start_date=start_date
    )

    return no_views_data


"""
NON VIEW NON-CONVERSIONS
"""


def build_kenya_non_view_non_conversions():
    create_non_conversions_non_view(
        path=path,
        data=no_view_non_conversions(),
        selection=selection,
        start_date=fourth_week_start,
        no_views_data=no_views_data(),
    )


"""
FETCH EYE TEST ORDER TIME
"""


def eyetest_order():
    eyetest_order = fetch_eyetest_order(
        engine=engine, start_date=start_date, database="mabawa_staging"
    )

    return eyetest_order


"""
BUILD EYE TEST ORDER REPORT
"""


def build_eyetest_order() -> None:
    eyetest_order_time(data=eyetest_order(), path=path, selection=selection)


"""
FETCH TIME FROM UPLOAD ATTACHMENT TO SENT PREAUTH 
"""

"FETCH EFFICIENCY BEFORE FEEDBACK"


def desk_before() -> pd.DataFrame:
    desk_before = fetch_eff_bef_feed_desk(
        start_date=start_date,
        engine=engine,
        database="mabawa_staging",
        view="mabawa_mviews",
    )
    return desk_before


def no_desk_before() -> pd.DataFrame:
    no_desk_before = fetch_eff_bef_feed_nodesk(
        start_date=start_date,
        engine=engine,
        database="mabawa_staging",
        view="mabawa_mviews",
    )

    return no_desk_before


def build_efficiency_before_feedback() -> None:
    efficiencyBeforeFeedback(
        insurance_desk=desk_before(), no_insurance_desk=no_desk_before(), path=path
    )


"FETCH EFFICIENCY AFTER FEEDBACK"


def desk_after() -> pd.DataFrame:
    desk_after = fetch_eff_af_feed_desk(start_date=start_date, engine=engine)
    return desk_after


def no_desk_after() -> pd.DataFrame:
    no_desk_after = fetch_eff_af_feed_nodesk(start_date=start_date, engine=engine)

    return no_desk_after


def build_efficiency_after_feedback() -> None:
    efficiencyAfterFeedback(
        insurance_desk=desk_after(), no_insurance_desk=no_desk_after(), path=path
    )


def direct_conversion():
    direct_conversion = fetch_direct_insurance_conversion(
        start_date=start_date, engine=engine, mview="mabawa_mviews"
    )

    return direct_conversion


def build_direct_conversion():
    createDirectInsuranceConversion(
        data=direct_conversion(), selection=selection, path=path
    )


def sap_approval_update() -> pd.DataFrame:
    return efficiency_after_feedback(
        engine=engine, start_date=start_date, view="mabawa_mviews"
    )


def build_approval_update_efficieny():
    sap_update_efficieny(data=sap_approval_update(), selection=selection, path=path)


"""
LOWER LEVEL CODE
"""

"""
SEND TO THE BRANCHES
"""


def trigger_kenya_smtp():
    return
    if not assert_integrity(engine=engine, database="mabawa_staging"):
        print(
            "We run into an error. Ensure all the tables are updated in data warehouse and try again."
        )
        return

    send_draft_upload_report(
        selection=selection, path=path, country="Kenya", target=target
    )


"""
SEND TO THE MANAGEMENT
"""


def trigger_kenya_branches_smtp():
    if not assert_integrity(engine=engine, database="mabawa_staging"):
        print(
            "We run into an error. Ensure all the tables are updated in data warehouse and try again."
        )
        return

    send_to_branches(
        branch_data=branch_data(),
        selection=selection,
        path=path,
        country="Kenya",
        filename=f"{path}draft_upload/log.txt",
    )


def clean_kenya_folder():
    clean_folders(path=path)


"""
DATA TEAM DEVELOPMENTS
IT SHOULD BE UNIQUE THOUGH

SIGNED BY
DOUGLAS KATHURIMA
"""


# # push_kenya_efficiency_data()
# build_efficiency_before_feedback()

# trigger_kenya_branches_smtp()


# build_kenya_insurance_conversion()



