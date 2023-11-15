import pandas as pd
from airflow.models import variable
from reports.draft_to_upload.utils.utils import uganda_target
from sub_tasks.libraries.utils import (
    uganda_path,
    create_unganda_engine,
    createe_engine,
    fourth_week_start,
    first_week_start
)

from reports.draft_to_upload.smtp.smtp import (
    send_draft_upload_report,
    send_to_branches,
    clean_folders
)

from reports.draft_to_upload.reports.draft import (
    create_draft_upload_report
)
from reports.draft_to_upload.reports.opening_time import (
    create_opening_time_report
)
from reports.draft_to_upload.utils.utils import get_report_frequency, return_report_daterange, get_start_end_dates
from reports.draft_to_upload.data.push_data import push_insurance_efficiency_data
from reports.draft_to_upload.reports.rejections import create_rejection_report
from reports.draft_to_upload.reports.ratings import create_ratings_report
from reports.draft_to_upload.reports.plano import (
    create_plano_report
)
from reports.insurance_conversion.reports.conversion import create_insurance_conversion
from reports.insurance_conversion.data.fetch_data import FetchData
from reports.draft_to_upload.reports.sops import create_ug_sops_report
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
    fetch_opening_time,
    fetch_daywise_efficiency,
    fetch_insurance_efficiency,
    fetch_mtd_efficiency,
    fetch_customers,
    fetch_daywise_rejections,
    fetch_mtd_rejections,
    fetch_branch_data,
    fetch_working_hours,
    fetch_detractors
)

database = "mawingu_staging"
engine = create_unganda_engine()
engine2 = createe_engine()
selection = get_report_frequency()
start_date = return_report_daterange(
    selection=selection
)

start_date = pd.to_datetime(start_date, format="%Y-%m-%d").date()


def eyetests() -> pd.DataFrame:
    eyetests = fetch_eyetests(
        database=database,
        engine=engine
    )

    return eyetests


def orderscreen() -> pd.DataFrame:
    orderscreen = fetch_orderscreen(
        database=database,
        engine=engine
    )

    return orderscreen


def all_orders() -> pd.DataFrame:
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


def sales_orders() -> pd.DataFrame:
    sales_orders = fetch_salesorders(
        database=database,
        engine=engine
    )

    return sales_orders


def payments() -> pd.DataFrame:
    payments = fetch_payments(
        database=database,
        engine=engine
    )
    return payments


def planos() -> pd.DataFrame:
    planos = fetch_planos(
        engine=engine,
        database=database,
        schema="mawingu_staging",
        customers="source_customers",
        users="source_users",
        table="et_conv",
        views="mawingu_mviews"
    )

    return planos


def plano_orderscreen() -> pd.DataFrame:
    plano_orderscreen = fetch_planorderscreen(
        database=database,
        engine=engine
    )

    return plano_orderscreen


def sops_info() -> pd.DataFrame:
    sops_info = fetch_sops_branch_info(engine=engine2)
    return sops_info


def registrations() -> pd.DataFrame:
    registrations = fetch_registrations(
        engine=engine,
        database="mawingu_staging",
        table="source_customers",
        table2="source_users"
    )

    return registrations


def branch_data() -> pd.DataFrame:
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
    date = '2023-10-01'


def opening_data() -> pd.DataFrame:
    opening_data = fetch_opening_time(
        database=database,
        start_date=date,
        engine=engine
    )

    return opening_data


def build_uganda_opening_time() -> None:
    create_opening_time_report(
        opening_data(),
        uganda_path
    )

def working_hours() -> pd.DataFrame:
    working_hours = fetch_working_hours(
        engine=engine
    )
    return working_hours


def push_uganda_efficiency_data() -> None:
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


pstart_date, pend_date = get_start_end_dates(
    selection=selection
)


def data_orders() -> pd.DataFrame:
    data_orders = fetch_insurance_efficiency(
        database=database,
        engine=engine,
        start_date=start_date,
        dw="mawingu_dw"
    )

    return data_orders


def daywise_efficiency() -> pd.DataFrame:
    daywise_efficiency = fetch_daywise_efficiency(
        database=database,
        engine=engine,
        start_date=pstart_date,
        end_date=pend_date,
        dw="mawingu_dw"
    )

    return daywise_efficiency


def mtd_efficiency() -> pd.DataFrame:
    mtd_efficiency = fetch_mtd_efficiency(
        database=database,
        engine=engine,
        start_date=pstart_date,
        end_date=pend_date,
        dw="mawingu_dw"
    )

    return mtd_efficiency


def build_ug_draft_upload() -> None:
    create_draft_upload_report(
        mtd_data=mtd_efficiency(),
        daywise_data=daywise_efficiency(),
        data_orders=data_orders(),
        selection=selection,
        start_date=start_date,
        target=uganda_target,
        branch_data=branch_data(),
        path=uganda_path,
    )


def daywise_rejections() -> pd.DataFrame:
    daywise_rejections = fetch_daywise_rejections(
        database=database,
        view="mawingu_mviews",
        engine=engine,
        start_date=pstart_date,
        end_date=pend_date
    )

    return daywise_rejections


def mtd_rejections() -> pd.DataFrame:
    mtd_rejections = fetch_mtd_rejections(
        database=database,
        view="mawingu_mviews",
        engine=engine,
        start_date=pstart_date,
        end_date=pend_date
    )

    return mtd_rejections


def build_ug_rejections() -> None:
    create_rejection_report(
        orderscreen=orderscreen(),
        all_orders=all_orders(),
        branch_data=branch_data(),
        path=uganda_path,
        selection=selection,
        start_date=start_date,
        sales_orders=sales_orders(),
        mtd_data=mtd_rejections(),
        daywise_data=daywise_rejections()
    )


def customers() -> pd.DataFrame:
    customers = fetch_customers(
        database="mawingu_mviews",
        engine=engine,
        start_date=start_date
    )

    return customers


def build_ug_sops() -> None:
    create_ug_sops_report(
        selection=selection,
        branch_data=branch_data(),
        sops_info=sops_info(),
        start_date=start_date,
        path=uganda_path,
        customers=customers()
    )


def surveys():
    surveys = fetch_detractors(
        database=database,
        engine=engine
    )

    return surveys


def build_uganda_ratings_report():
    create_ratings_report(
        selection=selection,
        surveys=surveys(),
        branch_data=branch_data(),
        path=uganda_path
    )


def build_plano_report() -> None:
    create_plano_report(
        branch_data=branch_data(),
        path=uganda_path,
        registrations=registrations(),
        payments=payments(),
        all_planos=planos(),
        plano_orderscreen=plano_orderscreen(),
        all_orders=all_orders(),
        selection=selection
    )


data_fetcher = FetchData(
    engine=engine,
    database="mawingu_staging"
)


def insurance_companies() -> pd.DataFrame:
    insurance_companies = data_fetcher.fetch_insurance_companies()

    return insurance_companies


def orders() -> pd.DataFrame:
    orders = data_fetcher.fetch_orders()

    return orders


def sales_orders() -> pd.DataFrame:
    sales_orders = data_fetcher.fetch_sales_orders(
        start_date=first_week_start
    )

    return sales_orders


def no_feedbacks() -> pd.DataFrame:
    no_feedbacks = data_fetcher.fetch_no_feedbacks(
        database="report_views",
        start_date=start_date
    )

    return no_feedbacks


def build_uganda_insurance_conversion() -> None:
    create_insurance_conversion(
        path=uganda_path,
        all_orders=orders(),
        orderscreen=orderscreen(),
        branch_data=branch_data(),
        sales_orders=sales_orders(),
        insurance_companies=insurance_companies(),
        selection=selection,
        date=start_date,
        working_hours=working_hours(),
        no_feedbacks=no_feedbacks(),
        country="Uganda"
    )


def trigger_uganda_smtp() -> None:
    # This is the function to trigger SMTP.
    # The Country argument is the name of the country the report is for.
    # If wanted to send for Rwanda then you would pass Rwanda as the argument for country parameter.
    # To Test this report. please pass Test as the argument.
    send_draft_upload_report(
        selection=selection,
        path=uganda_path,
        country="Uganda",
        target=uganda_target
    )


def trigger_uganda_branches_smtp() -> pd.DataFrame:
    send_to_branches(
        branch_data=branch_data(),
        selection=selection,
        path=uganda_path,
        country="Uganda",
        filename=f"{uganda_path}draft_upload/log.txt"
    )


def clean_uganda_folder() -> pd.DataFrame:
    clean_folders(path=uganda_path)
