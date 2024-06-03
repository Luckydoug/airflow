import pandas as pd
from airflow.models import variable
from sub_tasks.libraries.utils import path
from sub_tasks.libraries.utils import createe_engine
from reports.bi_weekly.reports.report import create_month_trend
from reports.bi_weekly.data.fetch_data import fetch_biweekly_kpis
from reports.bi_weekly.reports.report import create_biweekly_report
from reports.bi_weekly.reports.report import create_biweekly_report
from reports.draft_to_upload.data.fetch_data import fetch_branch_data
from reports.bi_weekly.reports.report import create_biweekly_comparison
from reports.bi_weekly.utils.utils import first_range_start
from reports.bi_weekly.utils.utils import first_range_end
from reports.bi_weekly.utils.utils import second_range_start
from reports.bi_weekly.utils.utils import second_range_end
from reports.bi_weekly.smtp.smtp import send_to_branch
from reports.bi_weekly.data.fetch_data import KPIsData
from reports.bi_weekly.reports.report import generate_biweekly_data

engine = createe_engine()


"""
Create an Instance of KPIsData Class with the following
arguments. With the class instance, we can now acess the 
class methods and or properties
"""

KPIsFetcher = KPIsData(
    engine=engine,
    start_date=second_range_start,
    end_date=second_range_end,
    view="mabawa_mviews",
    database="mabawa_staging",
)


def is_up_to_date() -> bool:
    return KPIsFetcher.is_up_to_date


def opening_time() -> pd.DataFrame:
    return KPIsFetcher.fetch_opening_time()


def eyetests_not_converted() -> pd.DataFrame:
    return KPIsFetcher.fetch_eyetest_not_converted()


def eyetest_order() -> pd.DataFrame:
    return KPIsFetcher.fetch_eytest_to_order_delays()


def identifier_delays() -> pd.DataFrame:
    return KPIsFetcher.fetch_identifier_printing_delays()


def passives_comment() -> pd.DataFrame:
    return KPIsFetcher.fetch_passive_comments()


def poor_google_reviews() -> pd.DataFrame:
    return KPIsFetcher.fetch_poor_google_reviews()


def sops_not_complied() -> pd.DataFrame:
    return KPIsFetcher.fetch_sop_non_compliance()


def frame_only_orders() -> pd.DataFrame:
    return KPIsFetcher.fetch_frame_only_orders()


def feedbacks_not_converted() -> pd.DataFrame:
    return KPIsFetcher.fetch_insurance_feedback_not_converted()


def conversion_trend() -> pd.DataFrame:
    return KPIsFetcher.fetch_conversion_trend()


def export_data():
    generate_biweekly_data(
        path=path,
        opening_time=opening_time(),
        et_nt_converted=eyetests_not_converted(),
        et_order_delays=eyetest_order(),
        identifier_delays=identifier_delays(),
        passives=passives_comment(),
        poor_reviews=poor_google_reviews(),
        sops_not_complied=sops_not_complied(),
        frame_only_orders=frame_only_orders(),
        insurance_non_conversions=feedbacks_not_converted(),
    )


def bi_weekly_kpis(start_date, end_date):
    bi_weekly_kpis = fetch_biweekly_kpis(
        start_date=start_date,
        end_date=end_date,
        database="mabawa_staging",
        view="mabawa_mviews",
        engine=engine,
    )

    return bi_weekly_kpis


def branch_data() -> pd.DataFrame:
    return fetch_branch_data(engine=engine, database="reports_tables")


def build_biweekly_kpi() -> None:
    create_biweekly_report(
        data=fetch_biweekly_kpis(
            start_date=second_range_start,
            end_date=second_range_end,
            engine=engine,
            database="mabawa_staging",
            view="mabawa_mviews",
        ),
        path=path,
    )


def build_biweekly_kpi_comparison() -> None:
    create_biweekly_comparison(
        data1=fetch_biweekly_kpis(
            start_date=first_range_start,
            end_date=first_range_end,
            engine=engine,
            database="mabawa_staging",
            view="mabawa_mviews",
        ),
        data2=fetch_biweekly_kpis(
            start_date=second_range_start,
            end_date=second_range_end,
            engine=engine,
            database="mabawa_staging",
            view="mabawa_mviews",
        ),
        path=path,
    )


def build_conversion_trend() -> None:
    create_month_trend(path=path, data=conversion_trend())


# send_to_branch(path=path, branch_data=branch_data())