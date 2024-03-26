import pandas as pd
from airflow.models import variable
from reports.queue_time.reports.queue_time import create_queue_time_report
from reports.queue_time.data.fetch_data import FecthData
from sub_tasks.libraries.utils import create_unganda_engine
from reports.queue_time.utils.utils import get_queue_frequency
from reports.draft_to_upload.utils.utils import return_report_daterange
from sub_tasks.libraries.utils import uganda_path
from reports.queue_time.smtp.smtp import send_branches_queue_time, send_to_management


engine = create_unganda_engine()
selection = get_queue_frequency()
start_date = return_report_daterange(selection)

data_fetcher = FecthData(
    engine=engine
)

def queue_data() -> pd.DataFrame:
    queue_data = data_fetcher.fetch_queue_data(
        start_date=start_date,
        mview="mawingu_mviews"
    )

    return queue_data

def branch_data() -> pd.DataFrame:
    branch_data = data_fetcher.fetch_branch_data(
        schema="reports_tables"
    )

    return branch_data

def build_uganda_queue_time() -> None:
    create_queue_time_report(
        selection=selection,
        start_date=start_date,
        queue_data=queue_data(),
        path = uganda_path,
        country="Uganda"
    )

def trigger_management_smtp() -> None:
    send_to_management(
        path=uganda_path,
        country="Uganda",
        selection=selection
    )

def trigger_branches_queue_smtp() -> None:
    send_branches_queue_time(
        path=uganda_path,
        selection=selection,
        branch_data=branch_data(),
        log_file=f"{uganda_path}queue_time/branch_log.txt",
        country="Uganda"

    )














