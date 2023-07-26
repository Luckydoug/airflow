from airflow.models import variable
import pandas as pd
from sub_tasks.libraries.utils import createe_engine
from reports.kpis.utils.utils import get_kpi_dateranges
from reports.kpis.data.fetch_data import FetchData

#Basic Configurations
engine = createe_engine()
database = "mabawa_staging"
first_day, start_date, end_date = get_kpi_dateranges()

fetcher = FetchData(
    engine=engine, 
    end_date=end_date
)

data = fetcher.fetch_pw_eyetests(
    database = "mabawa_mviews",
    users = "mabawa_dw",
    users_table = "dim_users",
    start_date = start_date
)

print(data)


