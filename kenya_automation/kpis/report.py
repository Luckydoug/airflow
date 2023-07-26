from airflow.models import variable
import pandas as pd
from sub_tasks.libraries.utils import createe_engine
from reports.kpis.utils.utils import get_kpi_dateranges
from reports.kpis.data.fetch_data import FetchData
from reports.kpis.reports.reviews import create_google_reviews_kpi

engine = createe_engine()
database = "mabawa_staging"
first_day, start_date, end_date = get_kpi_dateranges()

fetcher = FetchData(
    engine=engine,
    end_date=end_date,
    first_day=first_day,
    start_date=start_date
)

# Google Reviews
(   
    mtd_reviews, 
    pw_reviews, 
    reviews_data,
    mtd_conts,
    pw_counts

) = fetcher.fetch_google_reviews()

google_reviews, reviews_count, data = create_google_reviews_kpi(
    pw_reviews=pw_reviews,
    mtd_reviews=mtd_reviews,
    mtd_counts=mtd_conts,
    pw_counts=pw_counts,
    reviews_data=reviews_data
)

print(google_reviews)
print(reviews_count)
print(data)
