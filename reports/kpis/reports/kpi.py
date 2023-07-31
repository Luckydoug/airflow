from airflow.models import variable
import pandas as pd
import numpy as np
from reports.kpis.utils.utils import read_dataframe

#Google Reviews

def compile_kpi_report(path, branch_list = None) -> None:
    # Google Reviews
    mtd_pw_google_reviews = read_dataframe(
        name="google_reviews.xlsx",
        sheet_name="branch_mtd_pw",
        multindex=True,
        path=path
    ).set_index("Outlet")

    google_reviews_data = read_dataframe(
        name="google_reviews.xlsx",
        sheet_name="data",
        path=path
    )

    reviews_count = read_dataframe(
        name="google_reviews.xlsx",
        sheet_name="count_mtd_pw",
        path=path
    ).set_index("Outlet")

    print(mtd_pw_google_reviews)
    print(reviews_count)
