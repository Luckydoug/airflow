from airflow.models import variable
import pandas as pd
import datetime
from datetime import timedelta
import numpy as np

# This is the utils module for the KPIs report.
# All functions that are specific to only KPIs reports
# Should be defined in this module.
# If a function is used in other reports, you can import it
# in the file you want to use instead of re-inventing the wheel
# This will ensure good reuse of the code and no repetation.
# Always remember the DRY Concept. In short, Dont Repeat Yourself.


def get_kpi_dateranges() -> str:
    today = datetime.datetime.now().date()
    yesterday = today - timedelta(days=1)
    first_day_of_month = datetime.datetime(today.year, today.month, 1).date()
    start_date = today - timedelta(days=7)
    end_date = yesterday

    return (
        first_day_of_month.strftime("%Y-%m-%d"),
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d")
    )


def read_dataframe(name, path, sheet_name, multindex=False) -> pd.DataFrame:
    workbook = pd.ExcelFile(f"{path}kpi/{name}")
    if multindex:
        return workbook.parse(
            sheet_name,
            header=[0, 1],
            index_col=[0]
        )

    else:
        return workbook.parse(sheet_name, index_col=False)
