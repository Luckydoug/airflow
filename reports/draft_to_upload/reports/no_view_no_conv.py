from airflow.models import variable
import pandas as pd
from reports.draft_to_upload.utils.utils import today

"""
This function gets the data from the fuction fetch_non_conversion_non_views()
It checks the eye tests that have not been viewed and have not been converted
It call the function, supply it with appropriate parameters as follows
path - is the location you want your report to be saved
data - is the raw data returned from the function mentioned above.
selection = this is automatic depending on the day of move, but the selection can either be Daily, Weekly or Monthly.
start_date - This the date you want your filter to choose between
"""


def create_non_conversions_non_view(path, data, selection, start_date):
    tod = pd.to_datetime(today, format="%Y-%m-%d").date()
    start_date = pd.to_datetime(start_date, format="%Y-%m-%d").date()
    data["CreateDate"] = pd.to_datetime(data["CreateDate"], format="%Y-%m-%d").dt.date
    data = data[(data["CreateDate"] >= start_date) & (data["CreateDate"] <= tod)]

    if selection == "Daily":
        with pd.ExcelWriter(f"{path}draft_upload/non_view.xlsx") as writer:
            data.to_excel(writer, sheet_name = "Daily Data", index = False)

    elif selection == "Weekly":
        with pd.ExcelWriter(f"{path}draft_upload/non_view.xlsx") as writer:
            data.to_excel(writer, sheet_name = "Weekly Data", index = False)

    elif selection == "Daily":
        with pd.ExcelWriter(f"{path}draft_upload/non_view.xlsx") as writer:
            data.to_excel(writer, sheet_name = "Daily Data", index = False)