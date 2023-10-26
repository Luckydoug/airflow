from airflow.models import variable
import pandas as pd
from reports.draft_to_upload.utils.utils import today

"""
This function gets the data from the fuction fetch_non_conversion_non_views()
It checks the eye tests that have not been viewed and have not been converted
To call the function, supply it with appropriate parameters as follows
path - is the location you want your report to be saved
data - is the raw data returned from the function mentioned above.
selection = this is automatic depending on the day of move, but the selection can either be Daily, Weekly or Monthly.
start_date - This the date you want your filter to choose between
"""


def create_non_conversions_non_view(path, data, selection, start_date, no_views_data):
    tod = pd.to_datetime(today, format="%Y-%m-%d").date()
    start_date = pd.to_datetime(start_date, format="%Y-%m-%d").date()
    data["CreateDate"] = pd.to_datetime(data["CreateDate"], format="%Y-%m-%d").dt.date
    data = data[(data["CreateDate"] >= start_date) & (data["CreateDate"] <= tod)]

    if selection == "Daily":
        summayr_pivot = pd.pivot_table(
            no_views_data,
            index = "Handed Over To",
            values = ["Code", "RX","High RX Conversion", "Low RX Conversion", "No View NoN", "High_Non", "Low_Non"],
            aggfunc={
                "Code": ["count"],
                "No View NoN": "sum",
                "High_Non": "sum",
                "Low_Non": "sum",
                "RX": [lambda x: (x == "High Rx").sum(), lambda x: (x == "Low Rx").sum()],
                "High RX Conversion": "sum",
                "Low RX Conversion": "sum"
            }
        ).reset_index()
        summayr_pivot.columns = ["Handed Over To", "ETs", "High RX Conv", "High RX Non Views", "Low RX Conv", "Low RX Non Views", "No View NoN", "High RX Count", "Low RX Count"]
        no_views = summayr_pivot[summayr_pivot["No View NoN"] > 0].copy()
        no_views["% High RX Non Views"] = ((no_views["High RX Non Views"] / no_views["No View NoN"]) * 100).round(0)
        no_views["% Low RX Non Views"] = ((no_views["Low RX Non Views"] / no_views["No View NoN"]) * 100).round(0)
        final_table = no_views[["Handed Over To", "ETs", "No View NoN", "High RX Non Views", "% High RX Non Views","Low RX Non Views", "% Low RX Non Views"]]

        with pd.ExcelWriter(f"{path}draft_upload/non_view.xlsx") as writer:
            final_table.to_excel(writer, sheet_name = "EWC Summary", index = False)
            data.to_excel(writer, sheet_name = "Daily Data", index = False)


    elif selection == "Weekly":
        with pd.ExcelWriter(f"{path}draft_upload/non_view.xlsx") as writer:
            data.to_excel(writer, sheet_name = "Weekly Data", index = False)

    elif selection == "Daily":
        with pd.ExcelWriter(f"{path}draft_upload/non_view.xlsx") as writer:
            data.to_excel(writer, sheet_name = "Daily Data", index = False)