import pandas as pd
import numpy as np
from reports.queue_time.utils.utils import check_date_range

def eyetest_order_time(data, path, selection):
    if selection == "Weekly":
        data["Week Range"] = data.apply(lambda row: check_date_range(row, "et_completed_time"), axis = 1)
        data = data[data["Week Range"] != "None"]
        
        weekly_summary = pd.pivot_table(
            data,
            index = ["order_branch", "order_creator"],
            values = [
                "visit_id",
            ],
            aggfunc={
                "visit_id": "count",
                },
            columns=["Week Range"]
        )

        weekly_summary =  weekly_summary.reindex(
            ["visit_id"], 
            level = 0, 
            axis = 1
        )


        columns = weekly_summary.columns.get_level_values(1)
        dates = []

        for date in columns:
            stat_date = pd.to_datetime(date.split(" to ")[0], format="%b-%d")
            dates.append(stat_date)
        unique_dates = list(set(dates))
        sorted_dates = sorted(unique_dates)
        sorted_dates

        sorted_columns = []

        for date in sorted_dates:
            date_range = f"{date.strftime('%b-%d')} to " + f"{(date + pd.Timedelta(6, unit='d')).strftime('%b-%d')}"
            sorted_columns.append(date_range)

        

        weekly_summary = weekly_summary.reindex(sorted_columns, axis = 1, level=1)

        weekly_summary = weekly_summary.rename(
            columns = {
                "visit_id": "Delayed Orders",
            },
            level = 0).round(0).fillna("-")

        weekly_summary =  weekly_summary.reset_index()

        with pd.ExcelWriter(f"{path}draft_upload/et_to_order.xlsx") as writer:
            weekly_summary.to_excel(writer, sheet_name = "Data")


    if selection == "Daily":
        with pd.ExcelWriter(f"{path}draft_upload/et_to_order.xlsx") as writer:
            late_orders = data[data["Time Taken"] > 45]

            late_orders["ET Completed Time"] = pd.to_datetime(late_orders["ET Completed Time"], format="%Y-%m-%d %H:%M").dt.strftime("%Y-%m-%d %H:%M")
            late_orders["Order Date"] = pd.to_datetime(late_orders["Order Date"], format="%Y-%m-%d %H:%M").dt.strftime("%Y-%m-%d %H:%M")
            late_orders.to_excel(writer, sheet_name = "Data", index = False)
    else:
        return
