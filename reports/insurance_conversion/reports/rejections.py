import pandas as pd
from airflow.models import variable
from reports.queue_time.utils.utils import check_date_range as check_range

def create_rejections_report(data_rejections, selection, path):
    data_rejections["Week Range"] = data_rejections.apply(lambda row: check_range(row, "Date"), axis = 1)
    data_rejections = data_rejections[data_rejections["Week Range"] != "None"]
        
    dr_weekly_summary = pd.pivot_table(
        data_rejections,
        index = ["Outlet", "Order Creator"],
        values = [
            "Order Number",
        ],
        aggfunc={
            "Order Number": "count",
            },
        columns=["Week Range"]
    )

    dr_weekly_summary =  dr_weekly_summary.reindex(
        ["Order Number"], 
        level = 0, 
        axis = 1
    )


    columns = dr_weekly_summary.columns.get_level_values(1)
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

    

    dr_weekly_summary = dr_weekly_summary.reindex(sorted_columns, axis = 1, level=1)

    dr_weekly_summary = dr_weekly_summary.rename(
        columns = {
            "Order Number": "Count of Insurance Errors",
        },
        level = 0).round(0).fillna("-")

    dr_weekly_summary =  dr_weekly_summary.reset_index()
    

    with pd.ExcelWriter(f"{path}insurance_conversion/rejections.xlsx") as writer:
        dr_weekly_summary.to_excel(
            writer, 
            sheet_name="weekly_summary"
        )
