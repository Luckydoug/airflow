import pandas as pd
import numpy as np
from typing import Union
from sub_tasks.libraries.utils import get_comparison_months
from sub_tasks.libraries.styles import (properties, ug_styles)
import datetime


def get_queue_frequency():
    today = datetime.date.today()
    
    if today.weekday() == 4: 
        return "Weekly"
    elif today.day == 1:  
        return "Monthly"
    else:
        return "Daily"


def create_queue_summary(
    data: pd.DataFrame,
    index: list
) -> pd.DataFrame:
    
    agg_func = lambda x: round(x.mean(), 0)

    summary = pd.pivot_table(
        data,
        index=index,
        values=["Visit ID","Queue Time", "Eye Test Time"],
        aggfunc={
            "Visit ID": "count",
            "Queue Time": agg_func,
            "Eye Test Time": agg_func
        }
    ).reset_index()

    cols = index + [
        "Avg ET Time",
        "Avg Queue Time",
        "ETs Count"
    ]

    print(summary)

    summary.columns = cols
    cols_order = index + ["ETs Count", "Avg Queue Time", "Avg ET Time"]
    return summary[cols_order]

def create_queue_weekly(
    queue_data: pd.DataFrame,
    index: Union[list, str],
    target: int
) -> pd.MultiIndex:
    
    weekly_summary = pd.pivot_table(
        queue_data,
        index = index,
        values = [
            "Visit ID",
            "Queue Time", 
            "Eye Test Time"
        ],
        aggfunc={
            "Visit ID": "count",
            "Queue Time":"mean", 
            "Eye Test Time":"mean"
            },
        columns=["Week Range"]
    )

    weekly_summary =  weekly_summary.reindex(
        ["Visit ID", "Queue Time"], 
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
            "Visit ID": "Total Added to Queue",
            "Queue Time": f"Avg. Queue Time (Target = {target})",
            "Eye Test Time": "Avg. ET Time"
        },
        level = 0).round(0).fillna("-")

    return weekly_summary
    
def create_queue_monthly(
    queue_data: pd.DataFrame,
    index: Union[list, str],
) -> pd.MultiIndex:
    first_month, second_month = get_comparison_months()
    data = queue_data[queue_data["Month"].isin([first_month, second_month])]
    
    month_summary = pd.pivot_table(
        data,
        index = index,
        columns = "Month",
        values = [
            "Visit ID",
            "Queue Time", 
            "Eye Test Time"
        ],

        aggfunc={
            "Visit ID": "count",
            "Queue Time":"mean", 
            "Eye Test Time":"mean"
        }
    )

    month_summary = month_summary.reindex(
        ["Visit ID", "Queue Time"], 
        level = 0, 
        axis = 1
    )

    month_summary = month_summary.reindex(
        [first_month, second_month], 
        level = 1, 
        axis = 1
    )

    month_summary = month_summary.rename(
        columns = {
            "Visit ID": "ETs Count",
            "Queue Time": "Avg. Queue Time",
            "Eye Test Time": "Avg. ET Time"
        },
        level = 0).round(0)

    return month_summary


def transform_multindex(
        excel_file: pd.ExcelFile, 
        parse: str, 
        initial: str,
        filter = False,
        branch = None
    ) -> pd.MultiIndex:

    dataframe = excel_file.parse(
        parse,
        index_col = [0],
        header = [0 , 1]
    )

    dataframe = dataframe.reset_index()
    dataframe = dataframe.rename(
        columns = {initial: ""},
        level = 0
    )

    dataframe = dataframe.rename(
        columns = {"": initial},
        level = 1
    )

    if filter and branch is not None:
        dataframe = dataframe[dataframe[("", "Branch")] == branch]

    dataframe_style = dataframe.style.hide_index().set_table_styles(ug_styles).set_properties(**properties)
    
    dataframe_html = dataframe_style.to_html(doctype_html = True)

    return dataframe_html


def get_two_weeks_date_range():
    today = datetime.datetime.now().date()
    end_date = today - datetime.timedelta(days=14)
    start_date = end_date
    date_range = []
    for i in range(2):
        end_date = start_date + datetime.timedelta(days=6)
        date_range.append((start_date, end_date))
        start_date = end_date + datetime.timedelta(days=1)
    return date_range


date_ranges = get_two_weeks_date_range()
first_week_start = (date_ranges[0][0]).strftime('%Y-%m-%d')
first_week_end = date_ranges[0][1].strftime('%Y-%m-%d')
second_week_start = date_ranges[1][0].strftime('%Y-%m-%d')
second_week_end = date_ranges[1][1].strftime('%Y-%m-%d')


def date_in_range(date, start_date, end_date):
    if start_date <= date <= end_date:
        return True
    return False


def check_date_range(row, x):
    date = row[x].strftime('%Y-%m-%d')
    if date_in_range(date, first_week_start, first_week_end):
        return str(pd.to_datetime(first_week_start).strftime('%b-%d')) + " " + "to" + " " + str(pd.to_datetime(first_week_end).strftime('%b-%d'))
    elif date_in_range(date, second_week_start, second_week_end):
        return str(pd.to_datetime(second_week_start).strftime('%b-%d')) + " " + "to" + " " + str(pd.to_datetime(second_week_end).strftime('%b-%d'))
    else:
        return "None"

