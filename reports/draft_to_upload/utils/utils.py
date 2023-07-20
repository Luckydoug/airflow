"""
This file contains functions that are specific to a certain report.
"""
import numpy as np
import calendar
from airflow.models import variable
from sub_tasks.libraries.utils import service_file
from datetime import timedelta
import datetime
import pygsheets
import pandas as pd
from sub_tasks.libraries.utils import arrange_dateranges
import warnings

warnings.simplefilter(action='ignore', category=UserWarning)
target = 8
uganda_target = 8

today = datetime.datetime.now().strftime("%Y-%m-%d")

service_key = pygsheets.authorize(service_file=service_file)
sheet = service_key.open_by_key('1Wn7O54ohdn9z1YineGomEqIGCVw3GrzUSafOpYuIv_k')
to_drop = pd.DataFrame(sheet[0].get_all_records())

def get_report_frequency():
    today = datetime.date.today()
    
    if today.weekday() == 0: 
        return "Weekly"
    elif today.day == 1:  
        return "Monthly"
    else:
        return "Daily"


def return_report_daterange(selection):
    start_date = ""
    if selection == "Daily":
        today = datetime.date.today()
        if today.weekday() == 0:
            days_to_subtract = 2
        else:
            days_to_subtract = 1
        start_date = (
            today - datetime.timedelta(days=days_to_subtract)).strftime("%Y-%m-%d")
    if selection == "Weekly":
        today = datetime.date.today()
        delta = timedelta(weeks=4, days=1)
        start_date = (today - delta).strftime("%Y-%m-%d")
    if selection == "Monthly":
        today = datetime.date.today()
        if today.month <= 2:
            target_month = today.month + 10
            target_year = today.year - 1
        else:
            target_month = today.month - 2
            target_year = today.year
        start_date = datetime.date(
            target_year, target_month, 1).strftime("%Y-%m-%d")
        
    return start_date




def get_comparison_months():
    today = datetime.date.today()
    prev_month = today.month - 1
    prev_year = today.year
    if prev_month < 1:
        prev_month += 12
        prev_year -= 1
    prev_month_name = calendar.month_name[prev_month]

    prev_prev_month = prev_month - 1
    prev_prev_year = prev_year
    if prev_prev_month < 1:
        prev_prev_month += 12
        prev_prev_year -= 1
    prev_prev_month_name = calendar.month_name[prev_prev_month]
    
    return prev_prev_month_name, prev_month_name


def create_daily_submission_pivot(plano_data, index, cols, cols_order):
    daily_submission_branch = pd.pivot_table(plano_data,
        index=index,
        values=["Code","Submission", "Conversion"],
        aggfunc = {"Code": "count","Submission": [
            lambda x: ((x == "Submitted") | (x == "Submitted: (Cash/Direct)")).sum(), lambda x: (x == "Not Submitted").sum()
        ],
                "Conversion": "sum"}
    ).reset_index()

    try:
        daily_submission_branch.columns = cols
        daily_submission_branch = daily_submission_branch[cols_order]
        daily_submission_branch["%Conversion"] = (
            (daily_submission_branch["Converted"] / daily_submission_branch["Plano Eye Tests"]) * 100
        ).round(0).astype(int).astype(str) + "%"
        daily_submission_branch = daily_submission_branch.sort_values(by="SRM")

        return daily_submission_branch
    except:
        print("No Data For the Pivot")


def plano_submission_multindex(plano_data, index, set_index, columns, month):
    first_month, second_month = get_comparison_months()
    submission_branch = pd.pivot_table(
        plano_data,
        index=index,
        values=["Code","Submission", "Conversion"],
        columns=columns,
        aggfunc = {"Code": "count","Submission": [
            lambda x: ((x == "Submitted") | (x == "Submitted: (Cash/Direct)")).sum(), lambda x: (x == "Not Submitted").sum()
        ],
        "Conversion": "sum"}
    )

    if month:                         
        submission_branch = submission_branch.swaplevel(1, 0, 1).swaplevel(1, 2, 1)
        submission_branch = submission_branch.sort_index(level=[0,1], axis = 1).droplevel(2, axis=1).swaplevel(1, 0, 1)
        submission_branch = submission_branch.reindex(
        columns=[first_month, second_month], level = 0).reindex(
            columns=["count", "<lambda_0>", "<lambda_1>", "sum"], 
            level = 1
        )

        submission_branch = submission_branch.reset_index().fillna(0).rename(
            columns={"count": "Plano Eyetests", "<lambda_0>": "Submitted", "<lambda_1>": "Not Submitted", "sum": "Converted"}
        ).set_index(set_index)

        return submission_branch

    else:
        submission_branch = submission_branch.swaplevel(1, 0, 1).swaplevel(1, 2, 1).sort_index(level=[0,1], axis = 1)
        submission_branch =  submission_branch.droplevel(2, axis=1).swaplevel(1, 0, 1)
        sorted_columns = arrange_dateranges(submission_branch)
        submission_branch = submission_branch.reindex(
        columns=sorted_columns, level = 0).reindex(
            columns=["count", "<lambda_0>", "<lambda_1>", "sum"], level = 1
        ).reset_index().fillna(0).rename(
            columns={"count": "Plano Eyetests", "<lambda_0>": "Submitted", "<lambda_1>": "Not Submitted", "sum": "Converted"}
        ).set_index(set_index)

        return submission_branch
    


def highlight_efficiency(value):
    if value >= 90:
        colour = "green"
    elif value >= 80:
        colour = "yellow"
    else:
        colour = "red"
    return 'background-color: {}'.format(colour)


def create_rejections_ewc(orders, rejections):
    ewc_daily_orders = pd.pivot_table(
            orders,
            index=["Outlet", "Order Creator"],
            aggfunc="count",
            values="Order Number"
    ).reset_index().rename(columns={"Order Number": "Total Orders"})
     
    ewc_daily_rejections = pd.pivot_table(
        rejections,
        index=["Order Creator", "Outlet"],
        values="Order Number",
        aggfunc="count"
    ).reset_index().rename(columns={"Order Number": "Count of Rejections"})

    

    final = pd.merge(
        ewc_daily_orders,
        ewc_daily_rejections,
        on = ["Outlet", "Order Creator"],
        how = "outer"
    ).fillna(0)

    final["% Rejected"] = round((final["Count of Rejections"] / final["Total Orders"]) * 100, 0).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)

    return final[["Outlet", "Order Creator", "Total Orders", "Count of Rejections", "% Rejected"]]

def create_rejections_branches(orders, rejections, branch_data):
    daily_insurance_orders_pivot = pd.pivot_table(
        orders,
        index = "Outlet", 
        aggfunc="count", 
        values="Order Number"
    ).reset_index().rename(columns={"Order Number": "Total Ins Orders"})
     
    daily_rejections_pivot = pd.pivot_table(
        rejections,
        index = "Outlet",
        values="Order Number",
        aggfunc="count"
    ).reset_index().rename(columns={"Order Number": "Count of Rejections"})

    daily_rejections_branches = pd.merge(
        branch_data[["Outlet", "RM", "SRM"]],
        daily_rejections_pivot,
        on = "Outlet",
        how = "left"
    ).fillna(0)

    daily_rej_total = pd.merge(
        daily_rejections_branches, 
        daily_insurance_orders_pivot, 
        on = "Outlet", 
        how = "left"
    ).fillna(0)[["Outlet", "RM", "SRM", "Total Ins Orders", "Count of Rejections"]]

    daily_rej_total["% Rejected"] = round((daily_rej_total["Count of Rejections"] / daily_rej_total["Total Ins Orders"]) * 100, 0).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)

    return daily_rej_total

