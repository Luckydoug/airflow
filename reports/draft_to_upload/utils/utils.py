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
            days_to_subtract = 1
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
        aggfunc = {
            "Code": "count","Submission": [
                lambda x: ((x == "Submitted") | (x == "Submitted: (Cash/Direct)")).sum(), 
                lambda x: (x == "Not Submitted").sum()
            ],
            "Conversion": "sum"
        }
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
            columns={"count": "Insurance Eyetests", "<lambda_0>": "Submitted", "<lambda_1>": "Not Submitted", "sum": "Converted"}
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
            columns={"count": "Insurance Eyetests", "<lambda_0>": "Submitted", "<lambda_1>": "Not Submitted", "sum": "Converted"}
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


def return_slade(row):
    slades = [
        "JUBILEE",
        "APA",
        "MADISON"
    ]

    if row["Insurance Company"] in slades:
        return "Yes"
    else:
        return "No"
    

def get_start_end_dates(selection):
    if selection == "Monthly":
        today = datetime.date.today()
        if today.month < 2:
            target_month = today.month + 11
            target_year = today.year - 1
        else:
            target_month = today.month - 1
            target_year = today.year
        
        start_date = datetime.date(target_year, target_month, 1).strftime("%Y-%m-%d")
        month_days  = calendar.monthrange(target_year, target_month)[1]
        end_date = datetime.date(target_year, target_month, month_days).strftime("%Y-%m-%d")
        return start_date, end_date

    else:
        today = datetime.datetime.today()
        start_date = today.replace(day=1)
        end_date = today - timedelta(days=1)
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    

def create_monthly_draft(dataframe, first_month, second_month):
    monthly_stack = dataframe.stack()
    monthly_stack["Efficiency"] = round((monthly_stack[(
        'Draft to Upload', '<lambda>')] / monthly_stack[('Upload Time',    'count')]) * 100, 0)
    monthly_unstack = monthly_stack.unstack()
    monthly_unstack_two = monthly_unstack.swaplevel(0, 2, 1).droplevel(1, axis = 1)
    monthly_unstack_two = monthly_unstack_two.rename(columns={"Draft to Upload": f"Orders <= {target}", "Upload Time": "Total Orders"})
    final_month_report = monthly_unstack_two.reindex([first_month, second_month], level = 0, axis = 1)
    final_month_report = final_month_report.reindex(["Total Orders", f"Orders <= {target}", "Efficiency"], level = 1, axis = 1)

    return final_month_report



def create_monthly_rejections(
    insurance: pd.DataFrame, 
    first_month: str, 
    second_month: str, 
    branch_data: pd.DataFrame, 
    sales_orders: pd.DataFrame,
    rejections: pd.DataFrame
) -> pd.MultiIndex:
    monthly_insurance_orders = insurance.copy()
    monthly_insurance_orders["Month"] = monthly_insurance_orders["CreateDate"].dt.month_name(
    )
    monthly_insu_orders_data = monthly_insurance_orders[
        (monthly_insurance_orders["Month"] == first_month) |
        (monthly_insurance_orders["Month"] == second_month)
    ]

    monthly_insu_orders_data = pd.merge(
        monthly_insu_orders_data,
        branch_data[["Outlet", "RM", "SRM"]],
        on="Outlet",
        how="left"
    )

    monthly_rejections = rejections.copy()
    monthly_rejections = monthly_rejections.drop_duplicates(subset=["DocEntry", "Date", "Time"])
    monthly_rejections["Month"] = pd.to_datetime(monthly_rejections["Date"], dayfirst=True).dt.month_name()
    monthly_rejections_data = monthly_rejections[
        (monthly_rejections["Month"] == first_month) |
        (monthly_rejections["Month"] == second_month)
    ]

    monthly_rejection_conversion = monthly_rejections_data[
        monthly_rejections_data["Order Number"].isin(
            sales_orders["Order Number"]
        )
    ]
    monthly_conversion_pivot = pd.pivot_table(
        monthly_rejection_conversion,
        index=["Outlet", "RM", "SRM", "Order Creator"],
        columns="Month",
        values="Order Number",
        aggfunc="count"
    )

    monthly_insu_orders_pivot = pd.pivot_table(
        monthly_insu_orders_data,
        index=["Outlet", "RM", "SRM", "Order Creator"],
        columns="Month",
        values="Order Number",
        aggfunc="count"
    )

    monthly_rejections_pivot = pd.pivot_table(
        monthly_rejections_data,
        index=[
            "Outlet", 
            "RM", 
            "SRM", 
            "Order Creator"
        ],
        columns="Month",
        values="Order Number",
        aggfunc="count"
    )

    monthly_insu_orders_pivot.columns = pd.MultiIndex.from_product([["Total Orders"], monthly_insu_orders_pivot.columns])
    monthly_rejections_pivot.columns = pd.MultiIndex.from_product([["Rejected"], monthly_rejections_pivot.columns])
    monthly_conversion_pivot.columns = pd.MultiIndex.from_product([["Conversion"], monthly_conversion_pivot.columns])

    final_monthly_rejections = pd.merge(
        monthly_insu_orders_pivot, 
        monthly_rejections_pivot, 
        right_index=True, 
        left_index=True, 
        how="left"
    ).fillna(0)

    final_monthly_rejections = pd.merge(
        final_monthly_rejections, 
        monthly_conversion_pivot, 
        right_index=True, 
        left_index=True, 
        how="left"
    ).fillna(0)

    final_monthly_rejections = final_monthly_rejections.swaplevel(0, 1, axis=1).sort_index(axis=1, level=0)
    final_monthly_rejections = final_monthly_rejections.reindex(
        ["Total Orders", "Rejected", "Conversion"], 
        axis=1, 
        level=1
    )

    columns = [
        (first_month, "Total Orders"),
        (first_month, "Rejected"),
        (first_month, "Conversion"),
        (second_month, "Total Orders"),
        (second_month, "Rejected"),
        (second_month, "Conversion")
    ]
    final_monthly_rejections = final_monthly_rejections.reindex(
        columns, 
        axis=1, 
        fill_value=0
    )

    for date in final_monthly_rejections.columns.levels[0]:
        col_name = (date, '%Rejected')
        result = (final_monthly_rejections[(date, 'Rejected')] / final_monthly_rejections[(
            date, 'Total Orders')] * 100).round(0).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)
        final_monthly_rejections.insert(final_monthly_rejections.columns.get_loc(
            (date, 'Rejected')) + 1, col_name, result)

    for date in final_monthly_rejections.columns.levels[0]:
        col_name = (date, '%Conversion')
        result = (final_monthly_rejections[(date, 'Conversion')] / final_monthly_rejections[(
            date, 'Rejected')] * 100).round(0).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)
        final_monthly_rejections.insert(final_monthly_rejections.columns.get_loc(
            (date, 'Conversion')) + 1, col_name, result)

    final_monthly_rejections = final_monthly_rejections.reindex(
        ["Total Orders", "Rejected", "%Rejected"], 
        level=1, 
        axis=1
    )

    return final_monthly_rejections




def create_no_views_report(
    no_views: pd.DataFrame,
    index: list,
    append: list
) -> pd.DataFrame:
    summayr_pivot = pd.pivot_table(
        no_views,
        index = index,
        values = [
            "Code", 
            "RX",
            "High RX Conversion", 
            "Low RX Conversion", 
            "No Views NoN", 
            "High_Non", 
            "Low_Non"
        ],
        aggfunc={
            "Code": ["count"],
            "No Views NoN": "sum",
            "High_Non": "sum",
            "Low_Non": "sum",
            "RX": [lambda x: (x == "High Rx").sum(), lambda x: (x == "Low Rx").sum()],
            "High RX Conversion": "sum",
            "Low RX Conversion": "sum"
            }
    ).reset_index()

    summayr_pivot.columns = append + [
        "ETs", 
        "High RX Conv", 
        "High RX Non Views", 
        "Low RX Conv", 
        "Low RX Non Views", 
        "No Views NoN", 
        "High RX Count", 
        "Low RX Count"
    ]

    no_views = summayr_pivot[summayr_pivot["No Views NoN"] > 0].copy()
    no_views["% High RX Non Views"] = ((no_views["High RX Non Views"] / no_views["No Views NoN"]) * 100).round(0)
    no_views["% Low RX Non Views"] = ((no_views["Low RX Non Views"] / no_views["No Views NoN"]) * 100).round(0)

    common_cols = [ 
        "ETs", 
        "No Views NoN", 
        "High RX Non Views", 
        "% High RX Non Views",
        "Low RX Non Views", 
        "% Low RX Non Views"
    ]

    all_columns = append + common_cols

    return no_views[all_columns].sort_values(by = "No Views NoN", ascending=False)

planos_cols = [
    "Customer Code",
    "Plano RX",
    "Insurance Company",
    "Optom Name",
    "EWC Handover",
    "Who Viewed RX"
]


def generate_html_and_subject(branch, branch_manager, dataframe_dict, date, styles):
    html_start_template = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <title>Document</title>
        <style>
            table {{border-collapse: collapse; font-family: Bahnschrift, sans-serif; font-size: 10px;}}
            th {{text-align: left; font-family: Bahnschrift, sans-serif; padding: 2px;}}
            body, p, h3, div, span, var {{font-family: Bahnschrift, sans-serif;}}
            td {{text-align: left; font-family: Bahnschrift, sans-serif; font-size:11px; padding: 4px;}}
            h4 {{font-size: 14px; font-family: Bahnschrift, sans-serif;}}
        </style>
        </head>
        <body>
        <b>Hi {branch_manager},</b> <br>
        <p>Kindly comment on the following.</p>
    """
    html_end_template = """
        </body>
        </html>
    """
    
    html_content = ""
    subject_parts = []
    counter = 1
    
    for df_key, df in dataframe_dict.items():
        branch_data = df[df["Outlet"] == branch]
        branch_data = branch_data.drop(columns = ["Outlet"])

        cols = branch_data.columns
        columns_to_change = [
            "Preauth to Upload",
            "Draft to Upload In Mins",
            "Upload to Sent Pre-Auth In Mins",
            "Total Time (Target = 13 Mins)",
            "Preauth to Upload",
            "Draft to Preauth"
        ]


        for column in columns_to_change:
            if column in cols and df_key == "Delayed Orders from Upload Attachment to Sent - Preauth":
                branch_data[column] = branch_data[column].apply(
            lambda x: str(int(x)) if pd.notnull(x) else x
        )

        if df_key =="Time from Eye Test Completed to Draft Order Created":
            branch_data = branch_data[[
                "Customer Code",
                "Order Creator",
                "Order Type",
                "ET Completed Time",
                "Order Date",
                "Time Taken",
            ]].rename(columns = {"Time Taken": "TAT (Target = 45mins)"})

        elif df_key == "Insurance Clients not Submitted":
            branch_data = branch_data[planos_cols]

        elif df_key == "Insurance Orders With No Feedback":
            branch_data = branch_data[[
                "Order Number",
                "Order Creator",
                "Insurance Company",
                "Request Date"
            ]]

        elif df_key == "Insurance Errors":
            branch_data = branch_data[[
                "Order Number",
                "Order Creator",
                "Created User",
                "Remarks"
            ]]

            branch_data["Who Made the Error?"] = ""

        branch_data["Branch Remark"] = ""
        
        if not branch_data.empty:
            subject_parts.append(df_key)
            html_content += f"<b>{counter}. {df_key}</b>"
            html_content += "<br>"
            html_content += "<br>"
            html_content += f"<table>{branch_data.style.hide_index().set_table_styles(styles).to_html()}</table>"
            html_content = html_content.replace('<table ', '<table style="border-collapse: collapse; border: 1px solid black; padding: .4em; font-size: 8.5; text-align: center;" ')
            html_content = html_content.replace('<td ', '<td style="font-size: 8.5pt; text-align: left; padding: .4em; border: 1px solid black;" ')
            html_content = html_content.replace('<th ', '<th style = "font-size: 9.2pt; background-color: #87CEEB; text-align: center; padding: .6em; border: 1px solid black;"')
    
            search_text = "Not Contacted"
            style = 'style="background-color: red; color: black; text-align: left"'

            indices = [pos for pos, char in enumerate(html_content) if html_content[pos:pos + len(search_text)] == search_text]

            for index in indices:
                start_td = html_content.rfind("<td", 0, index)
                end_td = html_content.find("</td>", index) + len("</td>")

                html_content = html_content[:start_td] + f'<td {style} ' + html_content[start_td + len("<td"):end_td] + html_content[end_td:]
            
            html_content += "<br>"
            html_content += "<br>"
            counter += 1
    
    if not subject_parts:
        return None, None
    
    subject = f"{branch} Response Required: Daily Insurance Checks for {pd.to_datetime(date).strftime('%B %d, %Y')}"
    # subject = branch + " Response Required - " + ", ".join(subject_parts[:-1]) + " and " + subject_parts[-1] + " for " + str(date)  if len(subject_parts) > 1 else branch + " Response Required - " + subject_parts[0] + " for " + str(date)
    return html_start_template + html_content + html_end_template, subject


