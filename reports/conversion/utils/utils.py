from airflow.models import variable
import pandas as pd
import datetime
from sub_tasks.libraries.utils import (
    arrange_dateranges,
    first_week_start,
    fourth_week_end,
    get_comparison_months
)

first_month, second_month = get_comparison_months()


def get_conversion_frequency(report) -> str:
    today = datetime.date.today()
    if today.day == 1 and report == "Insurance Conversion":  
        return "Monthly"
    if today.day == 8 and report == "Conversion":
        return "Monthly"
    else:
        return "Weekly"


def return_conversion_daterange(selection):
    today = datetime.date.today()
    if selection == "Weekly":
        start_date = first_week_start
        end_date = fourth_week_end
        return start_date, end_date

    elif selection == "Monthly":
        today = datetime.date.today()
        if today.month <= 2:
            target_month = today.month + 10
            target_year = today.year - 1
        else:
            target_month = today.month - 2
            target_year = today.year
        start_date = datetime.date(
            target_year, target_month, 1
        ).strftime("%Y-%m-%d")

        first_day_of_current_month = today.replace(day=1)
        end_date = first_day_of_current_month - datetime.timedelta(days=1)
        return start_date, end_date

    else:
        return today, today


def manipulate_multiindex(dataframe, name, col1, col2, rename):
    stacked_dataframe = dataframe.stack()
    stacked_dataframe[name] = round(
        (stacked_dataframe[col1] / stacked_dataframe[col2]) * 100, 1).fillna(0).astype(str) + "%"
    unstacked_dataframe = stacked_dataframe.unstack()
    swapped_dataframe = unstacked_dataframe.swaplevel(0, 1, 1).sort_index(
        level=1, axis=1).reindex([col2, name], axis=1, level=1)

    sorted_columns = arrange_dateranges(swapped_dataframe)
    final_dataframe = swapped_dataframe.reindex(
        sorted_columns, axis=1, level=0)
    final_dataframe = final_dataframe.reindex([col2, name], level=1, axis=1)
    final_dataframe = final_dataframe.rename(columns={col2: rename}, level=1)

    return final_dataframe


def create_weeky_branch_conversion(conversions, index, week_range, values, cols):
    weekly_branches_conversion = pd.pivot_table(
        conversions,
        index=index,
        columns=week_range,
        values=[values[0], values[1]],
        aggfunc={values[0]: "count", values[1]: "sum"}
    ).fillna(0)

    return manipulate_multiindex(
        weekly_branches_conversion,
        cols[0],
        cols[1],
        cols[2],
        cols[3]
    )


def create_country_conversion(conversions, week_range, values, country, cols):
    conversions["Country"] = country
    summary_weekly_conversion = pd.pivot_table(
        conversions,
        index="Country",
        columns=week_range,
        values=[values[0], values[1]],
        aggfunc={values[0]: "count", values[1]: "sum"}
    )

    return manipulate_multiindex(
        summary_weekly_conversion,
        cols[0],
        cols[1],
        cols[2],
        cols[3]
    )


def create_branch_conversion(weekly_data, index, values, rename, cols_order):
    branch_conversion = pd.pivot_table(
        weekly_data,
        index=index,
        values=[values[0], values[1]],
        aggfunc={values[0]: "count", values[1]: "sum"}
    ).reset_index()

    branch_conversion["%Conversion"] = round(
        branch_conversion[values[1]] /
        branch_conversion[values[0]] * 100, 0
    ).fillna(0).astype(int).astype(str) + "%"

    return branch_conversion.rename(columns=rename)[cols_order]


def create_staff_conversion(weekly_data, index, values, rename, cols_order):
    staff_conversion = pd.pivot_table(
        weekly_data,
        index=index,
        values=[values[0], values[1]],
        aggfunc={values[0]: "count", values[1]: "sum"}
    ).reset_index()

    staff_conversion["%Conversion"] = round(
        staff_conversion[values[1]] /
        staff_conversion[values[0]] * 100, 0
    ).astype(int).astype(str) + "%"

    return staff_conversion.rename(columns=rename)[cols_order]


def create_monthly_summary(data, values, rename, country):
    data["Country"] = country
    country = pd.pivot_table(
        data,
        index="Country",
        columns="Month",
        values=values,
        aggfunc={values[0]: "count", values[1]: "sum"}
    )

    country_stack = country.stack()
    country_stack["%Conversion"] = round(
        country_stack[values[1]] / country_stack[values[0]] * 100, 0).astype(int).astype(str) + "%"
    country_unstack = country_stack.unstack()
    country_unstack = country_unstack.swaplevel(0, 1, 1).reindex(
        [first_month, second_month], level=0, axis=1)
    country_unstack = country_unstack.reindex(
        [values[0], values[1], "%Conversion"], level=1, axis=1)
    country_unstack = country_unstack.rename(columns=rename, level=1)

    return country_unstack


def create_monthly_conversion(data, index, values, rename):
    month_pivot = pd.pivot_table(
        data,
        index=index,
        columns="Month",
        values=values,
        aggfunc={values[0]: "count", values[1]: "sum"}
    )

    month_pivot_stack = month_pivot.stack()
    month_pivot_stack["%Conversion"] = round(
        month_pivot_stack[values[1]] / month_pivot_stack[values[0]] * 100, 0
    ).astype(int).astype(str) + "%"

    month_unstack = month_pivot_stack.unstack()
    month_unstack = month_unstack.swaplevel(0, 1, 1).reindex(
        [first_month, second_month], level=0, axis=1)
    month_unstack = month_unstack.reindex(
        [values[0], values[1], "%Conversion"], level=1, axis=1)
    month_unstack = month_unstack.rename(columns=rename, level=1)
    return month_unstack

from sub_tasks.libraries.styles import ug_styles

def style_dataframe(dataframe):
    return (dataframe.iloc[:, 1:]).style.hide_index().set_table_styles(ug_styles).to_html(doctype_html=True)


def generate_html(object, branch, rm):
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

        .content {{
            margin-top: 20px !important;
            border: none;
            background-color: white;
            padding: 4%;
            width: 85%;
            margin: 0 auto;
            border-radius: 3px;
        }}

        .salutation {{
            width: 20%;
            margin: 0 auto;
            margin-top: 20px;
            font-size: 10px;
        }}

        .division-line {{
            display: inline-block;
            border-top: 1px solid black;
            margin: 0 5px;
            padding: 0 2px;
            vertical-align: middle;
        }}

    </style>
    </head>
    <body style = "background-color: #F0F0F0; padding: 2% 2% 2% 2%;">
    <div class = "content">
    <b>Hi {rm},</b> <br>
    <p>
    Please review the conversion figures for {branch} for the specified period. <br>
    Please check if any salesperson(s) or optometrist(s) are below the target and advise them on how they can improve.
    </p>
    """

    html_end_template = """
        </div>
        </body>
        </html>
    """

    html_content = html_start_template
    counter = 0  # Initialize counter outside the loop

    for key, value in object.items():
        target = value.get("Target", 0)
        branch_report = value.get("Branch", pd.DataFrame())
        prev_branch = value.get("PreviousWeekBranch", pd.DataFrame())[["Outlet", "%Conversion"]].rename(columns = {"%Conversion": "Previous Week %Conversion"})
        branch_report_index = branch_report.set_index("Outlet")
        branch_report = pd.merge(branch_report, prev_branch, on = ["Outlet"], how = "left")

        sales_person_report = value.get("SalesPerson", pd.DataFrame())
        prev_sales = value.get("PreviousWeekSales", pd.DataFrame())[["Outlet", "Staff", "%Conversion"]].rename(columns = {"%Conversion": "Previous Week %Conversion"}) 

        optom_report = value.get("Optom", pd.DataFrame())
        
        target_met = {}

        sales_person_failed = sales_person_report[pd.to_numeric(sales_person_report['%Conversion'].str.rstrip('%')) < target]
        sales_person_failed = pd.merge(sales_person_failed, prev_sales, on = ["Outlet", "Staff"])
        sales_person_html = style_dataframe(sales_person_failed)

        if key == "Registration":
            optom_failed = pd.DataFrame()
        else:
            prev_optom = value.get("PreviousWeekOptom", pd.DataFrame())[["Outlet", "Optom", "%Conversion"]].rename(columns = {"%Conversion": "Previous Week %Conversion"}) 
            optom_failed = optom_report[pd.to_numeric(optom_report['%Conversion'].str.rstrip('%')) < target]
            optom_failed = pd.merge(optom_failed, prev_optom, on = ["Outlet", "Optom"], how = "left")
            optom_html = style_dataframe(optom_failed)

        branch_html = style_dataframe(branch_report)
        # Check if conversion meets target
        branch_conversion = branch_report_index.loc[branch, "%Conversion"].split("%")[0]
        if int(branch_conversion) >= target:
           if not len(sales_person_failed) and not len(optom_failed):
                counter += 1  # Increment counter only when a report is generated
                html_content += f"""
                <h4>{counter}. {key} Conversion (Target = {target})</h4>
                <p style="color: green;">Your {key} conversion is {branch_conversion}%. Congratulations! Target Met.👏</p>
                """

                target_met[key] = True
           else:
                target_met[key] = False
                counter += 1  # Increment counter only when a report is generated
                html_content += f"""
                <h4>{counter}. {key} Conversion (Target = {target})</h4>
                <ol style="list-style-type: lower-roman;">
                <li><b>Branch Conversion</b>
                <table>{branch_html}</table> <br>
                </li>"""

                if len(sales_person_failed):
                    html_content += f"""
                    <li><b>Conversion by EWC Handover.</b>
                    <p>Below Sales Person(s) have not achieved their target:</p>
                    <table>{sales_person_html}</table> <br>
                    </li>"""
                if key != "Registration" and len(optom_failed):
                    html_content += f"""
                    <li><b>Conversion by Optometrist.</b>
                    <p>Below Optoms have not achieved their target:</p>
                    <table>{optom_html}</table> <br>
                    </li>"""

                html_content += "</ol>"
        else:
            target_met[key] = False
            # Include Optom Conversion only if not Registration
            if key != "Registration":
                counter += 1  # Increment counter only when a report is generated
                html_content += f"""
                <h4>{counter}. {key} Conversion (Target = {target})</h4>
                <ol style="list-style-type: lower-roman;">
                    <li><b>Branch Conversion</b>
                    <table>{branch_html}</table> <br>
                    </li>
                    <li><b>Conversion by Optometrist.</b>
                    <p>Below Optoms have not achieved their target:</p>
                    <table>{optom_html}</table> <br>
                    </li>
                    <li><b>Conversion by EWC Handover.</b>
                    <p>Below Sales Person have not achieved their target:</p>
                    <table>{sales_person_html}</table> <br>
                    </li>
                </ol>
                """
            else:
                counter += 1  # Increment counter only when a report is generated
                html_content += f"""
                <h4>{counter}. {key} Conversion (Target = {target})</h4>
                <ol style="list-style-type: lower-roman;">
                    <li><b>Branch Conversion</b>
                    <table>{branch_html}</table> <br>
                    </li>
                    <li><b>Conversion by Sales Person.</b>
                    <p>Below Sales Person(s) have not achieved their target:</p>
                    <table>{sales_person_html}</table> <br>
                </ol>
                """
    target_met_truth = all(value for value in target_met.values())
    html_content += html_end_template
    return html_content, target_met_truth
