import pandas as pd
import numpy as np
import pygsheets
from airflow.models import variable
from reports.draft_to_upload.utils.utils import today
from sub_tasks.libraries.utils import(
    service_file, 
    get_yesterday_date, 
    get_rm_srm_total, 
    check_date_range, 
    arrange_dateranges,
    get_rm_srm_total_multiindex,
    get_comparison_months,
    return_four_week_range
)

first_month, second_month = get_comparison_months()

checked_sops = [
    'Photos-iport not Used',
    'Tablet/iport',
    'Tablet/iport not Used',
    'Lens Chart not Shown',
    'Lens Price List not Shown',
    'Late Shop Opening',
    'Late Shop Opening~Main Door',
    'Merchandise Cleaning',
    'Merchandise Cleaning~1st 30 mins',
    'Meet & Greet',
    'Early Closure~Main Door',
    'Early Closure',
    'Dispensing Trays',
    'FR Selection',
    'Demo Kit not Used'
]

sop_date = pd.to_datetime(
    get_yesterday_date(truth=True), format="%Y-%m-%d"
).date()

"""
:::::::::::::::::::::::::::::::
:::::::::::::::::::::::::::::::
This is the SOP (Standard Operating Procedure) Compliance Report. 
The purpose of this report is to provide detailed information regarding non-compliances 
and the number of unique customers served in each branch within a specific date range (daily, weekly, or monthly).

To access the daily records for SOPs, please follow this link to the corresponding Google Sheet:
    Link to Google Sheet:::::::   
    https://docs.google.com/spreadsheets/d/1fn8yaI3-1X6uq4Z_Vydxq3vl3F5jLryp09E6po_TJAI/

The report generation logic is as follows:

1. Non-Compliances Count:
    For each branch, we track the number of non-compliances recorded. This count can be obtained on a daily, weekly, or monthly basis.

2. Number of Unique Customers Served:
    We also determine the number of unique customers served in each branch within the specified date range. 
    To calculate this, we extract data from three datasets: registrations, eye tests, and orders. 
    From these datasets, we extract the Outlet, Customer Code, and Date columns. 
    We then remove duplicate entries based on these three columns.

3. Calculation of Non-Compliance Percentage:
    After obtaining the count of unique customers, we calculate the percentage of SOPs (Standard Operating Procedures) to unique customers. 
    This provides insights into the percentage of customers where non-compliance occurred.

To generate this report for any country, please make use of the create_ug_sops_report() function with the following arguments:
    1. selection: Specifies the time period for the report (weekly, monthly, or daily).
    2. branch_data: Contains information for each branch, including the branch code, Regional Manager, and Senior Regional Manager.
    3. sops_info: Includes branch information related to SOPs.
    4. start_date: Indicates the starting date for pulling data from orders, registrations, and eye tests.
    5. all_orders: Represents orders made from start_date to the previous day.
    6. registrations: Contains registration data for the specified date range.
    7. eye_tests: Includes eye test information for the specified date range.
    8. path: Specifies the path where you want to save the report.

Documentation v0.01
Written and Curated by Douglas
::::::::::::::::::::::::::::::::::::::::::::::::::
::::::::::::::::::::::::::::::::::::::::::::::::::
"""


def create_ug_sops_report(
    selection: str, 
    branch_data: pd.DataFrame, 
    sops_info: pd.DataFrame, 
    start_date: str, 
    customers: pd.DataFrame, 
    path: str
) -> None:
    customers = customers[
        (customers["Date"] >= start_date) &
        (customers["Date"] <= sop_date)
    ]
    service_key = pygsheets.authorize(service_file=service_file)
    sheet = service_key.open_by_key('1fn8yaI3-1X6uq4Z_Vydxq3vl3F5jLryp09E6po_TJAI')
    sop_compliance = pd.DataFrame(sheet.worksheet_by_title("Main Sheet").get_all_records())
    sop_compliance = sop_compliance[["Date", "Branch", "RM Name", "SRM Name", "SOP", "No of times"]].replace("", 1).fillna(1).copy()
    sop_compliance = sop_compliance[sop_compliance.SOP.isin(checked_sops)]
    sop_compliance = sop_compliance.dropna(subset=["Date"])
    sop_compliance["Date"] = pd.to_datetime(sop_compliance["Date"], dayfirst=True, errors="coerce").dt.date
    sop_compliance = pd.merge(sop_compliance, sops_info[["Branch", "Outlet"]], on="Branch", how="left")
    sop_compliance = sop_compliance[sop_compliance["Outlet"].str.strip().isin(branch_data["Outlet"])]


    if not len(sop_compliance):
        return

    if selection == "Daily":
        daily_sops = sop_compliance[sop_compliance["Date"] == sop_date].copy()
        
        if not len(daily_sops):
            return
        
        daily_sop_pivot = pd.pivot_table(
            daily_sops, index=["Branch"], values="No of times", aggfunc="sum").reset_index()
        daily_sop_pivot_merge = pd.merge(
            daily_sop_pivot, sops_info[["Branch", "Outlet"]], on="Branch", how="left")

        daily_customers_pivot = pd.pivot_table(
            customers,
            index="Outlet",
            values="Customer Code",
            aggfunc="sum").reset_index()
        unique_customers_daily_sops = pd.merge(
            daily_sop_pivot_merge,
            daily_customers_pivot,
            on="Outlet",
            how="right"
        )[["Outlet", "No of times", "Customer Code"]].merge(
            branch_data[["Outlet", "RM", "SRM"]], 
            on="Outlet", 
            how="right"
        ).fillna(0)[["Outlet", "RM", "SRM", "Customer Code", "No of times"]].rename(
            columns={
                "Customer Code": "Customers", "No of times": "SOP Count"
            }
        )

        unique_customers_daily_sops["% SOP/Customers"] = round(
            (unique_customers_daily_sops["SOP Count"] /
             unique_customers_daily_sops["Customers"]) * 100, 0
        ).fillna(0).astype(int)

        final_daily_sops_report = get_rm_srm_total(
            unique_customers_daily_sops,
            x=["% SOP/Customers"],
            y=["SOP Count"],
            z=["Customers"],
            has_perc=True
        )

        with pd.ExcelWriter(f"{path}draft_upload/sop_compliance.xlsx") as writer:
            final_daily_sops_report.to_excel(
                writer, sheet_name="daily_summary", index=False)
            daily_sops.iloc[:, :-1].to_excel(writer,sheet_name="daily_sops_data", index=False)

    if selection == "Weekly":
        weekly_sops = sop_compliance[
            (sop_compliance["Date"] >= start_date) &
            (sop_compliance["Date"] <= sop_date)
        ].copy()

        weekly_customers = customers.copy()
        weekly_customers["Week Range"] = weekly_customers.apply(lambda row: check_date_range(row, "Date"), axis=1)
        weekly_customers = weekly_customers[weekly_customers["Week Range"] != "None"]
        weekly_customers = pd.merge(
            weekly_customers,
            branch_data[["Outlet", "RM", "SRM"]],
            on = "Outlet",
            how = "left"
        )

        weekly_customers_pivot = pd.pivot_table(
            weekly_customers,index=["Outlet", "RM", "SRM"],
            columns="Week Range",
            aggfunc="sum",
            values="Customer Code"
        )

        weekly_sops["Week Range"] = weekly_sops.apply(lambda row: check_date_range(row, "Date"), axis=1)
        weekly_sops["No of times"] = weekly_sops["No of times"].astype(int)
        weekly_sops = weekly_sops[weekly_sops["Week Range"] != "None"]

        weekly_sops = pd.merge(
            weekly_sops,
            branch_data[["Outlet", "RM", "SRM"]],
            on = "Outlet",
            how = "left"
        )

        weekly_sops_pivot = pd.pivot_table(weekly_sops, 
            index=["Outlet", "RM", "SRM"], 
            columns="Week Range", 
            values="No of times", 
            aggfunc="sum"
        ).fillna(0)

        weekly_customers_pivot.columns = pd.MultiIndex.from_product([["Customers"], weekly_customers_pivot.columns])
        weekly_sops_pivot.columns = pd.MultiIndex.from_product([["Sops"], weekly_sops_pivot.columns])
        final_weeky_sops = pd.merge(weekly_sops_pivot, weekly_customers_pivot, right_index=True, left_index=True).fillna(0)
        final_weeky_sops = final_weeky_sops.swaplevel(0, 1, axis=1).sort_index(axis=1, level=0)
        final_weeky_sops = final_weeky_sops.reindex(["Customers", "Sops"], axis = 1, level = 1)
        sort_columns = return_four_week_range()

        colst = [
            (sort_columns[0], "Customers"),
            (sort_columns[0], "Sops"),
            (sort_columns[1], "Customers"),
            (sort_columns[1], "Sops"),
            (sort_columns[2], "Customers"),
            (sort_columns[2], "Sops"),
            (sort_columns[3], "Customers"),
            (sort_columns[3], "Sops"),
        ]
        
        final_weeky_sops = final_weeky_sops.reindex(colst, axis =1, fill_value=0)
        for date in final_weeky_sops.columns.levels[0]:
            col_name = (date, '%Sops/Customers')
            final_weeky_sops.insert(
                final_weeky_sops.columns.get_loc((date, 'Sops')) + 1, col_name,
                (final_weeky_sops[(date, 'Sops')] / final_weeky_sops[(date, 'Customers')] * 100).fillna(0).round().astype(int)
            )

        sorted_columns = arrange_dateranges(final_weeky_sops)
        final_weeky_sops_report = final_weeky_sops.reindex(sorted_columns,axis = 1, level = 0)
        weekly_sops_export_data = weekly_sops#[weekly_sops["Week Range"] == final_weeky_sops.columns.get_level_values(0).values[-1]]

        final_weeky_sops_report = get_rm_srm_total_multiindex(
            final_weeky_sops_report,
            week_month="Week",
            a = "%Sops/Customers",
            b = "Sops",
            c = "Customers",
            report = final_weeky_sops_report
        )

        column_sop = pd.pivot_table(
            weekly_sops,
            index="Outlet",
            columns=["Week Range","SOP"],
            values = "No of times",
            aggfunc="sum"
        ).fillna(0).reindex(sorted_columns, level = 0, axis = 1)

        with pd.ExcelWriter(f"{path}draft_upload/sop_compliance.xlsx") as writer:
            final_weeky_sops_report.to_excel(writer, sheet_name="weekly_summary")
            weekly_sops_export_data.iloc[:, :-2].to_excel(writer, sheet_name="weekly_sops_data", index=False)
            column_sop.to_excel(writer, sheet_name="sop_wise")

    if selection == "Monthly":
        monthly_sops = sop_compliance.copy()
        monthly_sops["Month"] = pd.to_datetime(monthly_sops["Date"], dayfirst="%Y-%m-%d").dt.month_name()
        monthly_sops_data = monthly_sops[(monthly_sops["Month"] == first_month) | (monthly_sops["Month"] == second_month)].copy()
        monthly_sops_data["RM Name"] = monthly_sops_data["RM Name"].str.replace("Mayanne", "Maryanne")

        monthly_customers = customers.copy()
        monthly_customers["Month"] = pd.to_datetime(monthly_customers["Date"], format="%Y-%m-%d").dt.month_name()
        monthly_customers = monthly_customers[
            (monthly_customers["Month"] == first_month) | 
            (monthly_customers["Month"] == second_month)
        ]
        monthly_customers = pd.merge(
            monthly_customers,
            branch_data[["Outlet", "RM", "SRM"]],
            on = "Outlet",
            how = "left"
        )

        monthly_customers_pivot = pd.pivot_table(
            monthly_customers,
            index=["Outlet", "RM", "SRM"],
            columns="Month",
            aggfunc="sum",
            values="Customer Code"
        )

        monthly_sops_pivot = pd.pivot_table(
            monthly_sops_data.rename(columns = {"RM Name": "RM", "SRM Name": "SRM"}),
            index=["Outlet", "RM", "SRM"],
            values="No of times",
            columns="Month",
            aggfunc="sum"           
        )

        column_sop = pd.pivot_table(
            monthly_sops_data,
            index="Outlet",
            columns=["Month","SOP"],
            values = "No of times",
            aggfunc="sum"
        ).fillna(0).reindex([first_month, second_month], level = 0, axis = 1)

        monthly_customers_pivot.columns = pd.MultiIndex.from_product([["Customers"], monthly_customers_pivot.columns])
        monthly_sops_pivot.columns = pd.MultiIndex.from_product([["Sops"], monthly_sops_pivot.columns])
        final_monthly_sops = pd.merge(monthly_sops_pivot, monthly_customers_pivot, right_index=True, left_index=True).fillna(0)
        final_monthly_sops = final_monthly_sops.swaplevel(0, 1, axis=1).sort_index(axis=1, level=0)
        final_monthly_sops = final_monthly_sops.reindex(["Customers", "Sops"], axis = 1, level = 1)
        final_monthly_sops = final_monthly_sops.reindex([first_month, second_month], level=0, axis=1)

        for month in final_monthly_sops.columns.levels[0]:
            col_name = (month, '%Sops/Customers')
            final_monthly_sops.insert(
                final_monthly_sops.columns.get_loc((month, 'Sops')) + 1, col_name,
                (final_monthly_sops[(month, 'Sops')] / final_monthly_sops[(month, 'Customers')] * 100).round().astype(int)
            )

        # save_dataframes_to_excel(
        #     path=f"{path}draft_upload/sop_compliance.xlsx",
        #     dataframes=[final_monthly_sops, monthly_sops_data],
        #     sheets=["monthly_summary", "monthly_sops_data"],
        #     multindex=[True, False]
        # )

        

        with pd.ExcelWriter(f"{path}draft_upload/sop_compliance.xlsx") as writer:
            final_monthly_sops.to_excel(writer, sheet_name="monthly_summary")
            column_sop.to_excel(writer, sheet_name = "sop_wise")
            monthly_sops_data.iloc[:, :-2].to_excel(writer, sheet_name = "monthly_sops_data", index = False)



        
