import numpy as np
import pandas as pd
from airflow.models import variable
from reports.draft_to_upload.utils.utils import get_comparison_months
from sub_tasks.libraries.utils import get_rm_srm_total, check_date_range
from reports.draft_to_upload.reports.branch_salesperson_efficiency import create_branch_salesperson_efficiency

"""

In programming, I personally love modularity. 
We aim to reuse our code. Without modularity, consider the scenario where Optica needs to expand to all countries in Africa. 
We would have to create 54 separate drafts to upload files. 
However, thanks to modularity, we only need to define a single function and call it with the appropriate arguments based on the country. 
This approach enhances code readability and ease of maintenance while saving storage space.

When preparing a report for any country, make sure to call the following function and pass the corresponding parameters:

1. selection: This is an automatic argument obtained from the get_report_frequency() function defined in the utils module.
2. orderscreen: The log for the orderscreen.
3. all_orders: All orders made over a specific date range.
4. start_date: This is also an automatic argument retrieved from the return_report_daterange() function defined in the utils module.
5. drop: In case there are any insurance companies you want to exclude from the data. By default, it is set to empty.
6. target: The target time for generating the draft and uploading the report, expressed in minutes.
7. path: The path where you want the reports to be saved.
8. branch_data: This includes data for the branches in a given country. Ensure that the list contains the Outlet, SRM, and RM columns.

"""

def create_draft_upload_report(data_orders, daywise_data, mtd_data, selection,start_date, target, branch_data, path, orders_drop = [], drop = ""):
    first_month, second_month = get_comparison_months()
    cols_req = [
        "Date",
        "SRM",
        "RM",
        "Order Number",
        "Outlet",
        "Front Desk",
        "Creator",
        "Order Creator",
        "Draft Time",
        "Preauth Time",
        "Upload Time",
        "Draft to Preauth",
        "Preuth to Upload",
        "Draft to Upload",
        "Insurance Company",
        "Slade",
        "Scheme Type",
        "Feedback 1"
    ]

    sales_cols = [
        "Order Number",
        "Outlet",
        "Front Desk",
        "Creator",
        "Order Creator",
        "Draft Time",
        "Preauth Time",
        "Upload Time",
        "Draft to Preauth",
        "Preuth to Upload",
        "Draft to Upload",
        "Insurance Company",
        "Slade",
        "Scheme Type",
        "Feedback 1"
    ]

    data_orders = data_orders[~data_orders["Order Number"].isin(orders_drop)]
    final_data_orders = data_orders.copy()
    final_data_orders = pd.merge(
        final_data_orders,
        branch_data[["Outlet", "RM", "SRM"]],
        on = "Outlet",
        how = "left"
    )
    final_data_orders["Date"] = final_data_orders["Upload Time"].dt.date
    final_data_orders = final_data_orders[final_data_orders["Insurance Company"] != drop]
    final_data_orders = final_data_orders.drop_duplicates(subset=["Order Number"])

    daywise_pivot = pd.pivot_table(
        daywise_data,
        index="Outlet",
        columns="Date",
        values="Efficiency",
        aggfunc="sum"
    ).fillna("-")

    mtd_daywise_pivot = pd.merge(
        daywise_pivot,
        mtd_data,
        on = "Outlet",
        how = "inner"
    )

    if selection == "Daily":
        daily_data = final_data_orders[final_data_orders["Upload Time"].dt.date == start_date]
        if not(len(daily_data)):
            return
        daily_pivot_report = pd.pivot_table(
            daily_data,
            index="Outlet",
            values=["Preauth Time", "Upload Time", "Draft to Upload"],
            aggfunc={
                "Preauth Time": pd.Series.count,
                "Upload Time": pd.Series.count,
                "Draft to Upload": ["mean", lambda x: (x <= target).sum(), lambda x: (x > target).sum(), "sum"]
            }
        )

        daily_pivot_report.columns = [
            "_".join(col).strip() for col in daily_pivot_report.columns.values]
        daily_pivot_report[f"% Efficiency (Target: {target} mins"] = pd.to_numeric(
            round(daily_pivot_report["Draft to Upload_<lambda_0>"] / daily_pivot_report["Upload Time_count"] * 100, 0), errors="coerce")
        daily_pivot_report.columns = [
            f"Orders < {target} mins (Draft to Upload)",
            f"Orders > {target} mins (Draft to Upload)",
            "Shop's Overall Avg. (Draft - Upload)",
            "Total Time",
            "Preauths",
            "Total Orders",
            f"% Efficiency (Target: {target} mins)"
        ]

        daily_rm_srm = pd.merge(
            daily_pivot_report,
            branch_data[["Outlet", "RM", "SRM"]],
            on="Outlet", how="outer"
        )   .fillna(0)


        column_total = get_rm_srm_total(
            daily_rm_srm,
            x=[f"% Efficiency (Target: {target} mins)"],
            y=[f"Orders < {target} mins (Draft to Upload)"],
            z=["Total Orders"],
            has_perc=True,
            avg_cols=[
                "Shop's Overall Avg. (Draft - Upload)", 
                "Total Time", 
                "Total Orders"
            ]
        )

        final_daily_report = column_total[[
            "Outlet", "RM", "SRM",
            "Shop's Overall Avg. (Draft - Upload)",
            "Total Orders", f"Orders > {target} mins (Draft to Upload)",
            f"Orders < {target} mins (Draft to Upload)",
            "Preauths",
            f"% Efficiency (Target: {target} mins)"
        ]].copy()

        final_daily_report["Shop's Overall Avg. (Draft - Upload)"] = pd.to_numeric(round(
            final_daily_report["Shop's Overall Avg. (Draft - Upload)"], 0), errors="coerce")

        final_daily_report = final_daily_report[[
            "Outlet", "RM", "SRM",
            "Shop's Overall Avg. (Draft - Upload)",
            "Total Orders", f"Orders > {target} mins (Draft to Upload)",
            f"% Efficiency (Target: {target} mins)"
        ]]

        sales_persons = create_branch_salesperson_efficiency(
            data = daily_data,
            target=target,
            path=path,
            cols_req=sales_cols
        )

        late_daily_orders = daily_data[daily_data["Draft to Upload"] > 8]

        with pd.ExcelWriter(f"{path}draft_upload/draft_to_upload.xlsx") as writer:
            final_daily_report.to_excel(
                writer, 
                sheet_name="daily_summary", 
                index=False
            )
            daily_data[cols_req].sort_values(by = ["Outlet", "Draft to Upload"], ascending = False).to_excel(
                writer, 
                sheet_name="daily_data", 
                index=False
            )

            sales_persons.to_excel(
                writer,
                sheet_name = "ewc_summary",
                index = False
            )

            late_daily_orders[cols_req].sort_values(by = ["Outlet", "Draft to Upload"], ascending = False).to_excel(
                writer, 
                sheet_name="late_orders", 
                index=False
            )

            mtd_daywise_pivot.sort_values(by = "Outlet", ascending=True).to_excel(
                writer,
                sheet_name="mtd-update",
                index=False
            )

        """
        The Daily Report ends here
        """

    if selection == "Weekly":
        """
        If the selection is weekly, then run the following code
        This is where also the code is including the sales persons efficiecny base on the target for the country.

        """
        weekly_data = final_data_orders.copy()
        weekly_data["Week Range"] = weekly_data.apply(
            lambda row: check_date_range(row, "Upload Time"), axis=1)
        weekly_data = weekly_data[weekly_data["Week Range"] != "None"]

        weekly_report_pivot = pd.pivot_table(
            weekly_data,
            index="Outlet",
            values=["Preauth Time", "Upload Time", "Draft to Upload"],
            columns="Week Range",
            aggfunc={
                "Upload Time": pd.Series.count,
                "Draft to Upload": [lambda x: (x <= target).sum()]
            }
        )

        stack = weekly_report_pivot.stack()
        stack["Efficiency"] = pd.to_numeric(round(
            (stack[('Draft to Upload', '<lambda>')] / stack[('Upload Time',    'count')]) * 100, 0), errors="coerce")
        unstacked = stack.unstack()
        unstacked_two = unstacked.swaplevel(0, 1, 1).sort_index(
            level=1).reindex(["Efficiency"], axis=1, level=1)

        stack_columns = unstacked_two.columns
        unique_date_ranges = stack_columns.get_level_values(2).values
        unstacked_two.columns = unique_date_ranges

        dates = []
        for date in unique_date_ranges:
            stat_date = pd.to_datetime(date.split(" to ")[0])
            dates.append(stat_date)
        unique_dates = list(set(dates))
        sorted_dates = sorted(unique_dates)
        sorted_dates

        sorted_columns = []

        for date in sorted_dates:
            date_range = f"{date.strftime('%Y-%b-%d')} to " + \
                f"{(date + pd.Timedelta(6, unit='d')).strftime('%Y-%b-%d')}"
            sorted_columns.append(date_range)

        final_weekly = unstacked_two.reindex(
            sorted_columns, axis=1).reset_index()
        final_weekly = pd.merge(
            branch_data[["Outlet", "RM", "SRM"]],
            final_weekly,
            how="left"
        )

        sales_efficiency_data = weekly_data[
            weekly_data["Week Range"]== final_weekly.columns[-1]
        ]

        create_branch_salesperson_efficiency(
            data = sales_efficiency_data,
            target=target,
            path=path,
            cols_req=sales_cols
        )

        sales_efficiency_data = sales_efficiency_data[cols_req]

        """
        Writing the reports to an Excel File.
        """

        with pd.ExcelWriter(f"{path}draft_upload/draft_to_upload.xlsx") as writer:
            final_weekly.sort_values(by="SRM").to_excel(
                writer, 
                sheet_name="weekly_summary", 
                index=False
            )
            sales_efficiency_data.iloc[:, :-1].to_excel(
                writer, 
                sheet_name="weekly_data", 
                index=False
            )
            mtd_daywise_pivot.sort_values(by = "Outlet", ascending=True).to_excel(
                writer,
                sheet_name="mtd-update",
                index=False
            )

    if selection == "Monthly":
        """
        The below code will run when a selection is Monthly
        """
        monthly_data = final_data_orders.copy()
        monthly_data["Month"] = monthly_data["Upload Time"].dt.month_name()

        monthly_data = monthly_data[
            (monthly_data["Month"] == first_month) | 
            (monthly_data["Month"] == second_month)
        ]

        monthly_report_pivot = pd.pivot_table(monthly_data,
            index=["Outlet", "RM", "SRM"],
            values=["Upload Time",
                    "Draft to Upload"],
            columns="Month",
            aggfunc={
                "Upload Time": pd.Series.count,
                "Draft to Upload": [lambda x: (x <= target).sum()]
            }
        )

        monthly_stack = monthly_report_pivot.stack()
        monthly_stack["Efficiency"] = round((monthly_stack[(
            'Draft to Upload', '<lambda>')] / monthly_stack[('Upload Time',    'count')]) * 100, 0)
        monthly_unstack = monthly_stack.unstack()
        monthly_unstack_two = monthly_unstack.swaplevel(0, 2, 1).droplevel(1, axis = 1)
        monthly_unstack_two = monthly_unstack_two.rename(columns={"Draft to Upload": f"Orders <= {target}", "Upload Time": "Total Orders"})
        final_month_report = monthly_unstack_two.reindex([first_month, second_month], level = 0, axis = 1)
        final_month_report = final_month_report.reindex(["Total Orders", f"Orders <= {target}", "Efficiency"], level = 1, axis = 1)

        create_branch_salesperson_efficiency(
            data = monthly_data,
            target=target,
            path=path,
            cols_req=sales_cols
        )

        monthly_data = monthly_data[cols_req]

        with pd.ExcelWriter(f"{path}draft_upload/draft_to_upload.xlsx") as writer:
            final_month_report.to_excel(
                writer, 
                sheet_name="monthly_summary"
            )
            monthly_data.iloc[:, :-1].to_excel(
                writer,
                sheet_name="monthly_data", 
                index=False
            )
            mtd_daywise_pivot.sort_values(by = "Outlet", ascending=True).to_excel(
                writer,
                sheet_name="mtd-update",
                index=False
            )


"""
Another One From Optica Data Team
Where we Build Intelligence

"""
