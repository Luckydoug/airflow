import numpy as np
import pandas as pd
from airflow.models import variable
from sub_tasks.libraries.time_diff import calculate_time_difference
from reports.draft_to_upload.utils.utils import today, get_comparison_months
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

first_month, second_month = get_comparison_months()

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


def create_draft_upload_report(selection, orderscreen, all_orders, start_date, target, branch_data, path,working_hours, drop=''):
    if not len(all_orders) or not len(orderscreen):
        return

    draft = ["Draft Order Created"]
    drafts = orderscreen[orderscreen.Status.isin(draft)].copy()
    draft_sorted = drafts.sort_values(by=["Date", "Time"], ascending=True)
    unique_draft = draft_sorted.drop_duplicates(
        subset="Order Number", keep="first").copy()
    unique_draft.loc[:, "Draft Time"] = pd.to_datetime(
        unique_draft["Date"].astype(str) + " " +
        unique_draft["Time"].astype(str),
        format="%Y-%m-%d %H:%M:%S"
    )

    upload = ["Upload Attachment"]
    upload_attachments = orderscreen[orderscreen.Status.isin(upload)].copy()
    upload_sorted = upload_attachments.sort_values(
        by=["Date", "Time"], ascending=True)
    unique_uploads = upload_sorted.drop_duplicates(
        subset="Order Number", keep="first").copy()
    unique_uploads.loc[:, "Upload Time"] = pd.to_datetime(
        unique_uploads["Date"].astype(str) + " " +
        unique_uploads["Time"].astype(str),
        format="%Y-%m-%d %H:%M:%S"
    )

    initiated = [
        "Pre-Auth Initiated For Optica Insurance",
        "Insurance Order Initiated", 
        "SMART to be captured"
    ]
    preauth_initiated = orderscreen[orderscreen.Status.isin(initiated)].copy()
    preauth_sorted = preauth_initiated.sort_values(
        by=["Date", "Time"], ascending=True)
    unique_preauths = preauth_sorted.drop_duplicates(
        subset="Order Number", keep="first").copy()
    unique_preauths.loc[:, "Preauth Time"] = pd.to_datetime(
        unique_preauths["Date"].astype(str) + " "
        + unique_preauths["Time"].astype(str),
        format="%Y-%m-%d %H:%M:%S"
    )

    final_data1 = pd.merge(
        unique_uploads[["Order Number", "Upload Time"]],
        unique_preauths[["Order Number", "Preauth Time"]],
        on="Order Number",
        how="left"
    )
    final_data = pd.merge(
        unique_draft[["Order Number", "Draft Time"]],
        final_data1, on="Order Number",
        how="right"
    )

    final_data_orders = pd.merge(
        final_data,
        all_orders[[
            "DocNum", "Code", "Last View Date",
            "Status", "Outlet", "Insurance Company",
            "Insurance Scheme", "Scheme Type",
            "Feedback 1", "Feedback 2",
            "Creator", "Order Creator"
        ]].rename(columns={"DocNum": "Order Number"}),
        on="Order Number",
        how="left"
    )

    final_data_orders["Month"] = final_data_orders["Preauth Time"].dt.month_name()
    final_data_orders = final_data_orders[
        (final_data_orders["Upload Time"] >= pd.to_datetime(start_date)) &
        (final_data_orders["Upload Time"] <= pd.to_datetime(today))
    ]

    final_data_orders = final_data_orders.dropna(subset=["Draft Time"])
    final_data_orders = final_data_orders.dropna(subset=["Outlet"])
    final_data_orders = final_data_orders[final_data_orders["Insurance Company"] != drop]
    final_data_orders = final_data_orders[final_data_orders["Outlet"] != "MUR"]

    if not len(final_data_orders):
        return

    final_data_orders["Draft to Preauth"] = final_data_orders.apply(
        lambda row: calculate_time_difference(
            row=row,
            x="Draft Time",
            y="Preauth Time",
            working_hours=working_hours
        ),
        axis=1
    )

    final_data_orders["Preuth to Upload"] = final_data_orders.apply(
        lambda row: calculate_time_difference(
            row=row,
            x="Preauth Time",
            y="Upload Time",
            working_hours=working_hours
        ),
        axis=1
    )

    final_data_orders["Draft to Upload"] = final_data_orders.apply(
        lambda row: calculate_time_difference(
            row=row,
            x="Draft Time",
            y="Upload Time",
            working_hours=working_hours
        ),
        axis=1
    )

    final_data_orders = pd.merge(
        final_data_orders,
        branch_data[["Outlet", "RM", "SRM", "Front Desk"]],
        on = "Outlet",
        how = "left"
    )

    final_data_orders["Slade"] = final_data_orders.apply(lambda row: return_slade(row), axis=1)
    final_data_orders = final_data_orders.drop_duplicates(
        subset=["Order Number"]
    )

    cols_req = [
        "SRM",
        "RM",
        "Order Number",
        "Outlet",
        "Front Desk",
        "Creator",
        "Order Creator",
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
    

    """
    This is the Daily Report. 
    Since this code will be running on Airflow, we can't afford to enter the dates manually. 
    There are three reports available: Daily, Weekly, and Monthly reports. 
    Depending on the day, we want the code to determine whether it should send the daily, weekly or monthly report.

    """

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
                "Shop's Overall Avg. (Draft - Upload)", "Total Time", "Total Orders"]
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

        create_branch_salesperson_efficiency(
            data = daily_data,
            target=target,
            path=path,
            cols_req=sales_cols
        )

        with pd.ExcelWriter(f"{path}draft_upload/draft_to_upload.xlsx") as writer:
            final_daily_report.to_excel(
                writer, sheet_name="daily_summary", index=False)
            daily_data[cols_req].to_excel(
                writer, sheet_name="daily_data", index=False)

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
                writer, sheet_name="weekly_summary", index=False)
            sales_efficiency_data.iloc[:, :-1].to_excel(
                writer, sheet_name="weekly_data", index=False)

    if selection == "Monthly":
        """
        The below code will run when a selection is Monthly
        """
        monthly_data = final_data_orders.copy()
        monthly_data = pd.merge(
            monthly_data,
            branch_data[["Outlet", "RM", "SRM"]],
            on="Outlet", how="left"
        )

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


"""
Another One From Optica Data Team
Where we Build Intelligence

"""
