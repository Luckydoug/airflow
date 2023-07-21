import pandas as pd
import numpy as np
from airflow.models import variable
from sub_tasks.libraries.utils import (
    get_rm_srm_total, 
    get_rm_srm_total_multiindex, 
    get_comparison_months, 
    return_four_week_range
)
from reports.draft_to_upload.utils.utils import (
    today, 
    create_rejections_ewc,
    create_rejections_branches
)
from sub_tasks.libraries.utils import check_date_range
first_month, second_month = get_comparison_months()

def create_rejection_report(orderscreen, all_orders, sales_orders, branch_data, path, selection, start_date):
    """
    When the data is empty for any of the dataset that is passed as an argument, we want our function to return.
    By returning, we mean we want to prevent further execution of the code without the sufficient data.
    It's just Accuracy emphasize.

    """
    if not len(orderscreen) or not len(all_orders) or not len(sales_orders):
        return
    insurance_status = ["Draft Order Created"]
    insurance_orders = orderscreen[orderscreen.Status.isin(insurance_status)].copy()
    unique_insurance_orders = insurance_orders.drop_duplicates(subset="Order Number")
    unique_insurance_merge = pd.merge(
        unique_insurance_orders, 
        all_orders[[
            "DocNum", "Outlet", "CreateDate", "Insurance Order", "Order Creator"
        ]].rename(columns={"DocNum": "Order Number"}), on = "Order Number", how = "left"
    )
    unique_insurance_merge["Date"] = pd.to_datetime(unique_insurance_merge["Date"], dayfirst=True)
    unique_insurance_merge["CreateDate"] = pd.to_datetime(unique_insurance_merge["CreateDate"], dayfirst=True)
    unique_insurance_merge = unique_insurance_merge[unique_insurance_merge["Insurance Order"] == "Yes"].copy()
    unique_insurance_merge = unique_insurance_merge.drop_duplicates(subset=["Order Number"])

    rejections_status = ["Rejected by Approvals Team", "Rejected by Optica Insurance"]
    rejections = orderscreen[orderscreen.Status.isin(rejections_status)]

    rejections_orders = pd.merge(
        rejections,
        all_orders[[
            "DocNum", "Outlet", 
            "Status", "Creator", 
            "Order Creator", "Insurance Company", "Insurance Scheme", "Scheme Type"
            ]].rename(columns={"DocNum": "Order Number", "Status": "status"}),
        on = "Order Number",
        how = "left"
    )

    rejections_orders = pd.merge(
        rejections_orders, 
        branch_data[["Outlet", "SRM", "RM", "Front Desk"]], 
        on = "Outlet"
    )

    cols_rej = [
        "Date",
        "RM",
        "SRM",
        "Outlet", 
        "Front Desk",
        "Status",
        "Order Number",
        "Creator",
        "Order Creator",
        "Created User", 
        "Insurance Company",
        "Insurance Scheme",
        "Scheme Type", 
        "Remarks"
    ]

    if selection == "Daily":
        daily_insurance_orders = unique_insurance_merge[unique_insurance_merge["CreateDate"].dt.date == start_date]
        daily_insurance_orders_pivot = pd.pivot_table(
            daily_insurance_orders,
            index = "Outlet", 
            aggfunc="count", 
            values="Order Number"
        ).reset_index().rename(columns={"Order Number": "Total Ins Orders"})

        ewc_daily_orders = pd.pivot_table(
            daily_insurance_orders,
            index=["Outlet", "Order Creator"],
            aggfunc="count",
            values="Order Number"
        ).reset_index().rename(columns={"Order Number": "Total Orders"})


        daily_rejections = rejections_orders[rejections_orders["Date"] == start_date]
        if not len(daily_rejections):
            return
        
        daily_unique_rejections = daily_rejections.drop_duplicates(subset=["Order Number", "Date", "Time"])

        daily_rejections_pivot = pd.pivot_table(
            daily_unique_rejections,
            index = "Outlet",
            values="Order Number",
            aggfunc="count"
        ).reset_index().rename(columns={"Order Number": "Count of Rejections"})

        ewc_daily_rejections = pd.pivot_table(
            daily_unique_rejections,
            index=["Order Creator", "Outlet"],
            values="Order Number",
            aggfunc="count"
        ).reset_index().rename(columns={"Order Number": "Count of Rejections"})

        final_ewc_pivot = pd.merge(
            ewc_daily_orders,
            ewc_daily_rejections,
            on = ["Outlet", "Order Creator"],
            how = "outer"
        ).fillna(0)

        final_ewc_pivot["%Rejected"] = round((final_ewc_pivot["Count of Rejections"] / final_ewc_pivot["Total Orders"])* 100, 0).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)
        daily_rejections_branches = pd.merge(
            branch_data[["Outlet", "RM", "SRM"]],
            daily_rejections_pivot,
            on = "Outlet",
            how = "left"
        ).fillna(0)

        daily_rejection_conversion = daily_unique_rejections[
            daily_unique_rejections["Order Number"].isin(sales_orders["Order Number"])
        ]

        daily_conversion_pivot = pd.pivot_table(
            daily_rejection_conversion,
            index=["Outlet"],
            values="Order Number",
            aggfunc="count"
        ).reset_index().rename(columns = {"Order Number": "Converted"})

        categories = ["Outlet", "Converted"]
        daily_conversion_pivot = daily_conversion_pivot.reindex(categories, axis=1, fill_value=0)

        daily_rej_total = pd.merge(
            daily_rejections_branches, 
            daily_insurance_orders_pivot, 
            on = "Outlet", 
            how = "left"
        ).fillna(0)[["Outlet", "RM", "SRM", "Total Ins Orders", "Count of Rejections"]]

        daily_rej_total["% Rejected"] = round((daily_rej_total["Count of Rejections"] / daily_rej_total["Total Ins Orders"]) * 100, 0).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)
        daily_rej_total = pd.merge(daily_rej_total, daily_conversion_pivot, on = "Outlet", how = "left").fillna(0)
        daily_rej_total["%Conversion"] = round((daily_rej_total["Converted"] / daily_rej_total["Count of Rejections"]) * 100, 0).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)

        final_daily_rejections_report = get_rm_srm_total(
            daily_rej_total, 
            x = ["% Rejected", "%Conversion"],
            y = ["Count of Rejections", "Converted"],
            z = ["Total Ins Orders", "Count of Rejections"],
            has_perc=True,  
        )

        rejections_daily_data = daily_unique_rejections[cols_rej] 

        with pd.ExcelWriter(f"{path}draft_upload/rejections_report.xlsx") as writer:
            final_daily_rejections_report.to_excel(writer, sheet_name="daily_summary", index=False)
            rejections_daily_data.to_excel(writer, sheet_name="daily_rejections_data", index=False)
            final_ewc_pivot.to_excel(writer, sheet_name="ewc_summary", index=False)

    if selection == "Weekly":
        weekly_insurance_orders = unique_insurance_merge[
            (unique_insurance_merge["CreateDate"] >= pd.to_datetime(start_date)) &
            (unique_insurance_merge["CreateDate"] <= pd.to_datetime(today))
        ].copy()

        weekly_insurance_orders["Week Range"] = weekly_insurance_orders.apply(lambda row: check_date_range(row, "Date"), axis=1)
        weekly_insurance_orders = weekly_insurance_orders[weekly_insurance_orders["Week Range"] != "None"]

        weekly_insurance_orders = pd.merge(
            weekly_insurance_orders,
            branch_data[["Outlet", "RM", "SRM"]],
            on = "Outlet",
            how = "left"
        )

        weekly_insurance_orders_pivot = pd.pivot_table(
            weekly_insurance_orders,
            index=["Outlet", "RM", "SRM"],
            columns="Week Range",
            values="Order Number",
            aggfunc="count"
        )

        rejections_orders["Date"] = pd.to_datetime(rejections_orders["Date"])
        raw_rejections_data = rejections_orders[
            (rejections_orders["Date"] >= pd.to_datetime(start_date)) &
            (rejections_orders["Date"] <= pd.to_datetime(today))
        ].copy()

        raw_rejections_data["Week Range"] = raw_rejections_data.apply(lambda row: check_date_range(row, "Date"), axis=1)
        weekly_rejections = raw_rejections_data[raw_rejections_data["Week Range"] != "None"]
        unique_weekly_rejections = weekly_rejections.drop_duplicates(subset=["Date", "Order Number", "Time"])

        weekly_rejection_conversion = unique_weekly_rejections[
            unique_weekly_rejections["Order Number"].isin(sales_orders["Order Number"])
        ]
        weely_rejection_conv = pd.pivot_table(
            weekly_rejection_conversion,
            index = "Outlet",
            columns = "Week Range",
            values = "Order Number",
            aggfunc = "count"
        ).rename(columns= {"Order Number": "Converted"}).fillna(0)

        weekly_conv_branches = pd.merge(branch_data[["Outlet", "RM", "SRM"]],
            weely_rejection_conv,
            on = "Outlet",
            how = "left"
        ).fillna(0).set_index(["Outlet", "SRM", "RM"])

        weekly_rejections_pivot = pd.pivot_table(unique_weekly_rejections,
            index = "Outlet",
            columns="Week Range",
            values="Order Number",
            aggfunc="count"
        ).rename(columns={"Order Number": "Count of Rejections"}).fillna(0)

        weekly_rejection_branches = pd.merge(branch_data[["Outlet", "RM", "SRM"]],
            weekly_rejections_pivot,
            on = "Outlet",
            how = "left"
        ).fillna(0).set_index(["Outlet", "SRM", "RM"])

        weekly_insurance_orders_pivot.columns = pd.MultiIndex.from_product([["Total Orders"], weekly_insurance_orders_pivot.columns])
        weekly_rejection_branches.columns = pd.MultiIndex.from_product([["Rejected"], weekly_rejection_branches.columns])
        weekly_conv_branches.columns = pd.MultiIndex.from_product([["Converted"], weekly_conv_branches.columns])

        final_reje_merge = pd.merge(
            weekly_insurance_orders_pivot, 
            weekly_rejection_branches, 
            right_index=True, 
            left_index=True, 
            how = "left"
        ).fillna(0)

        final_reje_merge = pd.merge(
            final_reje_merge, 
            weekly_conv_branches, 
            right_index=True, 
            left_index=True, 
            how = "left"
        ).fillna(0)

        final_reje_merge = final_reje_merge.swaplevel(0, 1, axis=1).sort_index(axis=1, level=0)
        final_reje_merge = final_reje_merge.reindex(["Total Orders", "Rejected", "Converted"], axis = 1, level = 1)
        sorted_columns = return_four_week_range()

        cols = []
        for column in sorted_columns:
            cols.append((column, "Total Orders"))
            cols.append((column, "Rejected"))
            cols.append((column, "Converted"))


        final_reje_merge = final_reje_merge.reindex(cols, axis =1, fill_value=0)
        for date in final_reje_merge.columns.levels[0]:
            col_name = (date, '%Rejected')
            final_reje_merge.insert(
                final_reje_merge.columns.get_loc((date, 'Rejected')) + 1, col_name,(final_reje_merge[(date, 'Rejected')] / final_reje_merge[(date, 'Total Orders')] * 100).replace([np.inf, -np.inf], np.nan).fillna(0).round().astype(int)
            )

        for date in final_reje_merge.columns.levels[0]:
            col_name = (date, '%Conversion')
            result = (final_reje_merge[(date, 'Converted')] / final_reje_merge[(date, 'Rejected')] * 100).round(0).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)
            final_reje_merge.insert(final_reje_merge.columns.get_loc((date, 'Converted')) + 1, col_name, result)

        final_weekly_rejections_report = final_reje_merge.reindex(
            sorted_columns,
            axis = 1, 
            level = 0).reindex(
            ["Total Orders", "Rejected", "%Rejected", "%Conversion"], 
            axis = 1, 
            level = 1
        )

        # final_weekly_rejections_report = get_rm_srm_total_multiindex(
        #     final_weekly_rejections_report, week_month="Week", 
        #     a = "%Rejected",
        #     b = "Rejected",
        #     c = "Total Orders",
        #     report = final_weekly_rejections_report
        # )

        weekly_rejections_data = unique_weekly_rejections[
            (unique_weekly_rejections["Week Range"] ==  sorted_columns[-1])
        ][cols_rej]

        weekly_insu_orders = weekly_insurance_orders[
            (weekly_insurance_orders["Week Range"] ==  sorted_columns[-1])
        ]

        ewc_weekly_pivot = create_rejections_ewc(
            orders = weekly_insu_orders,
            rejections=weekly_rejections_data
        )

        branches_rejections_weekly = create_rejections_branches(
            orders=weekly_insu_orders,
            rejections=weekly_rejections_data,
            branch_data=branch_data
        )

        with pd.ExcelWriter(f"{path}draft_upload/rejections_report.xlsx") as writer:
            final_weekly_rejections_report.to_excel(writer, sheet_name="weekly_summary")
            weekly_rejections_data.to_excel(writer, sheet_name="weekly_rejections_data", index=False)
            ewc_weekly_pivot.to_excel(writer, sheet_name="ewc_summary", index=False)
            branches_rejections_weekly.to_excel(writer, sheet_name="branch_summary", index=False)
    
    if selection == "Monthly":
        monthly_insurance_orders = unique_insurance_merge.copy()
        monthly_insurance_orders["Month"] = monthly_insurance_orders["CreateDate"].dt.month_name()
        monthly_insu_orders_data = monthly_insurance_orders[
            (monthly_insurance_orders["Month"] == first_month) | 
            (monthly_insurance_orders["Month"] == second_month)
        ]

        monthly_insu_orders_data = pd.merge(
            monthly_insu_orders_data, 
            branch_data[["Outlet", "RM", "SRM"]],
            on = "Outlet",
            how = "left"
        )

        monthly_rejections = rejections_orders.copy()
        monthly_rejections = monthly_rejections.drop_duplicates(subset=["DocEntry", "Date", "Time"])
        monthly_rejections["Month"] = pd.to_datetime(monthly_rejections["Date"], dayfirst=True).dt.month_name()
        monthly_rejections_data = monthly_rejections[
            (monthly_rejections["Month"] == first_month) | 
            (monthly_rejections["Month"] == second_month)
        ]

        monthly_rejection_conversion = monthly_rejections_data[
            monthly_rejections_data["Order Number"].isin(sales_orders["Order Number"])
        ]
        monthly_conversion_pivot = pd.pivot_table(
            monthly_rejection_conversion,
            index=["Outlet", "RM", "SRM"],
            columns="Month",
            values="Order Number",
            aggfunc="count"
        )

        monthly_insu_orders_pivot = pd.pivot_table(
            monthly_insu_orders_data,
            index=["Outlet", "RM", "SRM"],
            columns="Month",
            values="Order Number",
            aggfunc="count"
        )

        monthly_rejections_pivot = pd.pivot_table(monthly_rejections_data,
            index=["Outlet", "RM", "SRM"],
            columns="Month",
            values="Order Number",
            aggfunc="count"
        )

        monthly_insu_orders_pivot.columns = pd.MultiIndex.from_product([["Total Orders"], monthly_insu_orders_pivot.columns])
        monthly_rejections_pivot.columns = pd.MultiIndex.from_product([["Rejected"], monthly_rejections_pivot.columns])
        monthly_conversion_pivot.columns = pd.MultiIndex.from_product([["Conversion"], monthly_conversion_pivot.columns])

        final_monthly_rejections = pd.merge(monthly_insu_orders_pivot, monthly_rejections_pivot, right_index=True, left_index=True, how = "left").fillna(0)
        final_monthly_rejections = pd.merge(final_monthly_rejections, monthly_conversion_pivot, right_index=True, left_index=True, how = "left").fillna(0)
        final_monthly_rejections = final_monthly_rejections.swaplevel(0, 1, axis=1).sort_index(axis=1, level=0)
        final_monthly_rejections = final_monthly_rejections.reindex(["Total Orders", "Rejected", "Conversion"], axis = 1, level = 1)

        columns = [
            (first_month, "Total Orders"),
            (first_month, "Rejected"),
            (first_month, "Conversion"),
            (second_month, "Total Orders"),
            (second_month, "Rejected"),
            (second_month, "Conversion")
        ]
        final_monthly_rejections = final_monthly_rejections.reindex(columns, axis=1, fill_value=0)

        for date in final_monthly_rejections.columns.levels[0]:
            col_name = (date, '%Rejected')
            result = (final_monthly_rejections[(date, 'Rejected')] / final_monthly_rejections[(date, 'Total Orders')] * 100).round(0).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)
            final_monthly_rejections.insert(final_monthly_rejections.columns.get_loc((date, 'Rejected')) + 1, col_name, result)

        for date in final_monthly_rejections.columns.levels[0]:
            col_name = (date, '%Conversion')
            result = (final_monthly_rejections[(date, 'Conversion')] / final_monthly_rejections[(date, 'Rejected')] * 100).round(0).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)
            final_monthly_rejections.insert(final_monthly_rejections.columns.get_loc((date, 'Conversion')) + 1, col_name, result)

        final_monthly_rejections = final_monthly_rejections.reindex(["Total Orders", "Rejected", "%Rejected"], level = 1, axis=1)
        final_monthly_rejections = get_rm_srm_total_multiindex(final_monthly_rejections,week_month = "Month")

        new_cols = [(y, x) if x in ['Outlet', 'RM', 'SRM'] else (x, y) for x, y in final_monthly_rejections.columns]
        final_monthly_rejections.columns = pd.MultiIndex.from_tuples(new_cols)
    
        with pd.ExcelWriter(f"{path}draft_upload/rejections_report.xlsx") as writer:
            final_monthly_rejections.to_excel(writer, sheet_name = "monthly_summary")
            monthly_rejections_data.to_excel(writer, sheet_name="monthly_data", index=False)


