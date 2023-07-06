import pandas as pd
import numpy as np
from airflow.models import variable
from reports.draft_to_upload.utils.utils import to_drop
from sub_tasks.libraries.utils import (
    get_yesterday_date, 
    get_rm_srm_total, 
    check_date_range, 
    get_comparison_months,
)

fixed_columns = ["Outlet", "RM", "SRM"]
def arrange_date_ranges(dataframe):
    date_columns = [col for col in dataframe.columns if col not in fixed_columns]
    start_dates = [pd.to_datetime(col.split(" to ")[0]) for col in date_columns]
    date_columns = [x for _, x in sorted(zip(start_dates, date_columns))]
    
    return date_columns

first_month, second_month = get_comparison_months()

def create_ratings_report(selection, surveys, branch_data, path):
    if not len(surveys):
        return
    to_drop["Order Number"] = to_drop["Order Number"].astype(int)
    surveys["SAP Internal Number"] = surveys["SAP Internal Number"].astype(int)
    surveys = surveys[~surveys["SAP Internal Number"].isin(to_drop["Order Number"])]
    all_detractors = surveys[surveys["NPS Rating"] < 7]


    if selection == "Daily":
        daily_detractors = all_detractors[(all_detractors["Trigger Date"] == get_yesterday_date(truth=True))]
        if not len(daily_detractors):
            return
        daily_detractors_report = pd.pivot_table(
            daily_detractors,
            index = "Branch", 
            values="SAP Internal Number", 
            aggfunc="count"
        ).reset_index().rename(columns={"Branch": "Outlet"})

        daily_detractors_merge = pd.merge(
            daily_detractors_report.rename(columns={"SAP Internal Number": "Count of Detractors (0-6)"}),
            branch_data[["Outlet", "RM", "SRM"]],
            on = "Outlet", how = "right").fillna(0)[["Outlet", "RM", "SRM", "Count of Detractors (0-6)"]]

        daily_final_detractors = get_rm_srm_total(daily_detractors_merge)

        with pd.ExcelWriter(f"{path}draft_upload/detractors_report.xlsx") as writer:
            daily_final_detractors.to_excel(writer, sheet_name="daily_summary", index=False)
            daily_detractors.to_excel(writer, sheet_name="daily_detractors_data", index=False)

    if selection == "Weekly":
        weekly_detractors_data = all_detractors.copy()
        weekly_detractors_data["Week Range"] = weekly_detractors_data.apply(lambda row: check_date_range(row, "Trigger Date"), axis = 1)
        detractors_week_data = weekly_detractors_data[weekly_detractors_data["Week Range"] != "None"]

        weekly_detractors_report = pd.pivot_table(
            detractors_week_data,
            index=["Branch"],
            columns="Week Range",
            values="SAP Internal Number",
            aggfunc="count"
        ).fillna(0).reset_index().rename(columns = {"Branch": "Outlet"}).merge(branch_data[["Outlet", "RM", "SRM"]], on = "Outlet", how = "right").fillna(0)
        
        weekly_detractors_report = weekly_detractors_report[fixed_columns + arrange_date_ranges(weekly_detractors_report)]
        final_weekly_detractors_report = get_rm_srm_total(weekly_detractors_report, has_perc=False)

        week_detractors_data = detractors_week_data[
            (detractors_week_data["Week Range"] == weekly_detractors_report.columns[-1])
        ].iloc[:, :-1]
         
        with pd.ExcelWriter(f"{path}draft_upload/detractors_report.xlsx") as writer:
            final_weekly_detractors_report.to_excel(writer, sheet_name="weekly_summary", index=False)
            week_detractors_data.to_excel(writer, sheet_name="weekly_detractors_data", index=False)

    if selection == "Monthly":
        monthly_detractors = all_detractors.copy()
        monthly_detractors["Month"] = pd.to_datetime(monthly_detractors["Trigger Date"], dayfirst=True).dt.month_name()
        monthly_detractors = monthly_detractors[(monthly_detractors["Month"] == first_month) | (monthly_detractors["Month"] == second_month)]
        monthly_detractors = pd.merge(
            monthly_detractors,
            branch_data[["Outlet", "RM", "SRM"]].rename(columns={"Outlet": "Branch"}),
            on = "Branch",
            how = "left"
        )

        monthly_detractors_pivot = pd.pivot_table(
            monthly_detractors, 
            index=["Branch", "RM", "SRM"], 
            columns="Month", 
            values="SAP Internal Number", 
            aggfunc="count"
        ).fillna(0).reset_index().rename(columns={"Branch": "Outlet"})


        monthly_detractors_pivot = get_rm_srm_total(monthly_detractors_pivot)
        monthly_detractors_pivot = monthly_detractors_pivot[["Outlet", "RM", "SRM", first_month, second_month]]
        monthly_detractors_data = monthly_detractors[monthly_detractors["Month"] == second_month]

        with pd.ExcelWriter(f"{path}draft_upload/detractors_report.xlsx") as writer:
            monthly_detractors_pivot.to_excel(writer, sheet_name="monthly_summary", index=False)
            monthly_detractors_data.to_excel(writer, sheet_name="monthly_detractors_data", index=False)

       
