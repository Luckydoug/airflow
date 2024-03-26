from airflow.models import variable
import pandas as pd
import numpy as np
from reports.queue_time.utils.utils import create_queue_summary
from reports.queue_time.utils.utils import check_date_range
from reports.queue_time.utils.utils import create_queue_weekly
from reports.queue_time.utils.utils import create_queue_monthly
from sub_tasks.libraries.utils import save_dataframes_to_excel
from sub_tasks.libraries.utils import get_comparison_months

def create_queue_time_report(
    selection: str,
    queue_data: pd.DataFrame,
    start_date: str,
    path: str,
    country: str
) -> None:
    if not len(queue_data):
        return
    
    if country == "Kenya" or country == "Rwanda":
        target = 10
    elif country == "Uganda":
        target = 8

    if selection == "Daily":
        daily_data = queue_data[
            queue_data["CreateDate"] == pd.to_datetime(start_date, format="%Y-%m-%d").date()
        ]

        branch_daily = create_queue_summary(
            daily_data,
            index=["Branch"]
        )

        optom_daily = create_queue_summary(
            daily_data,
            index=["Branch", "Optom Name"]
        )

        save_dataframes_to_excel(
            path = f"{path}queue_time/queue_report.xlsx",
            dataframes=[branch_daily,optom_daily,daily_data],
            sheets=["Branch Summary", "Optom Summary", "Daily Data"],
            multindex=[False, False, False]
        )

    elif selection == "Weekly":
        weekly_data = queue_data.copy()
        weekly_data["Country"] = country
        weekly_data["Week Range"] =  queue_data.apply(lambda row: check_date_range(row, "CreateDate"), axis=1)
        weekly_data = weekly_data[weekly_data["Week Range"] != "None"]

        country_summary = create_queue_weekly(
            queue_data=weekly_data,
            index="Country",
            target = target
        )

        branch_summary = create_queue_weekly(
            queue_data=weekly_data,
            index="Branch",
            target = target
        )

        last_column = branch_summary.columns[3]
        branch_summary = branch_summary.sort_values(by=last_column, ascending=False)


        optom_summary = create_queue_weekly(
            queue_data=weekly_data,
            index=["Branch", "Optom Name"],
            target = target
        )

        # optom_summary = optom_summary.sort_values(by=last_column, ascending=False)


        with pd.ExcelWriter(f"{path}queue_time/queue_report.xlsx") as writer:
            country_summary.to_excel(writer, sheet_name="Country Summary")
            branch_summary.to_excel(writer, sheet_name="Branch Summary")
            optom_summary.reset_index().to_excel(writer, sheet_name="Optom Summary")
            weekly_data.to_excel(writer, sheet_name="Weekly Data", index=False)


    elif selection == "Monthly":
        monthly_data = queue_data.copy()
        monthly_data["Country"] = country

        country_summary = create_queue_monthly(
            queue_data=monthly_data,
            index="Country"
        )

        branch_summary = create_queue_monthly(
            queue_data=monthly_data,
            index="Branch"
        )

        optom_summary = create_queue_monthly(
            queue_data=monthly_data,
            index=["Branch", "Optom Name"]
        )

        first_month, second_month = get_comparison_months()
        export_data = monthly_data[
            monthly_data["Month"].isin([first_month, second_month])
        ]

        with pd.ExcelWriter(f"{path}queue_time/queue_report.xlsx") as writer:
            country_summary.to_excel(writer, sheet_name="Country Summary")
            branch_summary.to_excel(writer, sheet_name="Branch Summary")
            optom_summary.reset_index().to_excel(writer, sheet_name="Optom Summary")
            export_data.to_excel(writer, sheet_name="Monthly Data", index=False)

    
    else:
        return
        """
        If someone passes the wrong argument, we want to return from the function
        We just don't want to do anything
        But meanwhile, 
        Let's return
        """