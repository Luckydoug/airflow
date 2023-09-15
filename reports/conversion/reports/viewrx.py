import pandas as pd
from airflow.models import variable
from sub_tasks.libraries.utils import check_date_range, get_comparison_months
from reports.conversion.utils.utils import (
    create_staff_conversion,
    create_branch_conversion,
    create_country_conversion,
    create_weeky_branch_conversion,
    create_monthly_conversion,
    create_monthly_summary
)


def create_views_conversion(data, country, path, selection):
    conversions = data.copy()
    if selection == "Daily":
        return
    
    elif selection == "Weekly":
        conversions["Week Range"] = conversions.apply(
            lambda row: check_date_range(row, "ViewDate"),
            axis = 1
        )

        weekly_viewrx_conv = create_weeky_branch_conversion(
            conversions=conversions,
            index="Branch",
            week_range="Week Range",
            values=["DocEntry", "Conversion"],
            cols=["%Conversion", "Conversion", "DocEntry", "Views"]
        )

        summary_weekly_conv = create_country_conversion(
            conversions=conversions,
            week_range="Week Range",
            values=["DocEntry", "Conversion"],
            country=country,
            cols=["%Conversion", "Conversion", "DocEntry", "Views"]
        )

        last_date_range = summary_weekly_conv.columns.get_level_values(0)[-1]
        non_conversions = conversions[
            (conversions["Conversion"] == 0) &
            (conversions["Week Range"] == last_date_range)
        ]

        weekly_data = conversions[conversions["Week Range"] == last_date_range]

        branch_conversion = create_branch_conversion(
            weekly_data=weekly_data,
            index="Branch",
            values=["DocEntry", "Conversion"],
            rename={
                "DocEntry": "Views",
                "Conversion": "Converted",
            },
            cols_order=["Branch", "Views", "Converted", "%Conversion"]
        )

        ewc_conversion = create_staff_conversion(
            weekly_data=weekly_data,
            index=["Branch", "User Name"],
            values=["DocEntry", "Conversion"],
            rename={
                "User Name": "Staff",
                "DocEntry": "Views",
                "Conversion": "Converted",
            },
            cols_order=["Branch", "Staff", "Views", "Converted", "%Conversion"]
        )

        with pd.ExcelWriter(f"{path}conversion/viewrx/overall.xlsx") as writer:
            summary_weekly_conv.to_excel(writer, sheet_name="Summary_Conversion")
            weekly_viewrx_conv.to_excel(writer, sheet_name="Branches_Conversion")
            ewc_conversion.to_excel(writer, sheet_name="EWC", index=False)
            non_conversions.sort_values(by="Branch").to_excel(
                writer, sheet_name="Non Conversions", index=False)

        with pd.ExcelWriter(f"{path}conversion/viewrx/sales_persons.xlsx") as writer:
            for group, dataframe in ewc_conversion.groupby("Branch"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)

        with pd.ExcelWriter(f"{path}conversion/viewrx/branches.xlsx") as writer:
            for group, dataframe in branch_conversion.groupby("Branch"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)

        with pd.ExcelWriter(f"{path}conversion/viewrx/non_conversions.xlsx") as writer:
            non_conversions.to_excel(writer, sheet_name = "Master", index = False)
            for group, dataframe in non_conversions.groupby("Branch"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)

    elif selection == "Monthly":
        first_month, second_month = get_comparison_months()
        monthly_data = conversions[
            (conversions["Month"] == first_month) |
            (conversions["Month"] == second_month)
        ]


        country_conversion = create_monthly_summary(
            data=monthly_data,
            values=["DocEntry", "Conversion"],
            rename={
                "DocEntry": "Views",
                "Conversion": "Converted"
            },
            country=country
        )

        branch_conversion = create_monthly_conversion(
            data=monthly_data,
            index="Branch",
            values=["DocEntry", "Conversion"],
            rename={
                "DocEntry": "Views",
                "Conversion": "Converted"
            }
        )

        non_conversions =   monthly_data[
            (monthly_data["Conversion"] == 0) &
            (monthly_data["Month"] == second_month)
        ]


        with pd.ExcelWriter(f"{path}conversion/viewrx/overall.xlsx") as writer:
            country_conversion.to_excel(writer, sheet_name="Monthly_Conversion")
            branch_conversion.to_excel(writer, sheet_name="Branches_Conversion")
            non_conversions.sort_values(by="Branch").to_excel(
                writer, 
                sheet_name="Non Conversions", 
                index=False
            )

        with pd.ExcelWriter(f"{path}conversion/viewrx/non_conversions.xlsx") as writer:
            non_conversions.to_excel(writer, sheet_name = "Master", index = False)
            for group, dataframe in non_conversions.groupby("Branch"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)
    else:
        return
