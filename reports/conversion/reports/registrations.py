from airflow.models import variable
import pandas as pd
from sub_tasks.libraries.utils import check_date_range, get_comparison_months
from reports.conversion.utils.utils import (
    create_monthly_summary,
    create_monthly_conversion
)
from sub_tasks.libraries.utils import (
    create_staff_conversion,
    create_branch_conversion,
    create_country_conversion,
    create_weeky_branch_conversion,
    
)


def create_registrations_conversion(data, country, path, selection):
    conversions = data.copy()
    if selection == "Daily":
        return 
    
    elif selection == "Weekly":
        conversions["Week Range"] = conversions.apply(
            lambda row: check_date_range(row, "CreateDate"), axis=1
        )
        conversions = conversions.drop_duplicates(subset = ["Customer Code"])

        weekly_reg_conv = create_weeky_branch_conversion(
            conversions=conversions,
            index="Outlet",
            week_range="Week Range",
            values=["Customer Code", "Conversion"],
            cols=["%Conversion", "Conversion", "Customer Code", "Registrations"]
        )

        summary_weekly_conv = create_country_conversion(
            conversions=conversions,
            week_range="Week Range",
            values=["Customer Code", "Conversion"],
            country=country,
            cols=["%Conversion", "Conversion", "Customer Code", "Registrations"]
        )

        last_date_range = summary_weekly_conv.columns.get_level_values(0)[-1]
        weekly_data = conversions[conversions["Week Range"] == last_date_range]
        non_conversions = weekly_data[
            (weekly_data["Conversion"] == 0) &
            (weekly_data["Week Range"] == last_date_range)
        ]

        sales_persons_conversion = create_staff_conversion(
            weekly_data=weekly_data,
            index=["Outlet", "Staff"],
            values=["Customer Code", "Conversion"],
            rename={
                "Conversion": "Converted",
                "Customer Code": "Customers"
            },
            cols_order=["Outlet", "Staff", "Customers", "Converted", "%Conversion"]
        )

        branch_conversion = create_branch_conversion(
            weekly_data=weekly_data,
            index="Outlet",
            values=["Customer Code", "Conversion"],
            rename={
                "Conversion": "Converted",
                "Customer Code": "Customers"
            },

            cols_order=["Outlet", "Customers", "Converted", "%Conversion"]
        )

        with pd.ExcelWriter(f"{path}conversion/registrations/overall.xlsx") as writer:
            summary_weekly_conv.to_excel(writer, sheet_name="summary")
            weekly_reg_conv.to_excel(writer, sheet_name="per_branch")

        with pd.ExcelWriter(f"{path}conversion/registrations/sales_persons.xlsx") as writer:
            for group, dataframe in sales_persons_conversion.groupby("Outlet"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)

        with pd.ExcelWriter(f"{path}conversion/registrations/branch.xlsx") as writer:
            for group, dataframe in branch_conversion.groupby("Outlet"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)

        with pd.ExcelWriter(f"{path}conversion/registrations/non_conversions.xlsx") as writer:
            non_conversions.iloc[:, :-2].to_excel(writer, sheet_name="master", index=False)
            for group, dataframe in non_conversions.groupby("Outlet"):
                name = f'{group}'
                dataframe.iloc[:, :-2].to_excel(
                    writer, 
                    sheet_name=name, 
                    index=False
                )


    elif selection == "Monthly":
        first_month, second_month = get_comparison_months()
        monthly_data = conversions[
            (conversions["Month"] == first_month) |
            (conversions["Month"] == second_month)
        ]


        country_conversion = create_monthly_summary(
            data=monthly_data,
            values=["Customer Code", "Conversion"],
            rename={
                "Customer Code": "Customers",
                "Conversion": "Converted"
            },
            country="Kenya"
        )

        branch_conversion = create_monthly_conversion(
            data=monthly_data,
            index="Outlet",
            values=["Customer Code", "Conversion"],
            rename={
                "Customer Code": "Customers",
                "Conversion": "Converted"
            }
        )

        non_conversions =   monthly_data[
            (monthly_data["Conversion"] == 0) &
            (monthly_data["Month"] == second_month)
        ]

        with pd.ExcelWriter(f"{path}conversion/registrations/overall.xlsx") as writer:
            country_conversion.to_excel(writer, sheet_name="monthly_summary")
            branch_conversion.to_excel(writer, sheet_name="per_branch")
            non_conversions.to_excel(writer, sheet_name = "non_conversions", index = False)

        with pd.ExcelWriter(f"{path}conversion/registrations/non_conversions.xlsx") as writer:
            non_conversions.iloc[:, :-2].to_excel(writer, sheet_name="master", index=False)
            for group, dataframe in non_conversions.groupby("Outlet"):
                name = f'{group}'
                dataframe.iloc[:, :-2].to_excel(
                    writer, 
                    sheet_name=name, 
                    index=False
                )
