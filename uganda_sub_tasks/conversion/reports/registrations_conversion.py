from airflow.models import variable
import pandas as pd
from sub_tasks.libraries.utils import (
    uganda_path,
    check_date_range,
    create_staff_conversion,
    create_branch_conversion,
    create_country_conversion,
    create_weeky_branch_conversion,
)


def create_registrations_conversion(data):
    conversions = data.copy()
    conversions["Week Range"] = conversions.apply(
        lambda row: check_date_range(row, "CreateDate"), axis=1
    )

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
        country="Uganda",
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

    with pd.ExcelWriter(f"{uganda_path}conversion/registrations/overall.xlsx") as writer:
        summary_weekly_conv.to_excel(writer, sheet_name="summary")
        weekly_reg_conv.to_excel(writer, sheet_name="per_branch")

    with pd.ExcelWriter(f"{uganda_path}conversion/registrations/sales_persons.xlsx") as writer:
        for group, dataframe in sales_persons_conversion.groupby("Outlet"):
            name = f'{group}'
            dataframe.to_excel(writer, sheet_name=name, index=False)

    with pd.ExcelWriter(f"{uganda_path}conversion/registrations/branch.xlsx") as writer:
        for group, dataframe in branch_conversion.groupby("Outlet"):
            name = f'{group}'
            dataframe.to_excel(writer, sheet_name=name, index=False)

    with pd.ExcelWriter(f"{uganda_path}conversion/registrations/non_conversions.xlsx") as writer:
        non_conversions.iloc[:, :-
                             2].to_excel(writer, sheet_name="master", index=False)
        for group, dataframe in non_conversions.groupby("Outlet"):
            name = f'{group}'
            dataframe.iloc[:, :-
                           2].to_excel(writer, sheet_name=name, index=False)
