from airflow.models import variable
import pandas as pd
from sub_tasks.libraries.utils import check_date_range, get_comparison_months
from reports.conversion.utils.utils import (
    create_staff_conversion,
    create_branch_conversion,
    create_country_conversion,
    create_weeky_branch_conversion,
    create_monthly_conversion,
    create_monthly_summary
)

def create_eyetests_conversion(data, country, path, selection):
    conversions = data.copy()
    if selection == "Daily":
        return
    
    elif selection == "Weekly":
        conversions["week range"] = conversions.apply(
            lambda row: check_date_range(row, "create_date"), axis=1
        )

        weekly_et_conv = create_weeky_branch_conversion(
            conversions=conversions,
            index="branch_code",
            week_range="week range",
            values=["code", "conversion"],
            cols=["%Conversion", "conversion", "code", "ETs"]
        )

        summary_weekly_conv = create_country_conversion(
            conversions=conversions,
            week_range="week range",
            values=["code", "conversion"],
            country=country,
            cols=["%Conversion", "conversion", "code", "ETs"]
        )

        high_rx_data = conversions[conversions["RX"] == "High Rx"]
        weekly_highrx_conv = create_country_conversion(
            conversions=high_rx_data,
            week_range="week range",
            values=["code", "conversion"],
            country=country,
            cols=["%Conversion", "conversion", "code", "ETs"]
        )

        branch_high_rx = create_weeky_branch_conversion(
            conversions=high_rx_data,
            index="branch_code",
            week_range="week range",
            values=["code", "conversion"],
            cols=["%Conversion", "conversion", "code", "ETs"]
        )

        last_date_range = summary_weekly_conv.columns.get_level_values(0)[-1]
        non_conversions = conversions[
            (conversions["conversion"] == 0) &
            (conversions["week range"] == last_date_range)
        ]

        non_conversions_data = non_conversions.rename(columns={
            #Rename the columns so they can be easy to read.
            "code": "ET Code",
            "cust_code": "Customer Code",
            "create_date": "ET Date",
            "create_time": "ET Time",
            "branch_code": "Branch",
            "optom_name": "Opthom Name",
            "rx_type": "ET Type",
            "mode_of_pay": "Customer Type",
            "handed_over_to": "Handed Over To",
            "last_viewed_by": "RX Last Viewed By",
            "view_date": "View Date",
            "order_converted": "Order Converted",
            "date_converted": "Date Converted",
            "days": "Days to Convert",
            "on_after": "Order Created",
            "on_after_createdon": "Order Created On",
            "on_after_cancelled": "Order Cancelled",
            "on_after_status": "Order Status",
            "view_creator": "Who Viewed RX"
        })

        non_conversions_data = non_conversions_data[[
            #At this point we are selection only the columns that are
            #necessary to the branches and the management.
            #You can alway add an extra column whenever requested by the branch
            # and / or the management.
            "ET Code",
            "ET Date",
            "ET Time",
            "Branch",
            "Opthom Name",
            "Customer Code",
            "ET Type",
            "RX",
            "Customer Type",
            "Handed Over To",
            "RX Last Viewed By",
            "View Date",
            "Order Converted",
            "Date Converted",
            "Days to Convert",
            "Order Created",
            "Order Created On",
            "Order Cancelled",
            "Order Status"
        ]]

        weekly_data = conversions[conversions["week range"] == last_date_range]
        branch_conversion = create_branch_conversion(
            weekly_data=weekly_data,
            index="branch_code",
            values=["code", "conversion"],
            rename={
                "branch_code": "Outlet",
                "code": "ETs",
                "conversion": "Converted",
            },
            cols_order=["Outlet", "ETs", "Converted", "%Conversion"]
        )

        ewc_conversion = create_staff_conversion(
            weekly_data=weekly_data,
            index=["branch_code", "handed_over_to"],
            values=["code", "conversion"],
            rename={
                "branch_code": "Outlet",
                "handed_over_to": "Staff",
                "code": "ETs",
                "conversion": "Converted"
            },
            cols_order=["Outlet", "Staff", "ETs", "Converted", "%Conversion"]
        )

        opthom_conversion = create_staff_conversion(
            weekly_data=weekly_data,
            index = ["branch_code", "optom_name"],
            values=["code", "conversion"],
            rename={
                "branch_code": "Outlet",
                "optom_name": "Optom",
                "code": "ETs",
                "conversion": "Converted"
            },
            cols_order=["Outlet", "Optom", "ETs", "Converted", "%Conversion"]
        )

        with pd.ExcelWriter(f"{path}conversion/eyetests/overall.xlsx") as writer:
            summary_weekly_conv.to_excel(writer, sheet_name="Summary_Conversion")
            weekly_et_conv.to_excel(writer, sheet_name="Branches_Conversion")
            weekly_highrx_conv.to_excel(writer, sheet_name="Highrx_Conversion")
            branch_high_rx.to_excel(writer, sheet_name="high_rx_branch")
            non_conversions_data.sort_values(by="Branch").to_excel(
                writer, 
                sheet_name="Non Conversions", 
                index=False
            )

        with pd.ExcelWriter(f"{path}conversion/eyetests/sales_persons.xlsx") as writer:
            for group, dataframe in ewc_conversion.groupby("Outlet"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)

        with pd.ExcelWriter(f"{path}conversion/eyetests/opthoms.xlsx") as writer:
            for group, dataframe in opthom_conversion.groupby("Outlet"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)

        with pd.ExcelWriter(f"{path}conversion/eyetests/branches.xlsx") as writer:
            for group, dataframe in branch_conversion.groupby("Outlet"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)

        with pd.ExcelWriter(f"{path}conversion/eyetests/non_conversions.xlsx") as writer:
            non_conversions_data.to_excel(writer, sheet_name = "Master", index = False)
            for group, dataframe in non_conversions_data.groupby("Branch"):
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
            values=["code", "conversion"],
            rename={
                "code": "ETs",
                "conversion": "Converted"
            },
            country=country
        )

        branch_conversion = create_monthly_conversion(
            data=monthly_data,
            index="branch_code",
            values=["code", "conversion"],
            rename={
                "code": "ETs",
                "conversion": "Converted"
            }
        )

        high_rx_data = monthly_data[monthly_data["RX"] == "High Rx"]

        high_rx_conversion = create_monthly_summary(
            data=high_rx_data,
            values=["code", "conversion"],
            rename={
                "code": "ETs",
                "conversion": "Converted"
            },
            country=country
        )

        
        branch_highrx_conversion = create_monthly_conversion(
            data=high_rx_data,
            index="branch_code",
            values=["code", "conversion"],
            rename={
                "code": "ETs",
                "conversion": "Converted"
            }
        )

        optom_highrx_conversion = create_monthly_conversion(
            data=high_rx_data,
            index=["branch_code", "optom_name"],
            values=["code", "conversion"],
            rename={
                "code": "ETs",
                "conversion": "Converted"
            }
        )

        ewc_highrx_conversion = create_monthly_conversion(
            data=high_rx_data,
            index=["branch_code", "handed_over_to"],
            values=["code", "conversion"],
            rename={
                "code": "ETs",
                "conversion": "Converted"
            }
        )

        non_conversions = monthly_data[
            (monthly_data["conversion"] == 0) &
            (monthly_data["Month"] == second_month)
        ]


        non_conversions_data = non_conversions.rename(columns={
            "code": "ET Code",
            "cust_code": "Customer Code",
            "create_date": "ET Date",
            "create_time": "ET Time",
            "branch_code": "Branch",
            "optom_name": "Opthom Name",
            "rx_type": "ET Type",
            "mode_of_pay": "Customer Type",
            "handed_over_to": "Handed Over To",
            "last_viewed_by": "RX Last Viewed By",
            "view_date": "View Date",
            "order_converted": "Order Converted",
            "date_converted": "Date Converted",
            "days": "Days to Convert",
            "on_after": "Order Created",
            "on_after_createdon": "Order Created On",
            "on_after_cancelled": "Order Cancelled",
            "on_after_status": "Order Status",
            "view_creator": "Who Viewed RX",
            "insurance_name": "Insurance Company"
        })

        non_conversions_data = non_conversions_data[[
            "ET Code",
            "ET Date",
            "ET Time",
            "Branch",
            "Opthom Name",
            "Customer Code",
            "ET Type",
            "RX",
            "Customer Type",
            "Handed Over To",
            "RX Last Viewed By",
            "View Date",
            "Order Converted",
            "Date Converted",
            "Days to Convert",
            "Order Created",
            "Order Created On",
            "Order Cancelled",
            "Order Status"
        ]]

        with pd.ExcelWriter(f"{path}conversion/eyetests/overall.xlsx") as writer:
            country_conversion.to_excel(writer, sheet_name="Monthly_Conversion")
            branch_conversion.to_excel(writer, sheet_name="Branches_Conversion")
            high_rx_conversion.to_excel(writer, sheet_name="Highrx_Conversion")
            branch_highrx_conversion.to_excel(writer, sheet_name="branch_highrx")
            optom_highrx_conversion.to_excel(writer, sheet_name="Optom")
            ewc_highrx_conversion.to_excel(writer, sheet_name="EWC")
            high_rx_data.to_excel(writer, sheet_name = "Data")
            non_conversions_data.sort_values(by="Branch").to_excel(
                writer, 
                sheet_name="Non Conversions", 
                index=False
            )
            
        with pd.ExcelWriter(f"{path}conversion/eyetests/non_conversions.xlsx") as writer:
            non_conversions_data.to_excel(writer, sheet_name = "Master", index = False)
            for group, dataframe in non_conversions_data.groupby("Branch"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)
            
        

