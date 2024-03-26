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

def check_preauth(row):
    if pd.isna(row["scheme_type"]):
        return None
    else:
        if pd.isna(row["request"]) and row["scheme_type"] in ['PREAUTH + SMART', 'PREAUTH ONLY']:
            return "No"
        elif not pd.isna(row["request"]) and row["scheme_type"] in ['PREAUTH + SMART', 'PREAUTH ONLY']:
            return "Yes"
        elif not pd.isna(row["request"]) and row["scheme_type"] not in ['PREAUTH + SMART', 'PREAUTH ONLY']:
            return "Direct forms uploaded"
        else:
            return "Direct Forms not Uploaded"


def create_eyetests_conversion(data, country, path, selection, insurances = pd.DataFrame()):
    conversions = data.copy()

    if insurances.empty:
        conversions["Sent Preuath?"] = None
        pass
    else:
        conversions = pd.merge(
            insurances,
            conversions,
            on = "cust_code",
            how = "right"
        )

        conversions = conversions.drop_duplicates(subset=["code"])
        conversions["Sent Preuath?"] = conversions.apply(lambda row: check_preauth(row), axis=1)
    


    if selection == "Daily":
        return
    


    elif selection == "Weekly":
        conversions["week range"] = conversions.apply(
            lambda row: check_date_range(row, "create_date"), axis=1
        )

        """
        WEEKLY BRANCH CONVERSION
        """
        weekly_et_conv = create_weeky_branch_conversion(
            conversions=conversions,
            index="branch_code",
            week_range="week range",
            values=["code", "conversion"],
            cols=["%Conversion", "conversion", "code", "ETs"]
        )

        """
        COUNTRY CONVERSION SECTION: FIRST TABLE
        """

        summary_weekly_conv = create_country_conversion(
            conversions=conversions,
            week_range="week range",
            values=["code", "conversion"],
            country=country,
            cols=["%Conversion", "conversion", "code", "ETs"]
        )

        """
        HIGH RX REPORT: COUNTRY VERSION
        """
        high_rx_data = conversions[conversions["RX"] == "High Rx"]
        weekly_highrx_conv = create_country_conversion(
            conversions=high_rx_data,
            week_range="week range",
            values=["code", "conversion"],
            country=country,
            cols=["%Conversion", "conversion", "code", "ETs"]
        )

        """
        HIGH RX REPORT: BRANCH VERSION
        """
        branch_high_rx = create_weeky_branch_conversion(
            conversions=high_rx_data,
            index="branch_code",
            week_range="week range",
            values=["code", "conversion"],
            cols=["%Conversion", "conversion", "code", "ETs"]
        )

        ewc_high_rx_comparison = create_weeky_branch_conversion(
            conversions=high_rx_data,
            index=["branch_code", "staff", "designation"],
            week_range="week range",
            values=["code", "conversion"],
            cols=["%Conversion", "conversion", "code", "ETs"]
        )

        optom_high_rx_comparison = create_weeky_branch_conversion(
            conversions=high_rx_data,
            index=["branch_code", "optom_name"],
            week_range="week range",
            values=["code", "conversion"],
            cols=["%Conversion", "conversion", "code", "ETs"]
        )

        ewc = create_weeky_branch_conversion(
            conversions=conversions,
            index=["branch_code", "staff", "designation"],
            week_range="week range",
            values=["code", "conversion"],
            cols=["%Conversion", "conversion", "code", "ETs"]
        )

        optom = create_weeky_branch_conversion(
            conversions=conversions,
            index=["branch_code", "optom_name"],
            week_range="week range",
            values=["code", "conversion"],
            cols=["%Conversion", "conversion", "code", "ETs"]
        )

        """
        CASH HIGH RX CONVERSION
        """
        cash_high_rx_data = conversions[
            (conversions["RX"] == "High Rx") &
            (conversions["mode_of_pay"] == "Cash")
        ]

        cash_high_rx_conversion = create_weeky_branch_conversion(
            conversions=cash_high_rx_data,
            index="branch_code",
            week_range="week range",
            values=["code", "conversion"],
            cols=["%Conversion", "conversion", "code", "ETs"]
        )

        
        overall_cash = create_country_conversion(
            conversions=cash_high_rx_data,
            week_range="week range",
            values=["code", "conversion"],
            country=country,
            cols=["%Conversion", "conversion", "code", "ETs"]
        )


        """
        INSURANCE HIGH RX CONVERSION
        """
        cash_high_rx_data = conversions[
            (conversions["RX"] == "High Rx") &
            (conversions["mode_of_pay"] == "Insurance")
        ]

        insurance_high_rx_conversion = create_weeky_branch_conversion(
            conversions=cash_high_rx_data,
            index="branch_code",
            week_range="week range",
            values=["code", "conversion"],
            cols=["%Conversion", "conversion", "code", "ETs"]
        )


        """
        THE SECTION BELOW CREATES WEEKLY CONVERSION REPORT FOR THE BRANCHES
        WE ONLY USE THE DATA FOR THE LATEST WEEEK.
        """

        last_date_range = summary_weekly_conv.columns.get_level_values(0)[-1]
        
        non_conversions = conversions[
            (conversions["conversion"] == 0) & (conversions["week range"] == last_date_range)
        ]

        non_conversions_data = non_conversions.rename(columns={
            # Rename the columns so they can be easy to read.
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
            "ods_insurance_order": "Insurance Order",
            "scheme_type": "Scheme Type",
            "on_after_createdon": "Order Created On",
            "on_after_cancelled": "Order Cancelled",
            "on_after_status": "Order Status",
            "view_creator": "Who Viewed RX",
            "conversion_remarks": "Conversion Remarks"
        })

        non_conversions_data = non_conversions_data[[
            # At this point we are selection only the columns that are
            # necessary to the branches and the management.
            # You can alway add an extra column whenever requested by the branch
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
            "Insurance Order",
            # "Scheme Type",
            "Sent Preuath?",
            "Order Created On",
            "Order Cancelled",
            "Order Status",
            "Conversion Remarks"
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
            index=["branch_code", "optom_name"],
            values=["code", "conversion"],
            rename={
                "branch_code": "Outlet",
                "optom_name": "Optom",
                "code": "ETs",
                "conversion": "Converted"
            },
            cols_order=["Outlet", "Optom", "ETs", "Converted", "%Conversion"]
        )


        last_high_rx_data = weekly_data[weekly_data["RX"] == "High Rx"]

        highrx_branch_conversion = create_branch_conversion(
            weekly_data=last_high_rx_data,
            index="branch_code",
            values=["code", "conversion"],
            rename={
                "branch_code": "Outlet",
                "code": "ETs",
                "conversion": "Converted",
            },
            cols_order=["Outlet", "ETs", "Converted", "%Conversion"]
        )

        highrx_ewc_conversion = create_staff_conversion(
            weekly_data=last_high_rx_data,
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

        highrx_opthom_conversion = create_staff_conversion(
            weekly_data=last_high_rx_data,
            index=["branch_code", "optom_name"],
            values=["code", "conversion"],
            rename={
                "branch_code": "Outlet",
                "optom_name": "Optom",
                "code": "ETs",
                "conversion": "Converted"
            },
            cols_order=["Outlet", "Optom", "ETs", "Converted", "%Conversion"]
        )





        previous_week = summary_weekly_conv.columns.get_level_values(0)[5]
    
        previous_week_data  = conversions[conversions["week range"] == previous_week]

        branch_conversion_prev = create_branch_conversion(
                weekly_data=previous_week_data,
                index="branch_code",
                values=["code", "conversion"],
                rename={
                    "branch_code": "Outlet",
                    "code": "ETs",
                    "conversion": "Converted",
                },
                cols_order=["Outlet", "ETs", "Converted", "%Conversion"]
            )

        ewc_conversion_prev = create_staff_conversion(
            weekly_data=previous_week_data,
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

        opthom_conversion_prev = create_staff_conversion(
            weekly_data=previous_week_data,
            index=["branch_code", "optom_name"],
            values=["code", "conversion"],
            rename={
                "branch_code": "Outlet",
                "optom_name": "Optom",
                "code": "ETs",
                "conversion": "Converted"
            },
            cols_order=["Outlet", "Optom", "ETs", "Converted", "%Conversion"]
        )


        prev_last_high_rx_data = previous_week_data[previous_week_data["RX"] == "High Rx"]

        highrx_branch_conversion_prev = create_branch_conversion(
            weekly_data=prev_last_high_rx_data,
            index="branch_code",
            values=["code", "conversion"],
            rename={
                "branch_code": "Outlet",
                "code": "ETs",
                "conversion": "Converted",
            },
            cols_order=["Outlet", "ETs", "Converted", "%Conversion"]
        )

        highrx_ewc_conversion_prev = create_staff_conversion(
            weekly_data=prev_last_high_rx_data,
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

        highrx_opthom_conversion_prev = create_staff_conversion(
            weekly_data=prev_last_high_rx_data,
            index=["branch_code", "optom_name"],
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
            ewc.reset_index().to_excel(writer, sheet_name="EWC Oveall Comparison")
            optom.reset_index().to_excel(writer, sheet_name="Optom Ovearall Comparison")
            summary_weekly_conv.to_excel(writer, sheet_name="Summary_Conversion")
            weekly_et_conv.to_excel(writer, sheet_name="Branches_Conversion")
            weekly_highrx_conv.to_excel(writer, sheet_name="Highrx_Conversion")
            branch_high_rx.to_excel(writer, sheet_name="high_rx_branch")
            overall_cash.to_excel(writer, sheet_name="Overall Cash Conversion")
            cash_high_rx_conversion.to_excel(writer, sheet_name="High RX Cash Conversion")
            insurance_high_rx_conversion.to_excel(writer, sheet_name="High RX Insurance Conversion")
            ewc_conversion.to_excel(writer, sheet_name="EWC", index=False)
            ewc_high_rx_comparison.reset_index().to_excel(writer, sheet_name="EWC High RX Comparison")
            optom_high_rx_comparison.reset_index().to_excel(writer, sheet_name="Optom High RX Comparison")
            opthom_conversion.to_excel(writer, sheet_name="Optom", index=False)
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


        with pd.ExcelWriter(f"{path}conversion/eyetests/highrx_sales_persons.xlsx") as writer:
            for group, dataframe in highrx_ewc_conversion.groupby("Outlet"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)

        with pd.ExcelWriter(f"{path}conversion/eyetests/highrx_opthoms.xlsx") as writer:
            for group, dataframe in highrx_opthom_conversion.groupby("Outlet"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)

        with pd.ExcelWriter(f"{path}conversion/eyetests/highrx_branches.xlsx") as writer:
            for group, dataframe in highrx_branch_conversion.groupby("Outlet"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)

        with pd.ExcelWriter(f"{path}conversion/eyetests/non_conversions.xlsx") as writer:
            non_conversions_data.to_excel(
                writer, sheet_name="Master", index=False)
            for group, dataframe in non_conversions_data.groupby("Branch"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)


        with pd.ExcelWriter(f"{path}conversion/eyetests/sales_persons_prev.xlsx") as writer:
            for group, dataframe in ewc_conversion_prev.groupby("Outlet"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)

        with pd.ExcelWriter(f"{path}conversion/eyetests/opthoms_prev.xlsx") as writer:
            for group, dataframe in opthom_conversion_prev.groupby("Outlet"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)

        with pd.ExcelWriter(f"{path}conversion/eyetests/branches_prev.xlsx") as writer:
            for group, dataframe in branch_conversion_prev.groupby("Outlet"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)


        with pd.ExcelWriter(f"{path}conversion/eyetests/highrx_sales_persons_prev.xlsx") as writer:
            for group, dataframe in highrx_ewc_conversion_prev.groupby("Outlet"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)

        with pd.ExcelWriter(f"{path}conversion/eyetests/highrx_opthoms_prev.xlsx") as writer:
            for group, dataframe in highrx_opthom_conversion_prev.groupby("Outlet"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)

        with pd.ExcelWriter(f"{path}conversion/eyetests/highrx_branches_prev.xlsx") as writer:
            for group, dataframe in highrx_branch_conversion_prev.groupby("Outlet"):
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
            index=["branch_code", "staff", "designation"],
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
            "insurance_name": "Insurance Company",
            "conversion_remarks": "Conversion Remarks"
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
            "Order Status",
            "Conversion Remarks"
        ]]

        with pd.ExcelWriter(f"{path}conversion/eyetests/overall.xlsx") as writer:
            country_conversion.to_excel(
                writer, sheet_name="Monthly_Conversion")
            branch_conversion.to_excel(
                writer, sheet_name="Branches_Conversion")
            high_rx_conversion.to_excel(writer, sheet_name="Highrx_Conversion")
            branch_highrx_conversion.to_excel(
                writer, sheet_name="branch_highrx")
            optom_highrx_conversion.reset_index().to_excel(writer, sheet_name="Optom High RX")
            ewc_highrx_conversion.reset_index().to_excel(writer, sheet_name="EWC High RX")
            non_conversions_data.sort_values(by="Branch").to_excel(
                writer,
                sheet_name="Non Conversions",
                index=False
            )

        with pd.ExcelWriter(f"{path}conversion/eyetests/non_conversions.xlsx") as writer:
            non_conversions_data.to_excel(
                writer, sheet_name="Master", index=False)
            for group, dataframe in non_conversions_data.groupby("Branch"):
                name = f'{group}'
                dataframe.to_excel(writer, sheet_name=name, index=False)
