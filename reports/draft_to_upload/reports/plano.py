import pandas as pd
import numpy as np
from airflow.models import variable
from sub_tasks.libraries.utils import (
    get_yesterday_date, 
    check_date_range,
    get_comparison_months
)
from reports.draft_to_upload.utils.utils import(
    create_daily_submission_pivot, 
    plano_submission_multindex
)
from reports.queue_time.utils.utils import check_date_range as check_range
first_month, second_month = get_comparison_months()

def get_customer_type(row):
    if pd.isna(row["Mode of Pay"]):
        if row["Customer Type"] == "Insurance":
            return "Insurance"
        else:
            return "Cash"
    else:
        return row["Mode of Pay"]
    
def check_plano_submission(row):
    if pd.isna(row["Request"]) and not pd.isna(row["Order Number"]) and row["Customer Type"] == "Insurance" and row["Conversion"] == 0:
        return "Not Submitted"
    if pd.isna(row["Request"]) and not pd.isna(row["Order Number"]):
        return "Submitted: (Cash/Direct)"
    elif pd.isna(row["Request"]) and pd.isna(row["Order Number"]) and row["Conversion"] == 0:
        return "Not Submitted"
    else:
        return "Submitted"
    
required_columns = [ 
    'Insurance Company',
    'Scheme Name',
    'EWC Handover',
    'Who Viewed RX',
    'Code', 
    'Status', 
    'Customer Code', 
    'Branch', 
    'Create Date', 
    'RX Type', 
    'Opthom Name', 
    'Final Customer Type',
    'Conversion',
    'Plano RX'
]

columns_order = [
    "Create Date",
    "SRM",
    "RM",
    "Code",
    "Plano RX",
    "RX Type",
    "Status",
    "Branch",
    "Customer Code",
    "Insurance Company",
    "Scheme Name",
    "Optom Name",
    "EWC Handover",
    "Who Viewed RX",
    "Feedback",
    "Order Number",
    "Submission",
    "Conversion"
]

def create_plano_report(branch_data, path, registrations, payments, all_planos, plano_orderscreen, all_orders, selection):
    enrollment = registrations.copy()
    enrollment = enrollment[["Customer Code", "Customer Type"]]
    payments["Customer Code"] = payments["Customer Code"].astype(str)
    planos_payments = pd.merge(all_planos, payments, on = "Customer Code", how = "left")
    planos_payments_registrations = pd.merge(planos_payments, enrollment, on = "Customer Code", how = "left")
    plano_data = planos_payments_registrations.copy()
    print(plano_data)
    plano_data["Final Customer Type"] = plano_data.apply(lambda row: get_customer_type(row), axis=1)
    
    plano_data = plano_data.drop_duplicates(subset=["Code"])
    plano_data = plano_data[plano_data["Code"] != '4134']

    filtered_plano_data = plano_data[required_columns].rename(columns={"Final Customer Type": "Customer Type"})
    insurance_planos = filtered_plano_data[filtered_plano_data["Customer Type"] == "Insurance"].copy()
    unique_orderscreen = plano_orderscreen.drop_duplicates(subset="Order Number")[["Code", "Request", "Feedback"]]

    plano_orders = all_orders[["Code", "DocNum"]].copy()
    plano_orders = plano_orders.rename(columns={"DocNum": "Order Number"})
    plano_orders["Code"] = plano_orders["Code"].astype(int)
    insurance_planos["Code"] = insurance_planos["Code"].astype(int)
    insurance_plano_requests = pd.merge(insurance_planos, unique_orderscreen, on = "Code", how="left")
    insurance_plano_requests["Code"] = insurance_plano_requests["Code"].astype(int)
    plano_insurance_orders = pd.merge(insurance_plano_requests, plano_orders, on = "Code", how="left")
    

    plano_insurance_orders["Submission"] = plano_insurance_orders.apply(lambda row: check_plano_submission(row), axis=1)
    plano_insurance_orders["Month"] = pd.to_datetime(plano_insurance_orders["Create Date"], format="%Y-%m-%d").dt.month_name()

    final_plano_data = plano_insurance_orders
    final_plano_data = final_plano_data.drop_duplicates(subset=["Code"])

    final_plano_data["Create Date"] = pd.to_datetime(final_plano_data["Create Date"]).dt.date
    final_plano_data = pd.merge(
        final_plano_data, 
        branch_data[["Outlet", "RM", "SRM"]].rename(columns = {"Outlet": "Branch"}),
        on = "Branch",
        how = "left"
    )

    # final_plano_data = final_plano_data[
    #     final_plano_data["Insurance Company"] != "NHIF- COMPREHENSIVE MEDICAL INSURANCE"
    # ]

    if selection == "Daily":
        daily_plano_data = final_plano_data[final_plano_data["Create Date"] == get_yesterday_date(truth=False)]
        if not len(daily_plano_data):
            return
        
        daily_submission_branch = create_daily_submission_pivot(
            plano_data=daily_plano_data,
            index=["SRM", "RM", "Branch"],
            cols=[ "SRM", "RM", "Branch", "Plano Eye Tests", "Converted", "Submitted", "Not Submitted"],
            cols_order=["SRM", "RM", "Branch", "Plano Eye Tests", "Submitted", "Not Submitted", "Converted"]
        )

        daily_submission_optom = create_daily_submission_pivot(
            plano_data=daily_plano_data,
            index=["SRM", "RM", "Branch", "Opthom Name"],
            cols=["SRM", "RM", "Branch","Opthom Name", "Plano Eye Tests", "Converted", "Submitted", "Not Submitted"],
            cols_order=["SRM", "RM", "Branch","Opthom Name", "Plano Eye Tests", "Submitted", "Not Submitted", "Converted"]
        )

        daily_submission_ewc = create_daily_submission_pivot(
            plano_data=daily_plano_data,
            index=["SRM", "RM", "Branch", "EWC Handover"],
            cols=["SRM", "RM", "Branch","EWC Handover", "Plano Eye Tests", "Converted", "Submitted", "Not Submitted"],
            cols_order=["SRM", "RM", "Branch","EWC Handover", "Plano Eye Tests", "Submitted", "Not Submitted", "Converted"]
        )

        daily_plano_data["Create Date"] = daily_plano_data["Create Date"].astype(str)
        daily_plano_data = daily_plano_data.rename(columns = {"Opthom Name": "Optom Name"})

        with pd.ExcelWriter(f"{path}draft_upload/planorx_not_submitted.xlsx") as writer:
            daily_submission_branch.to_excel(writer, sheet_name="daily_submission_branch", index=False)
            daily_submission_optom.to_excel(writer, sheet_name="daily_submission_optom", index=False)
            daily_submission_ewc.sort_values(by = "Not Submitted", ascending = False).to_excel(writer, sheet_name="daily_submission_ewc", index=False)
            daily_plano_data[columns_order].sort_values(by="Submission").to_excel(writer, sheet_name="daily_data", index=False)

    if selection == "Weekly":
        weekly_plano_data = final_plano_data.copy()
        weekly_plano_data = weekly_plano_data.rename(columns={"Opthom Name": "Optom Name"})
        weekly_plano_data["Week Range"] = weekly_plano_data.apply(lambda row: check_date_range(row, "Create Date"), axis=1)
        weekly_plano_data = weekly_plano_data[
            (weekly_plano_data["Week Range"] != "None") & 
            (weekly_plano_data["Branch"] != "HOM")
        ]

        weekly_submission_branch = plano_submission_multindex(
            plano_data=weekly_plano_data,
            columns = ["Week Range"], 
            index=["SRM", "RM", "Branch"],
            set_index=["SRM", "RM", "Branch"],
            month=False
        )

        weekly_submission_optom = plano_submission_multindex(
            plano_data=weekly_plano_data,
            columns = ["Week Range"],
            index=["SRM", "RM", "Branch","Optom Name"],
            set_index=["SRM", "RM", "Branch","Optom Name"],
            month=False
        )

        weekly_submission_ewc = plano_submission_multindex(
            plano_data=weekly_plano_data,
            columns = ["Week Range"],
            index=["SRM", "RM", "Branch", "EWC Handover"],
            set_index=["SRM", "RM", "Branch", "EWC Handover"],
            month=False
        )


        export_data = weekly_plano_data[
            weekly_plano_data["Week Range"] == weekly_submission_ewc.columns.get_level_values(0)[-1]
        ]
            

        weekly_submission_ewc_two = create_daily_submission_pivot(
            plano_data= export_data,
            index=["SRM", "RM", "Branch", "EWC Handover"],
            cols=["SRM", "RM", "Branch","EWC Handover", "Plano Eye Tests", "Converted", "Submitted", "Not Submitted"],
            cols_order=["SRM", "RM", "Branch","EWC Handover", "Plano Eye Tests", "Submitted", "Not Submitted", "Converted"]
        )

        weekly_submission_branch_two = create_daily_submission_pivot(
            plano_data=export_data,
            index=["SRM", "RM", "Branch"],
            cols=[ "SRM", "RM", "Branch", "Plano Eye Tests", "Converted", "Submitted", "Not Submitted"],
            cols_order=["SRM", "RM", "Branch", "Plano Eye Tests", "Submitted", "Not Submitted", "Converted"]
        )

        branch_checks = weekly_plano_data.copy()
        branch_checks = branch_checks[branch_checks["Submission"] == "Not Submitted"]

        weekly_plano_data = weekly_plano_data[
            weekly_plano_data["Week Range"] == weekly_submission_ewc.columns.get_level_values(0)[-1]
        ]

        branch_checks = branch_checks.rename(columns = {"Branch": "Outlet"})

        branch_checks["Week Range"] = branch_checks.apply(lambda row: check_range(row, "Create Date"), axis = 1)
        branch_checks = branch_checks[branch_checks["Week Range"] != "None"]
        
        plano_branch_checks = pd.pivot_table(
            branch_checks,
            index = ["Outlet", "EWC Handover"],
            values = [
                "Code",
            ],
            aggfunc={
                "Code": "count",
                },
            columns=["Week Range"]
        )

        plano_branch_checks =  plano_branch_checks.reindex(
            ["Code"], 
            level = 0, 
            axis = 1
        )


        columns = plano_branch_checks.columns.get_level_values(1)
        dates = []

        for date in columns:
            stat_date = pd.to_datetime(date.split(" to ")[0], format="%b-%d")
            dates.append(stat_date)
        unique_dates = list(set(dates))
        sorted_dates = sorted(unique_dates)
        sorted_dates

        sorted_columns = []

        for date in sorted_dates:
            date_range = f"{date.strftime('%b-%d')} to " + f"{(date + pd.Timedelta(6, unit='d')).strftime('%b-%d')}"
            sorted_columns.append(date_range)

        

        plano_branch_checks = plano_branch_checks.reindex(sorted_columns, axis = 1, level=1)

        plano_branch_checks = plano_branch_checks.rename(
            columns = {
                "Code": "Non Submitted Insurance Clients",
            },
            level = 0).round(0).fillna("-")

        plano_branch_checks =  plano_branch_checks.reset_index()


        with pd.ExcelWriter(f"{path}draft_upload/insurance_not_submitted.xlsx") as writer:
            plano_branch_checks.to_excel(writer, sheet_name="weekly_submission_branch")


        with pd.ExcelWriter(f"{path}draft_upload/planorx_not_submitted.xlsx") as writer:
            # weekly_submission_branch.to_excel(writer, sheet_name="weekly_submission_branch")
            weekly_submission_branch_two.to_excel(writer, sheet_name="weekly_submission_branch", index = False)
            weekly_submission_optom.to_excel(writer, sheet_name="weekly_submission_optom")
            # weekly_submission_ewc.to_excel(writer, sheet_name="weekly_submission_ewc")
            weekly_plano_data[columns_order].sort_values(by="Submission").to_excel(writer, sheet_name="weekly_data", index=False)
            weekly_submission_ewc_two.to_excel(writer, sheet_name="weekly_submission_ewc", index=False)



    if selection == "Monthly":
        from reports.draft_to_upload.utils.utils import return_report_daterange
        from reports.draft_to_upload.utils.utils import today
        start_date = return_report_daterange("Monthly")

        monthly_plano_data = final_plano_data.copy()
        monthly_plano_data = monthly_plano_data[
            (monthly_plano_data["Create Date"] >= pd.to_datetime(start_date))
        ]
       
        monthly_plano_data = monthly_plano_data[
            (monthly_plano_data["Month"] == first_month) |
            (monthly_plano_data["Month"] == second_month)
        ]

        monthly_submission_branch = plano_submission_multindex(
            plano_data=monthly_plano_data ,
            columns = ["Month"], 
            index=["SRM", "RM", "Branch"],
            set_index=["SRM", "RM", "Branch"],
            month=True
        )

        monthly_submission_optom = plano_submission_multindex(
            plano_data=monthly_plano_data ,
            columns = ["Month"],
            index=["SRM", "RM", "Branch","Opthom Name"],
            set_index=["SRM", "RM", "Branch","Opthom Name"],
            month=True
        )

        monthly_submission_ewc = plano_submission_multindex(
            plano_data=monthly_plano_data,
            columns = ["Month"],
            index=["SRM", "RM", "Branch", "EWC Handover"],
            set_index=["SRM", "RM", "Branch", "EWC Handover"],
            month=True
        )

        with pd.ExcelWriter(f"{path}draft_upload/planorx_not_submitted.xlsx") as writer:
            monthly_submission_branch.to_excel(writer, sheet_name = "monthly_submission_branch")
            monthly_submission_optom.to_excel(writer, sheet_name = "monthly_submission_optom")
            monthly_submission_ewc.to_excel(writer, sheet_name = "monthly_submission_ewc")
            final_plano_data.sort_values(by="Submission").to_excel(writer, sheet_name="monthly_data", index=False)
        





        



