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
    if pd.isna(row["Request"]) and not pd.isna(row["Order Number"]):
        return "Submitted: (Cash/Direct)"
    elif pd.isna(row["Request"]) and pd.isna(row["Order Number"]):
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
    'Conversion'
]

columns_order = [
    "Code",
    "Create Date",
    "Month",
    "RX Type",
    "Status",
    "Branch",
    "Customer Code",
    "Insurance Company",
    "Scheme Name",
    "Opthom Name",
    "EWC Handover",
    "Who Viewed RX",
    "Request",
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

    plano_data["Final Customer Type"] = plano_data.apply(lambda row: get_customer_type(row), axis=1)
    plano_data = plano_data.drop_duplicates(subset=["Code"])

    filtered_plano_data = plano_data[required_columns].rename(columns={"Final Customer Type": "Customer Type"})
    insurance_planos = filtered_plano_data[filtered_plano_data["Customer Type"] == "Insurance"].copy()
    unique_orderscreen = plano_orderscreen.drop_duplicates(subset="Order Number")[["Code", "Request", "Feedback"]]

    plano_orders = all_orders[["Code", "DocNum"]].copy()
    plano_orders = plano_orders.rename(columns={"DocNum": "Order Number"})
    plano_orders["Code"] = plano_orders["Code"].astype(int)
    insurance_planos["Code"] = insurance_planos["Code"].astype(int)
    insurance_plano_requests = pd.merge(insurance_planos, unique_orderscreen, on = "Code", how="left")
    plano_insurance_orders = pd.merge(insurance_plano_requests, plano_orders, on = "Code", how="left")

    plano_insurance_orders["Submission"] = plano_insurance_orders.apply(lambda row: check_plano_submission(row), axis=1)
    plano_insurance_orders["Month"] = pd.to_datetime(plano_insurance_orders["Create Date"], format="%Y-%m-%d").dt.month_name()

    final_plano_data = plano_insurance_orders[columns_order]
    final_plano_data = final_plano_data.drop_duplicates(subset=["Code"])

    final_plano_data["Create Date"] = pd.to_datetime(final_plano_data["Create Date"]).dt.date
    final_plano_data = pd.merge(
        final_plano_data, 
        branch_data[["Outlet", "RM", "SRM"]].rename(columns = {"Outlet": "Branch"}),
        on = "Branch",
        how = "left"
    )

    final_plano_data = final_plano_data[
        final_plano_data["Insurance Company"] != "NHIF- COMPREHENSIVE MEDICAL INSURANCE"
    ]

    if selection == "Daily":
        daily_plano_data = final_plano_data[final_plano_data["Create Date"] == get_yesterday_date(truth=True)]

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

        with pd.ExcelWriter(f"{path}draft_upload/planorx_not_submitted.xlsx") as writer:
            daily_submission_branch.to_excel(writer, sheet_name="daily_submission_branch", index=False)
            daily_submission_optom.to_excel(writer, sheet_name="daily_submission_optom", index=False)
            daily_submission_ewc.to_excel(writer, sheet_name="daily_submission_ewc", index=False)
            daily_plano_data.sort_values(by="Submission").to_excel(writer, sheet_name="daily_data", index=False)

    if selection == "Weekly":
        weekly_plano_data = final_plano_data.copy()
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
            index=["SRM", "RM", "Branch","Opthom Name"],
            set_index=["SRM", "RM", "Branch","Opthom Name"],
            month=False
        )

        weekly_submission_ewc = plano_submission_multindex(
            plano_data=weekly_plano_data,
            columns = ["Week Range"],
            index=["SRM", "RM", "Branch", "EWC Handover"],
            set_index=["SRM", "RM", "Branch", "EWC Handover"],
            month=False
        )

        with pd.ExcelWriter(f"{path}draft_upload/planorx_not_submitted.xlsx") as writer:
            weekly_submission_branch.to_excel(writer, sheet_name="weekly_submission_branch")
            weekly_submission_optom.to_excel(writer, sheet_name="weekly_submission_optom")
            weekly_submission_ewc.to_excel(writer, sheet_name="weekly_submission_ewc")
            weekly_plano_data.sort_values(by="Submission").to_excel(writer, sheet_name="weekly_data", index=False)


    if selection == "Monthly":
        monthly_plano_data = final_plano_data.copy()
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
            monthly_plano_data.sort_values(by="Submission").to_excel(writer, sheet_name="monthly_data", index=False)
        





        



