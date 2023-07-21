import pandas as pd
import numpy as np
from airflow.models import variable
from sub_tasks.libraries.time_diff import calculate_time_difference
from pangres import upsert
from reports.draft_to_upload.utils.utils import return_slade, today

def push_insurance_efficiency_data(database, engine, orderscreen, all_orders, start_date, branch_data,working_hours):
    if not len(all_orders) or not len(orderscreen):
        return
    draft = ["Draft Order Created"]
    drafts = orderscreen[orderscreen.Status.isin(draft)].copy()
    draft_sorted = drafts.sort_values(by=["Date", "Time"], ascending=True)
    unique_draft = draft_sorted.drop_duplicates(
        subset="Order Number", 
        keep="first"
    ).copy()
    unique_draft.loc[:, "Draft Time"] = pd.to_datetime(
        unique_draft["Date"].astype(str) + " " +
        unique_draft["Time"].astype(str),
        format="%Y-%m-%d %H:%M:%S"
    )

    upload = ["Upload Attachment"]
    upload_attachments = orderscreen[orderscreen.Status.isin(upload)].copy()
    upload_sorted = upload_attachments.sort_values(
        by=["Date", "Time"], 
        ascending=True
    )
    unique_uploads = upload_sorted.drop_duplicates(
        subset="Order Number", 
        keep="first"
    ).copy()
    unique_uploads.loc[:, "Upload Time"] = pd.to_datetime(
        unique_uploads["Date"].astype(str) + " " +
        unique_uploads["Time"].astype(str),
        format="%Y-%m-%d %H:%M:%S"
    )

    initiated = [
        "Pre-Auth Initiated For Optica Insurance",
        "Insurance Order Initiated", 
        "SMART to be captured"
    ]

    preauth_initiated = orderscreen[orderscreen.Status.isin(initiated)].copy()
    preauth_sorted = preauth_initiated.sort_values(
        by=["Date", "Time"], 
        ascending=True
    )
    unique_preauths = preauth_sorted.drop_duplicates(
        subset="Order Number", 
        keep="first"
    ).copy()
    unique_preauths.loc[:, "Preauth Time"] = pd.to_datetime(
        unique_preauths["Date"].astype(str) + " "
        + unique_preauths["Time"].astype(str),
        format="%Y-%m-%d %H:%M:%S"
    )

    final_data1 = pd.merge(
        unique_uploads[["Order Number", "Upload Time"]],
        unique_preauths[["Order Number", "Preauth Time"]],
        on="Order Number",
        how="left"
    )
    final_data = pd.merge(
        unique_draft[["Order Number", "Draft Time"]],
        final_data1, on="Order Number",
        how="right"
    )

    final_data_orders = pd.merge(
        final_data,
        all_orders[[
            "DocNum", "Code", "Customer Code", "Last View Date",
            "Status", "Outlet", "Insurance Company",
            "Insurance Scheme", "Scheme Type",
            "Feedback 1", "Feedback 2",
            "Creator", "Order Creator"
        ]].rename(columns={"DocNum": "Order Number"}),
        on="Order Number",
        how="left"
    )

    final_data_orders["Month"] = final_data_orders["Preauth Time"].dt.month_name()
    final_data_orders = final_data_orders[
        (final_data_orders["Upload Time"] >= pd.to_datetime(start_date)) &
        (final_data_orders["Upload Time"] <= pd.to_datetime(today))
    ]

    final_data_orders = final_data_orders.dropna(subset=["Draft Time"])
    final_data_orders = final_data_orders.dropna(subset=["Outlet"])

    if not len(final_data_orders):
        return

    final_data_orders["Draft to Preauth"] = final_data_orders.apply(
        lambda row: calculate_time_difference(
            row=row,
            x="Draft Time",
            y="Preauth Time",
            working_hours=working_hours
        ),
        axis=1
    )

    final_data_orders["Preuth to Upload"] = final_data_orders.apply(
        lambda row: calculate_time_difference(
            row=row,
            x="Preauth Time",
            y="Upload Time",
            working_hours=working_hours
        ),
        axis=1
    )

    final_data_orders["Draft to Upload"] = final_data_orders.apply(
        lambda row: calculate_time_difference(
            row=row,
            x="Draft Time",
            y="Upload Time",
            working_hours=working_hours
        ),
        axis=1
    )

    final_data_orders = pd.merge(
        final_data_orders,
        branch_data[["Outlet", "RM", "SRM", "Front Desk"]],
        on = "Outlet",
        how = "left"
    )

    final_data_orders["Slade"] = final_data_orders.apply(lambda row: return_slade(row), axis=1)
    final_data_orders = final_data_orders.drop_duplicates(
        subset=["Order Number"]
    )

    data_to_upload = final_data_orders.copy()
    rename_data_to_upload = data_to_upload.rename(columns= {
        "Order Number": "order_number",
        "Customer Code": "customer_code",
        "Outlet": "outlet",
        "Front Desk": "front_desk",
        "Creator": "creator",
        "Order Creator": "order_creator",
        "Draft Time": "draft_time",
        "Preauth Time": "preauth_time",
        "Upload Time": "upload_time",
        "Draft to Preauth": "draft_to_preauth",
        "Preuth to Upload": "preauth_to_upload",
        "Draft to Upload": "draft_to_upload",
        "Insurance Company": "insurance_company",
        "Slade": "slade",
        "Insurance Scheme": "insurance_scheme",
        "Scheme Type": "scheme_type",
        "Feedback 1": "feedback_1",
        "Feedback 2": "feedback_2"
    })

    final_data_to_upload = rename_data_to_upload[[
        "order_number",
        "customer_code",
        "outlet",
        "front_desk",
        "creator",
        "order_creator",
        "draft_time",
        "preauth_time",
        "upload_time",
        "draft_to_preauth",
        "preauth_to_upload",
        "draft_to_upload",
        "insurance_company",
        "slade",
        "insurance_scheme",
        "scheme_type",
        "feedback_1",
        "feedback_2"
    ]].set_index("order_number")

    upsert(
        engine=engine,
        df=final_data_to_upload,
        schema=database,
        table_name="source_insurance_efficiency",
        if_row_exists='update',
        create_table=True
    )

    """
    This is the Daily Report. 
    Since this code will be running on Airflow, we can't afford to enter the dates manually. 
    There are three reports available: Daily, Weekly, and Monthly reports. 
    Depending on the day, we want the code to determine whether it should send the daily, weekly or monthly report.

"""