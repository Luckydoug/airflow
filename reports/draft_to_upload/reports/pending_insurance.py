import pandas as pd
import numpy as np


def create_pending_insurance(
    non_conversions: pd.DataFrame,
    order_screen: pd.DataFrame,
    path: str,
    selection: str
):
    if selection == "Weekly" or selection == "Monthly":
        return

    merged_data = pd.merge(
        non_conversions,
        order_screen[["Code", "Request"]],
        on = "Code",
        how="left"
    )
    pending = merged_data[merged_data["Request"].isna()].copy()
    pending["ET Date"] = pd.to_datetime(pending["ET Date"], format="%Y-%m-%d").dt.strftime("%Y-%m-%d")


    with pd.ExcelWriter(f"{path}draft_upload/pending_insurance.xlsx") as writer:
        pending.to_excel(writer, sheet_name = "Data", index = False)
    
