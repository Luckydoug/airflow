import pandas as pd
import numpy as np


def send_3CX_report(path) -> None:
    """
    SECTION I: Inbound Calls Report
    """
    inbound_calls_report = pd.ExcelFile(f"{path}/inbound_calls_report.xlsx")
    call_category = inbound_calls_report.parse(sheet_name="Category", index_col=False)
    category_hourly = inbound_calls_report.parse(sheet_name="Category_Hourly", index_col=False)
    call_agent = inbound_calls_report.parse(sheet_name="Agent", index_col=False)

    """
    SECTION II: Outbound Calls Report
    """
    outbound_calls_report = pd.ExcelFile(f"{path}/outbound_calls_report")
    outbound_agent = outbound_calls_report.parse(sheet_name="Outbound Agent", index_col=False)
    outbound_time = outbound_calls_report.parse(sheet_name="Outbound_Time", index_col=False)
    answered_hourly = outbound_time.parse(sheet_name="Answered_Hourly", index_col=False)

    """
    SECTION III: Missed Calls Report
    """
    missed_calls_report = pd.ExcelFile(f"{path}/missed_calls_report.xlsx")
    missed_calls_summary = missed_calls_report.parse(
        sheet_name="Missed_Calls_Pivot", 
        index_col=False
    )


    missed_calls_time = missed_calls_report.parse(sheet_name="Call_back_Pivot", index_col=False)
    evening_calls = missed_calls_report.parse(
        sheet_name="Evening_Missed_Calls", 
        index_col=False
    )

    missed_calls_data = missed_calls_report.parse(sheet_name="Missed Calls Data", index_col=False)
    not_yet_called = missed_calls_data[missed_calls_data["Call Status"] == "Not Yet Called"]
    not_yet_called = not_yet_called[[
        "Ticket Id",
        "Created Time (Ticket)",
        "Ticket Closed Time",
        "Channel",
        "Phone"
    ]]

