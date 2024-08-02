import pandas as pd
from datetime import datetime
from datetime import timedelta, time
from reports._3CX.utils.utils import save_file
from reports._3CX.utils.utils import return_status
from reports._3CX.utils.utils import check_time_range
from reports._3CX.utils.utils import get_yesterday_date
from reports._3CX.utils.utils import extract_phone_number
from reports._3CX.utils.utils import calculate_time_difference


def create_missed_calls_report(
    sender: str,
    path: str,
    subject: str,
    has_attachment: bool,
    call_logs_subject: str,
    start_date: str,
) -> None:

    missed_calls = pd.read_csv(
        save_file(
            sender=sender,
            subject=subject,
            has_attachment=has_attachment,
            date_since=start_date,
            path=path,
        ),
        skiprows=4,
        encoding="latin1",
    )[:-2]

    call_logs = pd.read_csv(
        save_file(
            sender=sender,
            subject=subject,
            has_attachment=has_attachment,
            date_since=start_date,
            path=path,
        ),
        skiprows=3,
        encoding="latin1",
    )[:-2]

    missed_calls["Phone (Ticket)"] = (
        missed_calls["Phone (Ticket)"].astype(int).astype(str)
    )
    call_logs["Talking"] = call_logs.apply(lambda row: return_status(row), axis=1)
    call_logs = call_logs[["Call Time", "Caller ID", "Destination", "Talking"]]
    call_logs = call_logs[call_logs["Destination"].str.startswith("0")]

    call_logs["Destination"] = call_logs["Destination"].str.lstrip("0")
    call_logs = call_logs.rename(columns={"Destination": "Dest"})
    call_logs["Destination"] = call_logs["Dest"].apply(extract_phone_number)
    call_logs = call_logs.sort_values(by="Call Time", ascending=True)
    call_logs = call_logs.drop_duplicates(subset=["Destination"], keep="last")

    calls_missed = missed_calls.copy()
    calls_missed["Phone"] = calls_missed["Phone (Ticket)"].apply(extract_phone_number)
    calls_missed = calls_missed.dropna(subset=["Phone"])
    calls_missed["Phone"] = calls_missed["Phone"].astype(float)

    today = datetime.today().date()
    to_date = today - timedelta(days=0)
    calls = call_logs.copy()
    calls["Destination"] = calls["Destination"].astype(float)
    returned_calls = pd.merge(
        calls_missed, calls, right_on="Destination", left_on="Phone", how="left"
    )
    returned_calls["CreateDate"] = pd.to_datetime(
        returned_calls["Created Time (Ticket)"], format="%d %b %Y %I:%M %p"
    )

    returned_calls["Date"] = returned_calls["CreateDate"].dt.date
    returned_calls = returned_calls.drop_duplicates(
        subset=["CreateDate", "Phone (Ticket)"]
    )
    from_date = get_yesterday_date()
    returned_calls = returned_calls[
        (returned_calls["CreateDate"].dt.date >= from_date)
        & (returned_calls["CreateDate"].dt.date < to_date)
    ]

    evening_calls = returned_calls[
        (returned_calls["Date"].astype(str) == str(from_date))
        & (returned_calls["CreateDate"].dt.hour >= 18)
    ].copy()
    evening_calls = evening_calls.drop_duplicates(subset=["Phone"])

    calls_called = returned_calls[
        (pd.to_datetime(returned_calls["Date"]).dt.date == returned_calls.Date.max())
        & (returned_calls["CreateDate"].dt.hour < 16)
    ]
    calls_called = calls_called.drop_duplicates(subset="Phone")

    evening_calls["Time Range"] = evening_calls["Call Time"].apply(check_time_range)
    evening_calls_pivot = (
        pd.pivot_table(
            evening_calls, index="Time Range", values="Ticket Id", aggfunc="count"
        )
        .reset_index()
        .sort_values(by="Time Range", ascending=False)
    )

    evening_calls_pivot["Time Range"] = pd.Categorical(
        evening_calls_pivot["Time Range"],
        categories=[
            "9:00AM - 9:30AM",
            "9:31AM - 10:00AM",
            "10:01AM - 10:30AM",
            "10:31AM - 11:00AM",
            "11: 01AM - 5:59PM",
            "Not Called",
        ],
    )

    evening_calls_pivot = evening_calls_pivot.sort_values(by="Time Range")
    evening_calls_pivot.rename(columns={"Ticket Id": "Count"})

    called_back = calls_called[(calls_called["Call Status"] != "Not Yet Called")].copy()
    called_back["Created Time (Ticket)"] = pd.to_datetime(called_back["Created Time (Ticket)"], format="%d %b %Y %I:%M %p")
    called_back["Call Time"] = pd.to_datetime(called_back["Call Time"], format="%m/%d/%Y %I:%M:%S %p")

    not_called_evening = evening_calls[
        ["Created Time (Ticket)", "Ticket Closed Time", "Phone", "Call Status"]
    ]
    not_called_evening = not_called_evening[
        not_called_evening["Call Status"] == "Not Yet Called"
    ]
    not_called_evening = evening_calls[
        ["Created Time (Ticket)", "Ticket Closed Time", "Phone", "Call Status"]
    ]
    not_called_evening = not_called_evening[
        not_called_evening["Call Status"] == "Not Yet Called"
    ]

    called_back["Time Difference"] = called_back.apply(
        lambda row: calculate_time_difference(
            row, "Created Time (Ticket)", "Call Time"
        ),
        axis=1,
    )

    called_back_pivot = pd.pivot_table(
        called_back, columns="Category", values="Phone (Ticket)", aggfunc="count"
    )
    categories = [
        "0 - 5 (Mins)",
        "6 - 10 (Mins)",
        "11 - 15 (Mins)",
        "16 - 20 (Mins)",
        "20 (Mins) +",
    ]
    called_back_pivot = called_back_pivot.reindex(categories, axis=1, fill_value=0)

    missed_calls_pivot = pd.pivot_table(
        calls_called, values=["Ticket Id"], aggfunc="count", columns="Call Status"
    )
    categories = ["Answered", "Unanswered", "Not Yet Called"]

    missed_calls_pivot = missed_calls_pivot.reindex(categories, axis=1, fill_value=0)
    missed_calls_pivot["Total Missed Calls"] = (
        missed_calls_pivot["Answered"]
        + missed_calls_pivot["Unanswered"]
        + missed_calls_pivot["Not Yet Called"]
    )

    missed_calls_pivot["Called Back"] = (
        missed_calls_pivot["Answered"] + missed_calls_pivot["Unanswered"]
    )

    missed_calls_pivot = missed_calls_pivot[
        [
            "Total Missed Calls",
            "Called Back",
            "Answered",
            "Unanswered",
            "Not Yet Called",
        ]
    ]

    missed_calls_pivot["%Called Back"] = (
        missed_calls_pivot["Called Back"]
        / missed_calls_pivot["Total Missed Calls"]
        * 100
    ).round(0).fillna(0).astype(int).astype(str) + "%"

    missed_calls_pivot["%Answered"] = (
        missed_calls_pivot["Answered"] / missed_calls_pivot["Called Back"] * 100
    ).fillna(0).round(0).astype(int).astype(str) + "%"

    cols = [
        "Ticket Id", 
        "Created Time (Ticket)", 
        "Ticket Closed Time", 
        "Phone","Caller ID", 
        "Destination",
        "Talking", 
        "Call Status"
    ]

    with pd.ExcelWriter(f"{path}_3CX/missed_calls_report.xlsx") as writer:
        missed_calls_pivot.to_excel(writer, sheet_name="Missed_Calls_Pivot", index=False)
        called_back_pivot.to_excel(writer, sheet_name="Call_Back_Pivot", index=False)
        evening_calls_pivot.to_excel(writer, sheet_name="Evening_Missed_Calls", index = False)
        calls_called[cols].sort_values(by = "Call Status", ascending = False).to_excel(writer, sheet_name = "Missed Calls Data")
