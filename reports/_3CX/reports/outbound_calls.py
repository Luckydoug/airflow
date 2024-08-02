from airflow.models import variable
from reports._3CX.utils.utils import save_file
from reports._3CX.utils.utils import call_status
from reports._3CX.utils.utils import custom_mean
import pandas as pd


def create_outbound_calls_report(
    sender: str,
    path: str,
    subject: str,
    has_attachment: bool,
    start_date: str,
    end_hour: int,
):
    outbound_calls = pd.read_csv(
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

    outbound_calls = outbound_calls.drop(
        columns=["Unnamed: 2", "Unnamed: 11", "Play"], axis=1
    )

    calls_outbound = outbound_calls.copy()
    calls_outbound["Call Time"] = pd.to_datetime(
        calls_outbound["Call Time"], format="%m/%d/%Y %I:%M:%S %p"
    )

    calls_outbound["Time"] = calls_outbound["Call Time"].dt.time
    calls_outbound = calls_outbound[
        (calls_outbound["Call Time"].dt.hour > 8)
        & (calls_outbound["Call Time"].dt.hour < end_hour)
    ]

    calls_outbound["Talk Time"] = (
        pd.to_timedelta(calls_outbound["Talking"]).dt.total_seconds().astype(int)
    ) / 60
    calls_outbound["Total Time"] = (
        pd.to_timedelta(calls_outbound["Totals"])
        .fillna(pd.Timedelta("00:00:00"))
        .dt.total_seconds()
        .astype(int)
    ) / 60
    calls_outbound["TT Time"] = (
        pd.to_timedelta(calls_outbound["Talking"]).dt.total_seconds().astype(int)
    )

    calls_outbound = calls_outbound[~calls_outbound["Caller ID"].str.startswith("7")]
    calls_outbound = calls_outbound[~calls_outbound["Caller ID"].str.startswith("+254")]

    calls_outbound = calls_outbound[
        (calls_outbound["Destination"].str.startswith("0"))
        | (calls_outbound["Destination"].str.startswith("1"))
    ]

    calls_outbound["Hour"] = calls_outbound["Call Time"].dt.hour
    calls_outbound[["Call Status", "Talking Time", "Ringing Time"]] = (
        calls_outbound.apply(lambda row: call_status(row), axis=1, result_type="expand")
    )

    calls_outbound = calls_outbound.drop_duplicates(
        subset=["Call Time", "Caller ID", "Destination"]
    )

    outbound_pivot = (
        pd.pivot_table(
            calls_outbound,
            index="Caller ID",
            values=["Time", "Talking Time", "Call Status", "Ringing Time"],
            aggfunc={
                "Time": ["count", "min", "max"],
                "Talking Time": ["sum", custom_mean],
                "Ringing Time": ["sum"],
                "Call Status": [
                    lambda x: (x == "Unreachable").sum(),
                    lambda x: (x == "Answered").sum(),
                    lambda x: (x == "Unanswered").sum(),
                ],
            },
        )
        .fillna(0)
        .reset_index()
    )

    outbound_pivot.columns = [
        "Agent",
        "Unreachable",
        "Answered",
        "Unanswered",
        "Ringing Time (Unaswered)",
        "Average Time(Min)",
        "Total Time(Min)",
        "Total Calls",
        "Last Call",
        "First Call",
    ]

    outbound_pivot = outbound_pivot[
        [
            "Agent",
            "First Call",
            "Last Call",
            "Total Calls",
            "Unanswered",
            "Unreachable",
            "Answered",
            "Total Time(Min)",
            "Average Time(Min)",
            "Ringing Time (Unaswered)",
        ]
    ]

    outbound_pivot["Total Time(Min)"] = pd.to_timedelta(
        outbound_pivot["Total Time(Min)"], unit="m"
    )
    outbound_pivot["Average Time(Min)"] = pd.to_timedelta(
        outbound_pivot["Average Time(Min)"], unit="m"
    )
    outbound_pivot["Ringing Time (Unaswered)"] = pd.to_timedelta(
        outbound_pivot["Ringing Time (Unaswered)"], unit="m"
    )

    outbound_pivot["Total Time(Min)"] = (
        (outbound_pivot["Total Time(Min)"].dt.total_seconds() / 60)
        .astype(int)
        .astype(str)
        .str.zfill(2)
        + ":"
        + (outbound_pivot["Total Time(Min)"].dt.total_seconds() % 60)
        .astype(int)
        .astype(str)
        .str.zfill(2)
    )

    outbound_pivot["Average Time(Min)"] = (
        (outbound_pivot["Average Time(Min)"].dt.total_seconds() / 60)
        .astype(int)
        .astype(str)
        .str.zfill(2)
        + ":"
        + (outbound_pivot["Average Time(Min)"].dt.total_seconds() % 60)
        .fillna(0)
        .astype(int)
        .astype(str)
        .str.zfill(2)
    )

    outbound_pivot["Ringing Time (Unaswered)"] = (
        (outbound_pivot["Ringing Time (Unaswered)"].dt.total_seconds() / 60)
        .astype(int)
        .astype(str)
        .str.zfill(2)
        + ":"
        + (outbound_pivot["Ringing Time (Unaswered)"].dt.total_seconds() % 60)
        .astype(int)
        .astype(str)
        .str.zfill(2)
    )

    oubound_answered = calls_outbound[calls_outbound["Call Status"] == "Answered"]
    outbound_calls_time = (
        pd.pivot_table(
            oubound_answered,
            index="Caller ID",
            columns="Hour",
            values="Call Time",
            aggfunc="count",
            margins=True,
        )
        .fillna(0)
        .reset_index()
    )


    with pd.ExcelWriter(f"{path}_3CX/outbound_calls_report.xlsx") as writer:
        outbound_pivot.to_excel(writer, sheet_name = "Outbound_Agent", index = False)
        outbound_calls_time.sort_values(by = "All", ascending=True).to_excel(writer, sheet_name = "Outbound_Time", index=False)
