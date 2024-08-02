from airflow.models import variable
import pandas as pd
from reports._3CX.utils.utils import save_file, count_direct, count_na
from sub_tasks.libraries.utils import uganda_path


def create_inbound_calls_report(
    sender, path, subject, has_attachment, start_date, end_hour
):
    inbound_calls = pd.read_csv(
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
    inbound_calls = inbound_calls.drop(
        columns=["Unnamed: 2", "Unnamed: 11", "Play"], axis=1
    )
    direct_calls = inbound_calls[
        (inbound_calls["Destination"].str.lower().str.contains("cs"))
        & (inbound_calls["Call Time"].notna())
        & (inbound_calls["Status"] == "answered")
    ].copy()

    in_calls = inbound_calls.copy()

    in_calls["Call Time"] = in_calls["Call Time"].fillna(method="ffill")
    in_calls["Talking"] = pd.to_timedelta(in_calls["Talking"])
    in_calls["Time"] = in_calls["Talking"].dt.total_seconds().astype(int)
    in_calls = in_calls[in_calls["Destination"] == "Q CALL CENTRE (800)"]
    answer = in_calls[
        (in_calls["Status"] == "answered")
        & (in_calls["Reason"].str.lower().str.contains("cs"))
    ]
    unsanswer = in_calls[
        (in_calls["Status"] == "unanswered") & (in_calls["Time"] > 5)
    ].copy()
    call_center_calls = pd.concat([answer, unsanswer], ignore_index=True)

    center_calls = call_center_calls.copy()
    center_calls["Call Time"] = pd.to_datetime(
        center_calls["Call Time"], format="%m/%d/%Y %I:%M:%S %p"
    ).copy()
    center_calls = center_calls[
        (center_calls["Call Time"].dt.hour < end_hour)
        & (center_calls["Call Time"].dt.hour > 8)
    ].copy()
    center_calls = center_calls.drop_duplicates(subset=["Call Time", "Caller ID"])
    center_calls["Hour"] = center_calls["Call Time"].dt.hour
    center_calls.sample()

    answer_unanswer = (
        pd.pivot_table(
            center_calls,
            index="Status",
            values="Call Time",
            aggfunc="count",
            margins=True,
        )
        .reset_index()
        .rename(columns={"Call Time": "Count"})
    )
    answer_unanswer["Percentage"] = (
        round(answer_unanswer["Count"] / (answer_unanswer["Count"][:-1].sum()) * 100, 1)
    ).astype(str) + "%"

    call_time = (
        pd.pivot_table(
            center_calls,
            index="Status",
            values="Caller ID",
            aggfunc="count",
            columns="Hour",
            margins=True,
        )
        .fillna(0)
        .reset_index()
    )

    calls_center = center_calls.copy()
    calls_center["Mins"] = calls_center["Time"] / 60
    calls_center["Agent"] = (
        calls_center["Reason"].str.extract(r"by (.*) \((\d+)\)")[0]
        + " ("
        + calls_center["Reason"].str.extract(r"by (.*) \((\d+)\)")[1]
        + ")"
    )

    calls_direct = direct_calls.copy()
    calls_direct["Call Time"] = pd.to_datetime(
        calls_direct["Call Time"], format="%d/%m/%Y %I:%M:%S"
    ).copy()
    calls_direct = calls_direct[
        (calls_direct["Call Time"].dt.hour < 19)
        & (calls_direct["Call Time"].dt.hour > 8)
    ].copy()
    calls_direct = calls_direct.drop_duplicates(
        subset=["Call Time", "Caller ID", "Destination"]
    )
    calls_direct["Talking"] = pd.to_timedelta(calls_direct["Talking"])
    calls_direct["Time"] = calls_direct["Talking"].dt.total_seconds().astype(int)
    calls_direct["Hour"] = calls_direct["Call Time"].dt.hour
    calls_direct["Mins"] = calls_direct["Time"] / 60
    calls_direct = calls_direct.rename(columns={"Destination": "Agent"})
    calls_direct = calls_direct[["Call Time", "Caller ID", "Agent", "Hour"]]

    agents_answered = (
        calls_center[calls_center["Status"] == "answered"]
        .copy()[["Call Time", "Caller ID", "Agent", "Hour"]]
        .copy()
    )
    agents_answered = pd.concat(
        [agents_answered, calls_direct], ignore_index=True
    ).dropna(subset=["Agent"])
    agents_answered = agents_answered[
        (agents_answered["Agent"].str.lower().str.contains("cs"))
    ]
    agents_answered_pivot = (
        pd.pivot_table(
            agents_answered,
            index="Agent",
            columns="Hour",
            values="Caller ID",
            aggfunc="count",
            margins=True,
        )
        .fillna(0)
        .reset_index()
    )
    agents_answered_pivot

    calls = inbound_calls.copy()
    calls["Call Time"] = calls["Call Time"].fillna(method="ffill")
    calls["Call Time"] = pd.to_datetime(
        calls["Call Time"], format="%m/%d/%Y %I:%M:%S %p"
    ).copy()

    calls = calls[
        (calls["Call Time"].dt.hour < 19) & (calls["Call Time"].dt.hour > 8)
    ].copy()

    calls = calls[
        (calls["Status"] == "answered")
        & (calls["Destination"].str.lower().str.contains("cs"))
    ]

    calls["Time"] = pd.to_timedelta(calls["Talking"]).dt.total_seconds().astype(int)
    calls = calls[calls["Time"] > 0]
    calls["Mins"] = calls["Time"] / 60

    agents_time = pd.DataFrame(
        calls.groupby("Destination")["Time"].sum(numeric_only=True)
    ).reset_index()
    agents_time["Code"] = agents_time["Destination"].str.extract(r"\((\d+)\)")

    direct_calls_2 = calls_direct.rename(columns={"Agent": "Destination"})[
        ["Call Time", "Destination", "Caller ID"]
    ].copy()

    direct_calls_2["Direct"] = "Direct"

    callss = pd.merge(
        calls, 
        direct_calls_2, 
        on=["Call Time", "Destination", "Caller ID"], 
        how="left"
    )

    call_reason = pd.pivot_table(
        callss,
        index="Destination",
        values=["Caller ID", "Mins", "Direct"],
        aggfunc={
            "Caller ID": "count",
            "Mins": [
                "mean", 
                "sum"
            ],
            "Direct": [
                count_direct, 
                count_na
            ],
        },
    ).reset_index()

    call_reason.columns = [
        "Agent",
        "Total Calls",
        "Direct Calls",
        "Queue Calls",
        "Average Time(min)",
        "Total Time(min)",
    ]

    call_reason["Percentage"] = (
        round(call_reason["Total Calls"] 
        / (call_reason["Total Calls"]
        .sum()) * 100, 2)
    ).astype(str) + "%"

    call_reason["Average Time(min)"] = (
        call_reason["Total Time(min)"] / call_reason["Total Calls"]
    )

    call_reason["Total Time(min)"] = pd.to_timedelta(
        call_reason["Total Time(min)"], "m"
    )

    call_reason["Average Time(min)"] = pd.to_timedelta(
        call_reason["Average Time(min)"], "m"
    )

    call_reason["Total Time(min)"] = (
        (call_reason["Total Time(min)"].dt.total_seconds() / 60)
        .round(5)
        .fillna(0)
        .astype(int)
        .astype(str)
        .str.zfill(2)
        + ":"
        + (call_reason["Total Time(min)"].dt.total_seconds() % 60)
        .fillna(0)
        .astype(int)
        .astype(str)
        .str.zfill(2)
    )

    call_reason["Average Time(min)"] = (
        (call_reason["Average Time(min)"].dt.total_seconds() / 60)
        .fillna(0)
        .astype(int)
        .astype(str)
        .str.zfill(2)
        + ":"
        + (call_reason["Average Time(min)"].dt.total_seconds() % 60)
        .fillna(0)
        .astype(int)
        .astype(str)
        .str.zfill(2)
    )

    call_reason = call_reason[
        [
            "Agent",
            "Queue Calls",
            "Direct Calls",
            "Total Calls",
            "Percentage",
            "Total Time(min)",
            "Average Time(min)",
        ]
    ]


    with pd.ExcelWriter(f"{path}_3CX/inbound_calls_report.xlsx") as writer:
        answer_unanswer.to_excel(writer, sheet_name="Category", index=False)
        call_time.to_excel(writer, sheet_name="Category_Hourly", index=False)
        call_time.sort_values("Total Calls", ascending=True).to_excel(writer, sheet_name="Agent", index=False)
        agents_answered_pivot.to_excel(writer, sheet_name="Answered_Hourly", index=False)



inbound_calls_subject = "Your 3CX Report UG CS Inbound Calls is ready"


create_inbound_calls_report(
    sender="noreply@3cx.net",
    path=uganda_path,
    subject=inbound_calls_subject,
    has_attachment=False,
    start_date="25-Apr-2024",
    end_hour=19
)
