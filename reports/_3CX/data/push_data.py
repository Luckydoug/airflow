import pandas as pd
from airflow.models import variable
from reports._3CX.utils.utils import save_file, upsert
from sub_tasks.libraries.utils import create_unganda_engine
from sub_tasks.libraries.utils import uganda_path
from pangres import upsert


def upsert_to_source_inbound_calls() -> None:
    engine = create_unganda_engine()
    inbound_calls = pd.read_csv(
        save_file(
            sender="noreply@3cx.net",
            subject="Your 3CX Report UG CS Inbound Calls is ready",
            has_attachment=False,
            date_since="05-Jul-2024",
            path=uganda_path,
        ),
        skiprows=3,
        encoding="latin1",
    )[:-2]

    inbound_calls = inbound_calls.drop(
        columns=["Unnamed: 2", "Unnamed: 11"], errors="ignore", axis=1
    )
    inbound_calls["Talking"] = pd.to_timedelta(
        inbound_calls["Talking"], errors="coerce"
    )
    inbound_calls["Totals"] = pd.to_timedelta(inbound_calls["Totals"], errors="coerce")
    inbound_calls["Ringing"] = pd.to_timedelta(
        inbound_calls["Ringing"], errors="coerce"
    )

    inbound_calls = inbound_calls.rename(
        columns={
            "Call Time": "call_time",
            "Caller ID": "caller_id",
            "Destination": "destination",
            "Status": "status",
            "Ringing": "ringing",
            "Talking": "talking",
            "Totals": "totals",
            "Cost": "cost",
            "Reason": "reason",
            "Play": "play",
        }
    )

    query = "SELECT COALESCE(MAX(call_id), 0) AS max_call_id FROM mawingu_staging.source_inbound_calls"
    max_call_id = pd.read_sql_query(query, con=engine)["max_call_id"][0]

    existing_calls_query = """
    SELECT call_id, date, caller_id, destination
    FROM mawingu_staging.source_inbound_calls
    """
    existing_calls = pd.read_sql_query(existing_calls_query, con=engine)
    existing_calls_mapping = existing_calls.set_index(
        ["date", "caller_id", "destination"]
    )["call_id"].to_dict()

    event_seqq = 1

    def generate_idss(row):
        nonlocal max_call_id
        nonlocal event_seqq

        if pd.notnull(row["call_time"]):
            max_call_id += 1
            event_seqq = 1
        else:
            event_seqq += 1

        return pd.Series([event_seqq])

    event_seq = 1

    def generate_ids(row):
        nonlocal max_call_id
        nonlocal event_seq

        key = (row["date"], row["caller_id"], row["destination"])
        if key in existing_calls_mapping:
            existing_call_id = existing_calls_mapping[key]
            return pd.Series([existing_call_id, event_seq])
        else:
            if pd.notnull(row["call_time"]):
                max_call_id += 1
                event_seq = 1
            else:
                event_seq += 1
            return pd.Series([max_call_id, event_seq])

    inbound_calls["date"] = pd.to_datetime(
        inbound_calls["call_time"].fillna(method="ffill")
    )
    inbound_calls = inbound_calls.replace({pd.NaT: None})
    inbound_calls[["call_id", "event_seq"]] = inbound_calls.apply(generate_ids, axis=1)
    inbound_calls[["event_seq"]] = inbound_calls.apply(generate_idss, axis=1)
    inbound_calls = inbound_calls.drop_duplicates(
        subset=["date", "destination", "caller_id"]
    )

    upsert(
        engine,
        inbound_calls.set_index(["date", "caller_id", "destination"]),
        schema="mawingu_staging",
        table_name="source_inbound_calls",
        if_row_exists="update",
    )


def upsert_to_source_outbound_calls() -> None:
    engine = create_unganda_engine()

    outbound_calls = pd.read_csv(
        save_file(
            sender="noreply@3cx.net",
            subject="Your 3CX Report From Internal Extensions to External Extensions is ready",
            has_attachment=False,
            date_since="08-Jul-2024",
            path=uganda_path,
        ),
        skiprows=5,
        encoding="latin1",
    )[:-2]

    outbound_calls = outbound_calls.drop(
        columns=["Unnamed: 2", "Unnamed: 11"], axis=1, errors="coerce"
    )

    calls_outbound = outbound_calls.copy()
    calls_outbound["Call Time"] = pd.to_datetime(
        calls_outbound["Call Time"], format="%m/%d/%Y %I:%M:%S %p"
    )
    calls_outbound = calls_outbound[~calls_outbound["Caller ID"].str.startswith("7")]
    calls_outbound = calls_outbound[~calls_outbound["Caller ID"].str.startswith("+254")]
    calls_outbound = calls_outbound[
        (calls_outbound["Destination"].str.startswith("0"))
        | (calls_outbound["Destination"].str.startswith("1"))
    ]

    calls_outbound = calls_outbound.rename(
        columns={
            "Call Time": "call_time",
            "Caller ID": "caller_id",
            "Destination": "destination",
            "Status": "status",
            "Ringing": "ringing",
            "Talking": "talking",
            "Totals": "totals",
            "Cost": "cost",
            "Reason": "reason",
            "Play": "play",
        }
    )

    upsert(
        engine,
        calls_outbound.set_index(["call_time", "caller_id", "destination"]),
        schema="mawingu_staging",
        table_name="source_outbound_calls",
        if_row_exists="update",
    )


# upsert_to_source_outbound_calls()
