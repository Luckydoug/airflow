from airflow.models import Variable
import pandas as pd
from pangres import upsert


def check_time(row):
    if row["lost_time"] >= 0:
        return row["lost_time"]
    else:
        return 0


def push_branch_opening_time_data(opening_time, rename, database, table, engine, form):
    opening_time_rename = opening_time.rename(columns=rename)[[
        "date",
        "day",
        "branch",
        "reporting_time",
        "opening_time",
        "time_opened"
    ]]

    opening_time_rename["date"] = pd.to_datetime(
        opening_time_rename["date"], format=form
    )
    opening_time_rename.loc[:, "rep_time"] = pd.to_datetime(opening_time_rename["date"].astype(
        str) + " " + opening_time_rename["reporting_time"].astype(str), format="%Y-%m-%d %H:%M:%S")
    opening_time_rename.loc[:, "ope_time"] = pd.to_datetime(opening_time_rename["date"].astype(
        str) + " " + opening_time_rename["opening_time"].astype(str), format="%Y-%m-%d %H:%M:%S")
    opening_time_rename.loc[:, "time_ope"] = pd.to_datetime(opening_time_rename["date"].astype(
        str) + " " + opening_time_rename["time_opened"].astype(str), format="%Y-%m-%d %H:%M:%S")

    opening_time_rename["lost_time"] = (opening_time_rename["time_ope"] - opening_time_rename["ope_time"]).apply(lambda x: x.total_seconds() / 60)
    opening_time_rename["lost_time"] = opening_time_rename.apply(
        lambda row: check_time(row), axis=1
    )

    opening_time_rename["opening_time"] = opening_time_rename["opening_time"].astype(str)
    opening_time_rename["reporting_time"] = opening_time_rename["reporting_time"].astype(str)
    opening_time_rename["time_opened"] = opening_time_rename["time_opened"].astype(str)
    opening_time_rename["lost_time"] = opening_time_rename["lost_time"].astype(int)

    required_columns = [
        "id",
        "date",
        "day",
        "branch",
        "reporting_time",
        "opening_time",
        "time_opened",
        "lost_time"
    ]

    final_opening_data = opening_time_rename.drop_duplicates(subset="branch")
    final_opening_data['id'] = final_opening_data['date'].astype(str).str.replace(
        '-', '') + final_opening_data['branch'].str.replace(' ', '').str.upper()
    final_opening_data = final_opening_data[required_columns].copy()
    final_opening_data = final_opening_data.set_index("id")

    upsert(
        engine=engine,
        df=final_opening_data,
        schema=database,
        table_name=table,
        if_row_exists='update',
        create_table=True
    )



def create_opening_time_report(data, path):
    if not len(data):
        return
    with pd.ExcelWriter(f"{path}draft_upload/opening_time.xlsx") as writer:
        data.to_excel(writer, sheet_name="daily_summary", index=False)



