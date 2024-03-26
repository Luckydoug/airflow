from airflow.models import variable
import pandas as pd
from pangres import upsert
import pygsheets
from sub_tasks.libraries.utils import (
    createe_engine,
    create_unganda_engine,
    create_rwanda_engine
)



def check_time(row):
    if row["lost_time"] >= 0:
        return row["lost_time"]
    else:
        return 0


def push_branch_opening_time_data(
    opening_time: pd.DataFrame, 
    rename: dict, 
    database: str, 
    table: pd.DataFrame, 
    engine: str, 
    form: str
    ) -> None:
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

    opening_time_rename["lost_time"] = (
        opening_time_rename["time_ope"] - opening_time_rename["ope_time"]).apply(lambda x: x.total_seconds() / 60)
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


def fetch_kenya_opening_time():
    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8')
    opening_time = pd.DataFrame(sh.worksheet_by_title("Kenya Opening Time").get_all_records())
    kenya_engine = createe_engine()

    push_branch_opening_time_data(
        opening_time=opening_time,
        rename={
            "Date": "date",
            "Day of the week": "day",
            "Branch": "branch",
            "ReportingTime": "reporting_time",
            "OpeningTime": "opening_time",
            "Time Opened": "time_opened"
        },
        database="mabawa_staging",
        table="source_opening_time",
        engine=kenya_engine,
        form="%d-%m-%y"
    )



def fetch_uganda_opening_time():
    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8')
    opening_time = pd.DataFrame(sh.worksheet_by_title("Uganda Opening Time").get_all_records())
    uganda_engine = create_unganda_engine()

    push_branch_opening_time_data(
        opening_time=opening_time,
        rename={
            "Date": "date",
            "Day of the week": "day",
            "Branch": "branch",
            "ReportingTime": "reporting_time",
            "OpeningTime": "opening_time",
            "Time Opened": "time_opened"
        },
        database="mawingu_staging",
        table="source_opening_time",
        engine=uganda_engine,
        form="%d-%m-%y"
    )


def fetch_rwanda_opening_time():
    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8')
    opening_time = pd.DataFrame(sh.worksheet_by_title("Rwanda Opening Time").get_all_records())
    rwanda_engine = create_rwanda_engine()

    push_branch_opening_time_data(
        opening_time=opening_time,
        rename={
            "Date": "date",
            "Day of the week": "day",
            "Branch": "branch",
            "Reporting Time": "reporting_time",
            "Opening Time": "opening_time",
            "Time Opened": "time_opened"
        },
        database="voler_staging",
        table="source_opening_time",
        engine=rwanda_engine,
        form="%d-%m-%y"
    )