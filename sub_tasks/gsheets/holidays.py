import sys
sys.path.append(".")
import pygsheets
import pandas as pd
from airflow.models import Variable
from pangres import upsert
from sub_tasks.libraries.utils import createe_engine
from sub_tasks.libraries.utils import create_unganda_engine
from sub_tasks.libraries.utils import create_rwanda_engine

def fetch_holidays():
    engine = createe_engine()
    gc = pygsheets.authorize(
        service_file="/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json"
    )
    sh = gc.open_by_key("1yRlrhjTOv94fqNpZIiA9xToKbDi9GaWkdoGrG4FcdKs")
    sh = sh[0]
    sh = pd.DataFrame(sh.get_all_records())
    sh.rename(columns={"Date": "holiday_date", "Holiday": "holiday_name"}, inplace=True)

    sh = sh.set_index(["holiday_date"])

    upsert(
        engine=engine,
        df=sh,
        schema="mabawa_dw",
        table_name="dim_holidays",
        if_row_exists="update",
        create_table=False,
    )


def fetch_uganda_holidays():
    engine = create_unganda_engine()
    gc = pygsheets.authorize(
        service_file="/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json"
    )
    sh = gc.open_by_key("1yRlrhjTOv94fqNpZIiA9xToKbDi9GaWkdoGrG4FcdKs")
    sh = sh[1]
    sh = pd.DataFrame(sh.get_all_records())
    sh.rename(columns={"Date": "holiday_date", "Holiday": "holiday_name"}, inplace=True)

    sh = sh.set_index(["holiday_date"])

    upsert(
        engine=engine,
        df=sh,
        schema="mawingu_dw",
        table_name="dim_holidays",
        if_row_exists="update",
        create_table=False,
    )


def fetch_rwanda_holidays():
    engine = create_rwanda_engine()
    gc = pygsheets.authorize(
        service_file="/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json"
    )
    sh = gc.open_by_key("1yRlrhjTOv94fqNpZIiA9xToKbDi9GaWkdoGrG4FcdKs")
    sh = sh[2]
    sh = pd.DataFrame(sh.get_all_records())
    sh.rename(columns={"Date": "holiday_date", "Holiday": "holiday_name"}, inplace=True)

    sh = sh.set_index(["holiday_date"])

    upsert(
        engine=engine,
        df=sh,
        schema="voler_dw",
        table_name="dim_holidays",
        if_row_exists="update",
        create_table=False,
    )
