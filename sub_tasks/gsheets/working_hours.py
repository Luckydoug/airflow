import pandas as pd
import pygsheets
from airflow.models import variable
from pangres import upsert
from sub_tasks.libraries.utils import (
    createe_engine,
    create_unganda_engine,
    create_rwanda_engine
)



def fetch_kenya_working_hours():
    kenya_engine = createe_engine()
    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8')
    working_hours = pd.DataFrame(sh.worksheet_by_title("Working Hours").get_all_records()).fillna("NAN")
    working_hours.rename(columns={
       "Warehouse Code": "warehouse_code",
       "Warehouse Name": 'warehouse_name',
       "DocNum": "docnum",
       "Days": "days",
       "Start Time": "start_time",
       "End Time": "end_time",
       "Auto Time": "auto_time",
    }, inplace=True)

    working_hours = working_hours.set_index(['warehouse_code', 'days'])

    upsert(engine=kenya_engine,
           df=working_hours,
           schema='reports_tables',
           table_name='working_hours',
           if_row_exists='update',
           create_table=False
    )


def fetch_uganda_working_hours():
    uganda_engine = create_unganda_engine()
    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8')
    working_hours = pd.DataFrame(sh.worksheet_by_title("UG_Working_Hours").get_all_records())
    working_hours.rename(columns={
       "Warehouse Code": "warehouse_code",
       "Warehouse Name": 'warehouse_name',
       "DocNum": "docnum",
       "Days": "days",
       "Start Time": "start_time",
       "End Time": "end_time",
       "Auto Time": "auto_time",
    }, inplace=True)

    working_hours = working_hours[[
        "warehouse_code",
        "warehouse_name",
        "docnum",
        "days",
        "start_time",
        "end_time",
        "auto_time"
    ]].set_index(['warehouse_code', 'days'])

    upsert(engine=uganda_engine,
           df=working_hours,
           schema='reports_tables',
           table_name='working_hours',
           if_row_exists='update',
           create_table=False
    )

def fetch_rwanda_working_hours():
    rwanda_engine = create_rwanda_engine()
    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8')
    working_hours = pd.DataFrame(sh.worksheet_by_title("RW_Working_Hours").get_all_records())
    working_hours.rename(columns={
       "Warehouse Code": "warehouse_code",
       "Warehouse Name": 'warehouse_name',
       "DocNum": "docnum",
       "Days": "days",
       "Start Time": "start_time",
       "End Time": "end_time",
       "Auto Time": "auto_time",
    }, inplace=True)

    working_hours = working_hours.set_index(['warehouse_code', 'days'])

    upsert(engine=rwanda_engine,
           df=working_hours,
           schema='reports_tables',
           table_name='working_hours',
           if_row_exists='update',
           create_table=False
    )

# fetch_kenya_working_hours()