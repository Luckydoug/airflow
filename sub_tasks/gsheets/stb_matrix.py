from airflow.models import Variable
from sub_tasks.data.connect import engine
from sub_tasks.data.connect_voler import engine as rwanda_engine
from sub_tasks.data.connect_mawingu import engine as uganda_engine
from pangres import upsert
import pandas as pd
import pygsheets
import sys
sys.path.append(".")

def transporters_matrix():
    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8')
    sh = pd.DataFrame(sh.worksheet_by_title("STB Matrix").get_all_records()).fillna("NAN")
    sh = sh.drop_duplicates(subset=["Outlet"])
    sh.rename(columns={
       "Outlet": "branch_code",
       "Branch": 'branch_name',
       "Address":"transporter",
       "Timings":"riders_time"
    }, inplace=True)

    sh = sh[[
        "branch_code",
        "branch_name",
        "transporter",
        "riders_time"
    ]].set_index(['branch_code'])

    upsert(engine=engine,
           df=sh,
           schema='mabawa_staging',
           table_name='source_transporters_matrix',
           if_row_exists='update',
           create_table=False
    )
# transporters_matrix()    