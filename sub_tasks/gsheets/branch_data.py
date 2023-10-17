from airflow.models import Variable
from sub_tasks.data.connect import engine
from sub_tasks.data.connect_voler import engine as rwanda_engine
from sub_tasks.data.connect_mawingu import engine as uganda_engine
from pangres import upsert
import pandas as pd
import pygsheets
import sys
sys.path.append(".")

def fetch_kenya_branch_data():
    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8')
    sh = pd.DataFrame(sh.worksheet_by_title("SRM_RM_List").get_all_records()).fillna("NAN")
    sh = sh.drop_duplicates(subset=["Outlet"])
    sh.rename(columns={
       "Outlet": "branch_code",
       "Branch": 'branch_name',
       "Email": "email",
       "RM": "rm",
       "RM Email": "rm_email",
       "RM Group": "rm_group",
       "SRM": "srm",
       "SRM Email": "srm_email",
       "Branch Manager": "branch_manager",
       "Front Desk": "front_desk",
       "Zone": "zone"
    }, inplace=True)

    sh = sh.set_index(['branch_code'])

    upsert(engine=engine,
           df=sh,
           schema='reports_tables',
           table_name='branch_data',
           if_row_exists='update',
           create_table=False
    )
    


def fetch_rwanda_branch_data():
    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8')
    sh = pd.DataFrame(sh.worksheet_by_title("RW_SRM_RM").get_all_records())
    sh = sh.drop_duplicates(subset=["Outlet"])
    sh.rename(columns={
       "Outlet": "branch_code",
       "Branch": 'branch_name',
       "Email": "email",
       "RM": "rm",
       "RM Email": "rm_email",
       "RM Group": "rm_group",
       "SRM": "srm",
       "SRM Email": "srm_email",
       "Branch Manager": "branch_manager",
       "Front Desk": "front_desk",
       "Zone": "zone"
    }, inplace=True)

    sh = sh.set_index(['branch_code'])

    upsert(engine=rwanda_engine,
           df=sh,
           schema='reports_tables',
           table_name='branch_data',
           if_row_exists='update',
           create_table=False)
    

def fetch_uganda_branch_data():
    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8')
    sh = pd.DataFrame(sh.worksheet_by_title("UG_SRM_RM").get_all_records())
    sh = sh.drop_duplicates(subset=["Outlet"])
    sh.rename(columns={
       "Outlet": "branch_code",
       "Branch": 'branch_name',
       "Email": "email",
       "RM": "rm",
       "RM Email": "rm_email",
       "RM Group": "rm_group",
       "SRM": "srm",
       "SRM Email": "srm_email",
       "Branch Manager": "branch_manager",
       "Front Desk": "front_desk",
       "Zone": "zone"
    }, inplace=True)

    sh = sh.set_index(['branch_code'])

    upsert(engine=uganda_engine,
           df=sh,
           schema='reports_tables',
           table_name='branch_data',
           if_row_exists='update',
           create_table=False)

    
