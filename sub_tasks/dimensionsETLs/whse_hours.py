import sys
sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
from sub_tasks.data.connect import (pg_execute, engine) 
# from sub_tasks.api_login.api_login import(login)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from sub_tasks.libraries.utils import return_session_id


def fetch_sap_whse_hours():
    SessionId = return_session_id(country = "Kenya")
    #SessionId = login()
    
    payload={}
    headers = {}
    url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetWarehousesHours&SessionId={SessionId}"
    response = requests.request("GET", url, headers=headers, data=payload, verify=False)
    whsehrs = response.json()
    whsehrs = whsehrs['result']['body']['recs']['Results']
    whsehrs = pd.DataFrame(whsehrs)
    whsehrs = whsehrs.T

    whsehrs.rename (columns = {'WHS_Code':'whse_code', 
                       'WHS_Name':'whse_name', 
                       'No_Of_Days':'no_of_days', 
                       'Days':'days', 
                       'Start_Time':'start_time', 
                       'End_Time':'end_time', 
                       'U_VSPMXDTM':'u_vspmxdtm'}
            ,inplace=True)
    whsehrs = whsehrs.set_index(['whse_code', 'no_of_days'])

    upsert(engine=engine,
        df=whsehrs,
        schema='mabawa_staging',
        table_name='source_whse_hrs',
        if_row_exists='update',
        create_table=False)

    print('Update successful')

def create_mviews_whse_hrs():

    query = """
    truncate mabawa_mviews.whse_hrs;
    insert into mabawa_mviews.whse_hrs
    SELECT whse_code, whse_name, no_of_days, days, start_time, end_time, u_vspmxdtm, work_status
    FROM mabawa_mviews.v_whse_hrs;
    """

    query = pg_execute(query)
    
# create_mviews_whse_hrs()

def create_mviews_branch_hours_array():

    whsehrs = """
    truncate mabawa_mviews.branch_hours_array;
    insert into mabawa_mviews.branch_hours_array
    SELECT whse_code, whse_name, array_agg, null as category
    FROM mabawa_mviews.v_branch_hours_array;
    """

    whsehrs = pg_execute(whsehrs)

def update_categories():

    query = """
    update mabawa_mviews.branch_hours_array set category =	'Category 1'	where array_agg =	'{"1 10:00:00 1 19:00:00","2 10:00:00 2 19:00:00","3 10:00:00 3 19:00:00","4 10:00:00 4 19:00:00","5 10:00:00 5 19:00:00","6 10:00:00 6 18:30:00","7 11:00:00 7 17:00:00"}'	;
    update mabawa_mviews.branch_hours_array set category =	'Category 2'	where array_agg =	'{"1 10:00:00 1 19:00:00","2 10:00:00 2 19:00:00","3 10:00:00 3 19:00:00","4 10:00:00 4 19:00:00","5 10:00:00 5 19:00:00","6 10:00:00 6 18:00:00","7 11:00:00 7 17:00:00"}'	;
    update mabawa_mviews.branch_hours_array set category =	'Category 3'	where array_agg =	'{"1 09:30:00 1 18:00:00","2 09:30:00 2 18:00:00","3 09:30:00 3 18:00:00","4 09:30:00 4 18:00:00","5 09:30:00 5 18:00:00","6 10:00:00 6 18:00:00"}'	;
    update mabawa_mviews.branch_hours_array set category =	'Category 4'	where array_agg =	'{"1 10:00:00 1 19:00:00","2 10:00:00 2 19:00:00","3 10:00:00 3 19:00:00","4 10:00:00 4 19:00:00","5 10:00:00 5 19:00:00","6 10:30:00 6 18:30:00","7 11:00:00 7 18:00:00"}'	;
    update mabawa_mviews.branch_hours_array set category =	'Category 5'	where array_agg =	'{"1 09:00:00 1 18:00:00","2 09:00:00 2 18:00:00","3 09:00:00 3 18:00:00","4 09:00:00 4 18:00:00","5 09:00:00 5 18:00:00","6 10:00:00 6 17:00:00"}'	;
    update mabawa_mviews.branch_hours_array set category =	'Category 6'	where array_agg =	'{"1 10:00:00 1 19:00:00","2 10:00:00 2 19:00:00","3 10:00:00 3 19:00:00","4 10:00:00 4 19:00:00","5 10:00:00 5 19:00:00","6 10:00:00 6 19:00:00","7 11:00:00 7 18:00:00"}'	;
    update mabawa_mviews.branch_hours_array set category =	'Category 7'	where array_agg =	'{"1 10:00:00 1 19:00:00","2 10:00:00 2 19:00:00","3 10:00:00 3 19:00:00","4 10:00:00 4 19:00:00","5 10:00:00 5 19:00:00","6 10:00:00 6 18:00:00"}'	;
    update mabawa_mviews.branch_hours_array set category =	'Category 8'	where array_agg =	'{"1 09:00:00 1 18:00:00","2 09:00:00 2 18:00:00","3 09:00:00 3 18:00:00","4 09:00:00 4 18:00:00","5 09:00:00 5 18:00:00","6 10:00:00 6 16:00:00"}'	;
    update mabawa_mviews.branch_hours_array set category =	'Category 9'	where array_agg =	'{"1 09:00:00 1 18:00:00","2 09:00:00 2 18:00:00","3 09:00:00 3 18:00:00","4 09:00:00 4 18:00:00","5 09:00:00 5 18:00:00","6 09:00:00 6 18:00:00","7 11:00:00 7 16:00:00"}'	;
    update mabawa_mviews.branch_hours_array set category =	'Category 10'	where array_agg =	'{"1 08:30:00 1 17:30:00","2 08:30:00 2 17:30:00","3 08:30:00 3 17:30:00","4 08:30:00 4 17:30:00","5 08:30:00 5 17:30:00","6 09:00:00 6 15:00:00"}'	;
    update mabawa_mviews.branch_hours_array set category =	'Category 11'	where array_agg =	'{"1 09:00:00 1 18:00:00","2 09:00:00 2 18:00:00","3 09:00:00 3 18:00:00","4 09:00:00 4 18:00:00","5 09:00:00 5 18:00:00","6 09:00:00 6 16:00:00"}'	;
    update mabawa_mviews.branch_hours_array set category =	'Category 12'	where array_agg =	'{"1 09:00:00 1 18:00:00","2 09:00:00 2 18:00:00","3 09:00:00 3 18:00:00","4 09:00:00 4 18:00:00","5 09:00:00 5 18:00:00","6 09:00:00 6 18:00:00"}'	;
    update mabawa_mviews.branch_hours_array set category =	'Category 13'	where array_agg =	'{"1 10:00:00 1 19:00:00","2 10:00:00 2 19:00:00","3 10:00:00 3 19:00:00","4 10:00:00 4 19:00:00","5 10:00:00 5 19:00:00","6 10:00:00 6 17:00:00","7 11:00:00 7 16:00:00"}'	;
    update mabawa_mviews.branch_hours_array set category =	'Category 14'	where array_agg =	'{"1 10:00:00 1 19:00:00","3 10:00:00 3 19:00:00","4 10:00:00 4 19:00:00","5 10:00:00 5 19:00:00","6 10:00:00 6 18:00:00","7 11:00:00 7 18:00:00"}'	;
    update mabawa_mviews.branch_hours_array set category =	'Category 15'	where array_agg =	'{"1 09:00:00 1 18:00:00","2 09:00:00 2 18:00:00","3 09:00:00 3 18:00:00","4 09:00:00 4 18:00:00","5 09:00:00 5 18:00:00","6 09:00:00 6 16:00:00",NULL}'	;
    update mabawa_mviews.branch_hours_array set category =	'Category 16'	where array_agg =	'{"1 10:00:00 1 19:00:00","2 10:00:00 2 19:00:00","3 10:00:00 3 19:00:00","4 10:00:00 4 19:00:00","5 10:00:00 5 19:00:00","6 10:00:00 6 18:00:00","7 11:00:00 7 16:00:00"}'	;
    """
    query = pg_execute(query)

def create_dim_branch_hrs():

    query = """
    truncate mabawa_dw.dim_branch_hrs;
    insert into mabawa_dw.dim_branch_hrs
    SELECT whse_code, whse_name, no_of_days, days, start_time, end_time, u_vspmxdtm, work_status, category
    FROM mabawa_dw.v_dim_branch_hrs;
    """

    query = pg_execute(query)

    
