import sys
sys.path.append(".")
from airflow.models import variable
import json
import psycopg2
import requests
import pygsheets
import io
import pandas as pd
from datetime import date,timedelta
from sub_tasks.api_login.api_login import(login)
from pangres import upsert
from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate

# FromDate = '2024-05-01'
# ToDate = '2024-05-27'
print(FromDate,ToDate)

def fetch_ajua_info ():
    SessionId = return_session_id(country = "Kenya")
   #SessionId = login()

    #get number of pages
    page_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetAjuaInformation&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    payload = {}
    headers = {}

    page_data = requests.request("GET", page_url, headers=headers, data=payload, verify=False)
    page_data = page_data.json()
    page_data = page_data['result']['body']['recs']['PagesCount']   
    print("Number of pages is", page_data)

    #get data
    ajua_info = pd.DataFrame()
    payload = {}
    headers = {}

    for i in range (1, page_data+1):
        page = i
        data_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetAjuaInformation&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}" 
        try:
            response = requests.request("GET", data_url, headers=headers, data=payload, verify=False)
            response = response.json()
            response = response['result']['body']['recs']['Results']
            responsedf = pd.DataFrame(response)
            responsedf = responsedf.T
            ajua_info = ajua_info.append(responsedf, ignore_index=True)
        except:
            ("Error fetching data!")

    print("Data fetching completed!")

    #rename df columns
    ajua_info.rename (columns = {'DocEntry':'doc_entry', 
                             'LineId':'line_id',
                             'Type':"type",
                             'SAP_Internal_Number':'sap_internal_number',
                             'Branch':'branch',
                             'Survey_Id':'survey_id',
                             'Ajua_Triggered':'ajua_triggered',
                             'Ajua_Response':'ajua_response',
                             'NPS_Rating':'nps_rating',
                             'NPS_Rating_2':'nps_rating_2',
                             'FeedBack':'feedback',
                             'Feedback':'feedback2', 
                             'Triggered_Time':'triggered_time',
                             'Trigger_Date':'trigger_date', 
                             'Long_Feedback':'long_feedback'} 
                    ,inplace=True)
    print("Columns renamed!")
    print(ajua_info)

    #set index
    ajua_info = ajua_info.set_index('line_id')

    upsert(engine=engine,
    df=ajua_info,
    schema='mabawa_staging',
    table_name='source_ajua_info',
    if_row_exists='update',
    create_table=False)

    print("Data Inserted")


# def fetch_ajua_sheet():
#     gc =  pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
#     sh = gc.open_by_key('1EWmNuWC859iif2KgoWjZGsGQjqcdj5iIC-KW-YNJ2lc')
#     sh = sh[0]
#     values = sh.get_all_values()
#     sh = pd.DataFrame(values)
#     csv_string = sh.to_csv(index=False,header=False)
#     ajua = pd.read_csv(io.StringIO(csv_string), na_values='')

#     ajua.rename(columns = {'DocEntry':'doc_entry', 
#                     'LineId':'line_id',
#                     'Type':"type",
#                     'SAP Internal Number':'sap_internal_number',
#                     'Branch':'branch',
#                     'Survey Id':'survey_id',
#                     'Ajua Triggered':'ajua_triggered',
#                     'Ajua Response':'ajua_response',
#                     'NPS Rating':'nps_rating',
#                     'NPS Date and Time':'nps_rating_2',
#                     'FeedBack':'feedback',
#                     'Feedback Date and Time':'feedback2', 
#                     'Triggered  Time':'triggered_time',
#                     'Trigger Date':'trigger_date', 
#                     'Long Remarks':'long_feedback'}, 
#         inplace=True)

#     cols = ['doc_entry','line_id','type','sap_internal_number','branch','survey_id','ajua_triggered','ajua_response','nps_rating',
#         'nps_rating_2','feedback','feedback2','triggered_time','trigger_date','long_feedback']

#     numeric = ['doc_entry','line_id','triggered_time']

#     string = ['type','sap_internal_number','branch','survey_id','ajua_triggered','ajua_response','nps_rating',
#         'nps_rating_2','feedback','feedback2','trigger_date','long_feedback']

#     ajua['triggered_time'] = ajua['triggered_time'].str.replace(':','')

#     ajua[numeric] = ajua[numeric].astype(int)

#     ajua[string] = ajua[string].astype(str)

#     ajua = ajua[cols].set_index('line_id')

#     upsert(engine=engine,
#     df=ajua,
#     schema='mabawa_staging',
#     table_name='source_ajua_info',
#     if_row_exists='update',
#     create_table=False)

#     print("Data Inserted")

def update_log_nps():

    query = """
    insert into mabawa_dw.update_log(table_name, update_time) values('nps', default);
    """

    query = pg_execute(query)

# fetch_ajua_info ()
# update_log_nps()