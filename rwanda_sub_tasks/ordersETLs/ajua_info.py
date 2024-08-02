import sys
sys.path.append(".")
import requests
import pandas as pd
from datetime import date
from pangres import upsert
from airflow.models import Variable
from sub_tasks.data.connect_voler import (engine) 
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate


def fetch_ajua_info():
    SessionId = return_session_id(country="Rwanda")   
    page_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetAjuaInformation&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
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
        data_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetAjuaInformation&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}" 
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
    schema='voler_staging',
    table_name='source_ajua_info',
    if_row_exists='update',
    create_table=True)

    print("Data Inserted")

    # fetch_ajua_info()
