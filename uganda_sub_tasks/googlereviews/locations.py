import sys
from numpy import nan
sys.path.append(".")
#import libraries
import json
import requests
import psycopg2
import pandas as pd
from airflow.models import Variable 
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 


from sub_tasks.data.connect_mawingu import (pg_execute, pg_fetch_all, engine)  
# from sub_tasks.api_login.api_login import(login_uganda)
from sub_tasks.googlereviews.refresh_token import (refresh_tokens)
conn = psycopg2.connect(host="10.40.16.19",database="mawingu", user="postgres", password="@Akb@rp@$$w0rtf31n")

token = refresh_tokens()

def fetch_locations():

    print (token)
    
    #url = "https://mybusiness.googleapis.com/v4/accounts/105865586632368693390/locations"
    url = "https://mybusinessbusinessinformation.googleapis.com/v1/accounts/113556761891998395560/locations?readMask=title&readMask=name&readMask=storeCode"    
    
    payload={}
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }

    locations_df = pd.DataFrame()

    locations = requests.request("GET", url, headers=headers, data=payload)
    locations = locations.json()

    
    locations_data = locations['locations']
    print(locations_data)
    keys = ["name","storeCode", "title"] 
    print(locations_data)
    
    res = []  
    for dict1 in locations_data: 
        result = dict((k, dict1[k]) for k in keys if k in dict1) 
        res.append(result) 

    locations_data = pd.DataFrame(res)
    locations_df= locations_df.append(locations_data)
    print(locations_data)
    # nextPageToken = locations['nextPageToken'] 
    nextPageToken = False   
    while bool(nextPageToken)==True:
        mytoken = nextPageToken
        urll = f"https://mybusinessbusinessinformation.googleapis.com/v1/accounts/113556761891998395560/locations?readMask=title&readMask=name&readMask=storeCode&pageToken={mytoken}"
        nextpage = requests.request("GET", urll, headers=headers, data=payload)
        nextpage=nextpage.json()

        try:
            nextpagedata = nextpage['locations']
            keys = ["name","storeCode", "title"] 

            rez = []  
            for dict1 in nextpagedata: 
                result = dict((k, dict1[k]) for k in keys if k in dict1) 
                rez.append(result)
            df = pd.DataFrame(rez)
            locations_df= locations_df.append(df)
        except:
            print('Whoops! Ran into an Error!')
        
        try:
            nextPageToken = nextpage['nextPageToken']
        except:
            print("The End is Here!")
            break

    locations_df.rename (columns = {'name':'location_id', 
                       'storeCode':'store_code', 
                       'title':'location_name'}
            ,inplace=True)

    locations_df['location_id'] = 'accounts/113556761891998395560/' + locations_df['location_id'].astype(str)

    query="""truncate mawingu_staging.source_locations;"""
    query=pg_execute(query)
    print("Truncated Table")
    print(locations_df)
    locations_df.to_sql('source_locations', con = engine, schema='mawingu_staging', if_exists = 'append', index=False)
    
    print("Inserted Data")

def update_store_code():

    query = """
    update mawingu_staging.source_locations
    set store_code = 'ACA'
    where location_name = 'Optica - Opticians in Acacia Mall';
    update mawingu_staging.source_locations
    set store_code = 'ARE'
    where location_name = 'Optica - Opticians in Arena Mall';
    """

    query = pg_execute(query)