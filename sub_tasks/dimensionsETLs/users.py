import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
from datetime import date
import requests
import pandas as pd
from datetime import date
from pangres import upsert, DocsExampleTable

from pandas.io.json._normalize import nested_to_record 
from sqlalchemy import create_engine
from airflow.models import Variable

from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine, pg_bulk_insert) 
from sub_tasks.api_login.api_login import(login)

# LOCAL_DIR = "/tmp/"
#location = Variable.get("LOCAL_DIR", deserialize_json=True)
#dimensionstore = location["dimensionstore"]

# get session id
SessionId = login()

FromDate = date.today().strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')

# api details
pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetUserDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
pagecount_payload={}
pagecount_headers = {}

def fetch_sap_users():

    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    pages = data['result']['body']['recs']['PagesCount']

    print("Pages outputted", pages)

    usersdf = pd.DataFrame()
    payload={}
    headers = {}
    for i in range(1, pages+1):
        page = i
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetUserDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        users = response.json()
        stripped_users = users['result']['body']['recs']['Results']
        users_df = pd.DataFrame.from_dict(stripped_users)
        users_df2 = users_df.T
        usersdf = usersdf.append(users_df2, ignore_index=True) 

    print('TRANSFORMATION! Adding new columns')

    # condition to add new column if optom or sales person
    usersdf.loc[usersdf['Department'] == 1, 'user_department_name'] = 'Sales Person' 
    usersdf.loc[usersdf['Department'] == 2, 'user_department_name'] = 'Optom' 

    print('INFO! %d rows' %(len(usersdf)))

    usersdf.rename (columns = {'UserSignature':'user_signature', 
                'Internal_Number':'internal_no', 
                'User_Code':'user_code', 
                'User_Name':'user_name',
                'Max_Discount':'max_discount', 
                'User_Locked':'user_locked_status', 
                'Department':'user_department_code',
                'Spct_Len_Eligible':'spct_len_eligible', 
                'SE_Optom':'se_optom'}
    ,inplace=True)

    # query = """truncate mabawa_staging.source_users;"""
    # query = pg_execute(query)
    
    # # usersdf.to_sql('source_users_staging', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    # table_name = 'source_users'
    # schema = 'mabawa_staging'

    # pg_bulk_insert(usersdf, table_name, schema)

    if usersdf.empty:
        print('INFO! Users dataframe is empty!')
    else:
        usersdf = usersdf.set_index(['user_signature'])
        print('INFO! Users upsert started...')

        upsert(engine=engine,
        df=usersdf,
        schema='mabawa_staging',
        table_name='source_users',
        if_row_exists='update',
        create_table=False)

        print('Update successful')


    return "insert users done"

def create_dim_users():

    query = """
    truncate mabawa_dw.dim_users;
    insert into mabawa_dw.dim_users
    SELECT user_signature, internal_no, user_code, user_name, max_discount, user_locked_status, user_department_code, spct_len_eligible, se_optom, user_department_name
    FROM mabawa_dw.v_dim_users;
    """

    query = pg_execute(query)

# fetch_sap_users()