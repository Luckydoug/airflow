import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import datetime
import pandas as pd
from datetime import date
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine
from airflow.models import Variable
from pandas.io.json._normalize import nested_to_record 


from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine, pg_bulk_insert) 
from sub_tasks.api_login.api_login import(login)


# get session id
SessionId = login()
FromDate = '2018/01/01'
# ToDate = date.today().strftime('%Y/%m/%d')

# FromDate = date.today().strftime('%Y/%m/%d')
ToDate = date.today().strftime('%Y/%m/%d')

# api details
pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetBranchTargetCalculation&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
pagecount_payload={}
pagecount_headers = {}

def fetch_sap_branch_targets():

    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    pages = data['result']['body']['recs']['PagesCount']

    print("Pages outputted", pages)

    targets_df = pd.DataFrame()
    payload={}
    headers = {}
    for i in range(1, pages+1):
        page = i
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetBranchTargetCalculation&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        targets = response.json()
        stripped_targets = targets['result']['body']['recs']['Results']
        stripped_targets_df = pd.DataFrame.from_dict(stripped_targets)
        targets_df = targets_df.append(stripped_targets_df, ignore_index=True) 
    

    
    # This will check if there was no fetched data to prevent error

    if targets_df.empty:
        print('INFO! Targets dataframe is empty!')
    else:

        '''
        INSERT THE Targets Header TABLE
        '''
        targets_details = targets_df['details'].apply(pd.Series)

        print('INFO! %d rows' %(len(targets_details)))
        
        targets_details.rename (columns = {
            'DocEntry':'doc_entry',
            'DocNum':'doc_num',
            'UserSign':'user_signature',
            'CreateDate':'budget_create_date',
            'CreateTime':'budget_create_time',
            'UpdateDate':'budget_update_date',
            'UpdateTime':'budget_update_time',
            'Creator':'budget_creator',
            'Monthly':'budget_month',
            'Year':'budget_year'
            }
        ,inplace=True)
        
        targets_details = targets_details.set_index('doc_entry')

        print('TRANSFORMATION! Adding new target header rows')

        upsert(engine=engine,
        df=targets_details,
        schema='mabawa_staging',
        table_name='source_targets',
        if_row_exists='update',
        create_table=False)
    
        # targets_details.to_sql('source_targets', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)


        '''
        INSERT THE TARGET DETAILS TABLE
        '''
        
        targets_itemdetails = targets_df['itemdetails']
        targets_itemdetails_df = targets_itemdetails.to_frame()

        itemdetails_df = pd.DataFrame()
        for index, row in targets_itemdetails_df.iterrows():
            row_data=row['itemdetails']
            data = pd.DataFrame.from_dict(row_data)
            data1 = data.T
            itemdetails_df = itemdetails_df.append(data1, ignore_index=True)

        print('INFO! %d rows' %(len(itemdetails_df)))
        
        # Convert to integer and ensure that default is 0. this is because there are nulls
        itemdetails_df['Staff_Code'] = pd.to_numeric(itemdetails_df['Staff_Code'], errors='coerce').fillna(0)

        itemdetails_df.rename (columns = {
            'DocEntry':'doc_entry',
            'LineId':'line_id',
            'Branch':'warehouse_code',
            'Branch_Target':'target_branch_amount',
            'Staff_Code':'user_signature',
            'Cash':'target_cash_qty',
            'Insurance':'target_insurance_qty',
            'Individual_Cash_Trgt':'target_individual_cash',
            'Insurance_Branch':'target_insurance_branch',
            'Insurance_Individual':'target_individual_insurance'}
        ,inplace=True)

        itemdetails_df = itemdetails_df.set_index(['doc_entry','line_id'])

        print('TRANSFORMATION! Adding new Target Details rows')

        upsert(engine=engine,
        df=itemdetails_df,
        schema='mabawa_staging',
        table_name='source_targets_details',
        if_row_exists='update',
        create_table=False)

        # itemdetails_df.to_sql('source_targets_details', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)

    return "insert targets successfull"


fetch_sap_branch_targets()