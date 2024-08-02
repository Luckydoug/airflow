import sys
sys.path.append(".")
import requests
import pandas as pd
from datetime import date
from pangres import upsert
from airflow.models import Variable
from sub_tasks.data.connect_voler import engine
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate



def fetch_sap_branch_targets():
    SessionId = return_session_id(country = "Rwanda")

    pagecount_url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetBranchTargetCalculation&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"

    pagecount_payload={}
    pagecount_headers = {}

    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    pages = data['result']['body']['recs']['PagesCount']

    print("Pages outputted", pages)

    targets_df = pd.DataFrame()
    payload={}
    headers = {}
    for i in range(1, pages+1):
        page = i
        url = f"https://10.40.16.9:4300/RWANDA_BI/XSJS/BI_API.xsjs?pageType=GetBranchTargetCalculation&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        targets = response.json()
        stripped_targets = targets['result']['body']['recs']['Results']
        stripped_targets_df = pd.DataFrame.from_dict(stripped_targets)
        targets_df = targets_df.append(stripped_targets_df, ignore_index=True) 
    

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
        schema='voler_staging',
        table_name='source_targets',
        if_row_exists='update',
        create_table=False)
    
        # targets_details.to_sql('source_targets', con = engine, schema='voler_staging', if_exists = 'append', index=False)


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
        # itemdetails_df['Staff_Code'] = pd.to_numeric(itemdetails_df['Staff_Code'], errors='coerce').fillna(0)

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
        schema='voler_staging',
        table_name='source_targets_details',
        if_row_exists='update',
        create_table=False)

        print('source_targets_details')

        # itemdetails_df.to_sql('source_targets_details', con = engine, schema='voler_staging', if_exists = 'append', index=False)

# fetch_sap_branch_targets()