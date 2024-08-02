import sys
sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from sub_tasks.data.connect import engine
# from sub_tasks.api_login.api_login import(login)
from pangres import upsert
from sub_tasks.libraries.utils import return_session_id

def fetch_sap_warehouses():
    SessionId = return_session_id(country = "Kenya")
    #SessionId = login()

    pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetWarehousesDetails&pageNo=1&SessionId={SessionId}"
    pagecount_payload={}
    pagecount_headers = {}

    pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
    data = pagecount_response.json()
    pages = data['result']['body']['recs']['PagesCount']

    print("Pages outputted", pages)

    warehousesdf = pd.DataFrame()
    payload={}
    headers = {}
    for i in range(1, pages+1):
        page = i
        url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetWarehousesDetails&pageNo={page}&SessionId={SessionId}"
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        warehouses = response.json()
        stripped_warehouses = warehouses['result']['body']['recs']['Results']
        warehouses_df = pd.DataFrame.from_dict(stripped_warehouses)
        warehouses_df2 = warehouses_df.T
        warehousesdf = warehousesdf.append(warehouses_df2, ignore_index=True)  

    warehousesdf.rename (columns = {
        'Warehouse_Code':'warehouse_code',
        'Warehouse_Name':'warehouse_name',
        'CreateDate':'warehouse_create_date',
        'UpdateDate':'warehouse_update_date',
        'Address':'warehouse_address',
        'Default_Delvy_Time':'warehouse_def_delivery_time',
        'Latitude':'warehouse_lat',
        'Longitude':'warehouse_long',
        'Lab':'warehouse_lab',
        'Is_Lab':'warehouse_ls_lab',
        'Glazzing_At':'warehouse_glazzing_at',
        'Branch_Location':'warehouse_branch_locateion',
        'Collection_Branch':'warehouse_collection_branch',
        'Frame_Timing':'warehouse_frame_timing',
        'PF_Timing':'warehouse_pf_timing',
        'HQ_Lens_Add_Time':'warehouse_hq_lens_add_tme',
        'PL_Add_Time':'warehouse_pl_add_time',
        'HQ_Warehouse':'hq_warehouse',
        'Safari_PriceList':'warehouse_safari_price_list',
        'Company_Brand':'warehouse_company_brand',
        'Other_collection':'warehouse_other_collection'}
    ,inplace=True)
    print('renaming successful')


    if warehousesdf.empty:
        print('warehouse dataframe is empty!')
    else:
        warehousesdf = warehousesdf.set_index(['warehouse_code'])
        upsert(engine=engine,
        df=warehousesdf,
        schema='mabawa_staging',
        table_name='source_warehouses',
        if_row_exists='update',
        create_table=False)

        print('Update successful')






