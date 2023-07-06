# import sys

# from numpy import nan
# sys.path.append(".")

# #import libraries
# from io import StringIO
# import json
# import psycopg2
# import requests
# import datetime
# import pandas as pd
# from datetime import date, timedelta
# from airflow.models import Variable 
# from sqlalchemy import create_engine
# from pangres import upsert, DocsExampleTable
# from sqlalchemy import create_engine, text, VARCHAR
# from pandas.io.json._normalize import nested_to_record 

# from sub_tasks.data.connect import (pg_execute, engine) 
# from sub_tasks.api_login.api_login import(login)

# conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")

# SessionId = login()

# FromDate = '2022/01/01'
# ToDate = '2022/10/10'

# """
# today = date.today()
# pastdate = today - timedelta(days=5)
# FromDate = pastdate.strftime('%Y/%m/%d')
# ToDate = date.today().strftime('%Y/%m/%d')
# """

# # api details
# #orderscreen_url = 'https://41.72.211.10:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrderDetails&pageNo=1'
# pagecount_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrders&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
# pagecount_payload={}
# pagecount_headers = {}

# def fetch_sap_orders():

#     pagecount_response = requests.request("GET", pagecount_url, headers=pagecount_headers, data=pagecount_payload, verify=False)
#     data = pagecount_response.json()
    
#     orders = pd.DataFrame()
#     payload={}
#     headers = {}
#     pages = data['result']['body']['recs']['PagesCount']

#     print("Retrieved No of Pages")

#     for i in range(1, pages+1):
#         page = i
#         url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetOrders&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
#         response = requests.request("GET", url, headers=headers, data=payload, verify=False)
#         response = response.json()
#         response = response['result']['body']['recs']['Results']
#         response = pd.DataFrame.from_dict(response)
#         orders = orders.append(response, ignore_index=True)
    
#     print("Finished API calls")

#     '''
#     ORDERS HEADERS
#     '''
#     """
#     orders_header = orders['details'].apply(pd.Series)

#     print("Separated Details")

#     orders_header.rename (columns = {
#             'Internal_Number':'internal_number',
#             'Document_Number':'document_number',
#             'Canceled':'order_canceled',
#             'Document_Status':'document_status',
#             'Warehouse_Status':'warehouse_status',
#             'Posting_Date':'posting_date',
#             'Customer_Vendor_Code':'cust_vendor_code',
#             'Total_Tax':'total_tax',
#             'Discount_for_Document':'order_document_discount',
#             'Total_Discount':'order_total_discount',
#             'Document_Total':'doc_total',
#             'Generation_Time':'generation_time',
#             'Sales_Employee':'sales_employee',
#             'Tax_Amount_SC':'tax_amt_sc',
#             'Document_Total_SC':'doc_total_sc',
#             'Creation_Date':'creation_date',
#             'User_Signature':'user_signature',
#             'Creatn_Time_Incl_Secs':'creation_time',
#             'Prescription_Spectacle':'presc_spec',
#             'Advanced_Payment':'advanced_payment',
#             'Remaining_Payment':'remaining_payment',
#             'Order_Screen':'orderscreen',
#             'Branch':'order_branch',
#             'Prescription_No':'presc_no',
#             'Presc_Print_Type':'presc_print_type',
#             'Draft_Order_No':'draft_orderno'}
#         ,inplace=True)

#     print("Columns Renamed")

#     orders_header["posting_date"] = pd.to_datetime(orders_header["posting_date"])

#     orders_header = orders_header.set_index(['internal_number','document_number'])

#     upsert(engine=engine,
#        df=orders_header,
#        schema='mabawa_staging',
#        table_name='source_orders_header',
#        if_row_exists='update',
#        create_table=True)

#     print("Inserted Orders Header")
#     """

#     '''
#     ORDERS LINE
#     '''
    
#     orders_line = orders['itemdetails']

#     orders_line = orders_line.to_frame()

#     print("To Frame")

#     orders_line_df = pd.DataFrame()
#     for index, row in orders_line.iterrows():
#         row_data=row['itemdetails']
#         row_data = pd.DataFrame.from_dict(row_data)
#         row_data = row_data.T
#         orders_line_df = orders_line_df.append(row_data, ignore_index=True)

#     print("Unnested")
#     print(orders_line_df)

#     orders_line_df.rename (columns = {
#             'DocumentInternal_ID':'doc_internal_id',
#             'Target_Document_Internal_ID':'target_doc_internal_id',
#             'Row_Status':'row_status',
#             'Item_No':'item_no',
#             'Quantity':'qty',
#             'Unit_Price':'unit_price',
#             'Discount_per_Row':'disc_per_row',
#             'Warehouse_Code':'warehouse_code',
#             'Gross_Price_after_Discount':'gross_price_after_disc',
#             'Total_Tax_Row':'total_tax_row',
#             'Flag_for_order':'flag_for_order'}
#         ,inplace=True)

#     print("Columns Renamed")

#     query = """truncate mabawa_staging.landing_source_orders_line;"""
#     query = pg_execute(query)

#     orders_line_df.to_sql('landing_source_orders_line', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)


#     print("Orders line inserted")

#     return "Fetched Orders"

# def update_source_orders_line():

#     df = pd.read_sql("""
#     SELECT 
#     rowno, doc_internal_id, target_doc_internal_id, row_status, 
#     (case when item_no is null then 'NO CODE'
#     else item_no end) as item_no, qty, unit_price, 
#     disc_per_row, warehouse_code, gross_price_after_disc, total_tax_row, flag_for_order
#     FROM mabawa_mviews.v_source_orders_line
#     """, con=engine)

#     df = df.set_index(['rowno', 'doc_internal_id', 'item_no'])
#     print("Data Indexed")

#     upsert(engine=engine,
#        df=df,
#        schema='mabawa_staging',
#        table_name='source_orders_line',
#        if_row_exists='update',
#        create_table=False)
    
#     print("Data Updated")

#     return "Created"

# def create_order_live():

#     query = """
#     truncate mabawa_dw.fact_orders_header;
#     insert into mabawa_dw.fact_orders_header
#     SELECT 
#         internal_number, document_number, order_canceled, document_status, warehouse_status, 
#         posting_date, cust_vendor_code, total_tax, order_document_discount, order_total_discount, 
#         doc_total, generation_time, sales_employee, tax_amt_sc, doc_total_sc, user_signature, 
#         creation_date, creation_time_int, creation_time, creation_datetime, presc_spec, advanced_payment, 
#         remaining_payment, orderscreen, order_branch, presc_no, presc_print_type, draft_orderno
#     FROM mabawa_mviews.v_fact_orders_header;
#     """
#     query = pg_execute(query)
#     return "Created"
