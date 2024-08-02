import sys
sys.path.append(".")

#import libraries
import json
import datetime
import psycopg2
import requests
import pygsheets
import pandas as pd
from datetime import date
from airflow.models import Variable
from sqlalchemy import create_engine
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record  


from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine) 
from sub_tasks.api_login.api_login import(login)


def fetch_novax_data():
    
     gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
     sh = gc.open_by_key('11b4saaTI-xmNooQ_9IJwDHoF8GsOQz_v991k8c8Z11Y')
     wk1 = sh.worksheet_by_title("Revised Novax Data")
     wk1 = pd.DataFrame(wk1.get_all_records())
     wk1.rename (columns = {'Order_DATE':'order_date', 
                       'Order_Delivery_DATE':'order_delivery_date', 
                       'OrdId':'order_no', 
                        'CusCompany':'branch_name',
                        'ProFullName':'pro_full_name'}
            ,inplace=True)
     print(wk1.columns)
     wk1 = wk1.drop_duplicates(subset = "order_no",keep = 'first') 
     wk1["order_date"] = pd.to_datetime(wk1["order_date"])
     wk1["order_delivery_date"] = pd.to_datetime(wk1["order_delivery_date"])
     wk1 = wk1.set_index('order_no')
     substrings_to_exclude = ['ACACIA', 'ARENA', 'OASIS', 'BOULVARD', 'KIGALI', 'SILVERBACK']
     wk1 = wk1[~wk1['branch_name'].str.contains('|'.join(substrings_to_exclude))]

     upsert(engine=engine,
          df=wk1,
          schema='mabawa_staging',
          table_name='source_novax_data',
          if_row_exists='update',
          create_table=False)

     return 'something' 


def create_dim_novax_data():
    
     query = """
     truncate mabawa_dw.dim_novax_data;
     insert into mabawa_dw.dim_novax_data
     SELECT order_no, order_date, order_delivery_date, 
     "OrdRefNo",
     NULLIF(regexp_replace("OrdRefNo", '\D','','g'), '')::numeric AS doc_no, 
     branch_name, pro_full_name
     FROM mabawa_staging.source_novax_data;
     """

     query = pg_execute(query)
     print(query)
     return 'something' 


def fetch_dhl_data():
    
     gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
     sh = gc.open_by_key('11b4saaTI-xmNooQ_9IJwDHoF8GsOQz_v991k8c8Z11Y')
     wk1 = sh.worksheet_by_title("DHL Data")
     wk1 = pd.DataFrame(wk1.get_all_records())
     print(wk1)
     wk1.rename (columns = {'Invoice Date':'invoice_date', 
                       'Delivery Date':'delivery_date', 
                       'Delivery Time':'delivery_time', 
                        'AWB':'awb',
                        'Invoice':'invoice',
                        'PARCEL DESCRIPTION':'parcel_description',
                        'NUMBER OF PARCELS':'number_of_parcels'}
            ,inplace=True)

     # wk1 = wk1.drop_duplicates()
     # wk1 = wk1.set_index('invoice')

     query = """truncate mabawa_staging.source_dhl_data;"""
     query = pg_execute(query)

     wk1.to_sql('source_dhl_data', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
     print(wk1)
     return 'something'



def create_dim_dhl_data():
    
     query = """
     truncate mabawa_dw.dim_dhl_data;
     insert into mabawa_dw.dim_dhl_data
     SELECT 
          to_date(invoice_date,'dd-MM-yy') as invoice_date,
          to_date(delivery_date,'dd-MM-yy') as delivery_date, 
          case when delivery_time <> '' then lpad(delivery_time::text,4,'0')::time end as delivery_time,
          awb,
          invoice,
          parcel_description,
          number_of_parcels
     FROM mabawa_staging.source_dhl_data
     """
     
     query = pg_execute(query)
     
     return 'something'

def create_dhl_with_orderscreen_data():
    
     query = """
     truncate mabawa_dw.dhl_with_orderscreen_data;
     insert into mabawa_dw.dhl_with_orderscreen_data
     SELECT invoice_date, delivery_date, delivery_time, awb, invoice, parcel_description,number_of_parcels,order_screen, order_entry
     FROM mabawa_dw.v_dhl_with_orderscreen_data;
     """
     
     query = pg_execute(query)
     
     return 'something'

# fetch_novax_data()
# create_dim_novax_data()
# fetch_dhl_data()
# create_dim_dhl_data()
# create_dhl_with_orderscreen_data()