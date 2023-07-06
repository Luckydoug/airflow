import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pygsheets
import io
import pandas as pd
from datetime import date
from airflow.models import Variable
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 


from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine) 

def fetch_sg_targets():
    gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
    sh = gc.open_by_key('1U1l1OsifHM15mLX1XVNm_kALGfxXBYDNDtlnDNvPOKs')
    sh = sh[1]
    values = sh.get_all_values()
    sh = pd.DataFrame(values)
    csv_string = sh.to_csv(index=False,header=False)
    sh = pd.read_csv(io.StringIO(csv_string), na_values='')

    drop_table = """truncate table mabawa_dw.branch_sg_sales_targets;"""
    drop_table = pg_execute(drop_table)

    sh[['branch','target']].to_sql('branch_sg_sales_targets', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)

    print('sg sales targets pulled')

def create_source_sg_sales():

    query = """
    truncate mabawa_staging.source_sg_sales;
    insert into mabawa_staging.source_sg_sales
    SELECT doc_internal_id, target_doc_internal_id, item_no, item_desc, item_category, qty, internal_number, document_number, order_canceled, cust_vendor_code, sales_employee, creation_date, creation_time, order_branch, draft_orderno
    FROM mabawa_staging.v_sg_sales;
    """

    query = pg_execute(query)

def create_sg_summary():

    query = """
    truncate mabawa_mviews.branch_sg_summary;
    insert into mabawa_mviews.branch_sg_summary
    SELECT pk_date, branch_code, sales, target, days_in_month
    FROM mabawa_mviews.v_branch_sg_summary;
    """
    query = pg_execute(query)

    


