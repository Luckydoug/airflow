import sys

from numpy import nan
sys.path.append(".")

#import libraries
from io import StringIO
import json
import psycopg2
import requests
import pandas as pd
from pandas.io.json._normalize import nested_to_record 
from sqlalchemy import create_engine
from airflow.models import Variable
from pandas.io.json._normalize import nested_to_record 
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from datetime import date, timedelta
import datetime


from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.api_login.api_login import(login)
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def create_incentive_cash():

    query = """
    refresh materialized view mabawa_mviews.m_cash_payment_details;
    refresh materialized view mabawa_mviews.m_cash_or_insurance_order;
    refresh materialized view mabawa_mviews.m_ojdt_details;
    refresh materialized view mabawa_mviews.m_discount_details;
    refresh materialized view mabawa_mviews.incentive_cash; 
    """

    query = pg_execute(query)
    print('cash incentive done')

def create_incentive_insurance():
    query = """
    truncate mabawa_mviews.incentive_insurance;
    insert into mabawa_mviews.incentive_insurance
    SELECT origin_no, posting_date, je_type, remarks, draft_order_no, doc_no, ods_creator, user_name, 
    ods_outlet, ods_status, ods_normal_repair_order, "month", "year"
    FROM mabawa_mviews.v_incentive_insurance;
    insert into mabawa_dw.update_log(table_name, update_time) values('incentives', default);
    """

    query = pg_execute(query)
    print('insurance incentive done')
    
   
# create_incentive_cash()
# create_incentive_insurance()