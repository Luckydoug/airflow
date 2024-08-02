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


from sub_tasks.data.connect_voler import (pg_execute, engine) 
# from sub_tasks.api_login.api_login import(login_rwanda)
conn = psycopg2.connect(host="10.40.16.19",database="voler", user="postgres", password="@Akb@rp@$$w0rtf31n")
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def create_incentive_cash():

    query = """
    refresh materialized view voler_mviews.m_cash_payment_details;
    refresh materialized view voler_mviews.m_cash_or_insurance_order;
    refresh materialized view voler_mviews.m_ojdt_details;
    refresh materialized view voler_mviews.m_discount_details;
    refresh materialized view voler_mviews.incentive_cash; 
    """

    query = pg_execute(query)
    print('cash incentive done')

def create_incentive_insurance():

    query = """
    refresh materialized view voler_mviews.incentive_insurance2; 
    insert into voler_dw.update_log(table_name, update_time) values('incentives', default);
    """

    query = pg_execute(query)
    print('insurance incentive done')

def refresh_lens_silh():

    query = """
    refresh materialized view voler_mviews.lens_incentive; 
    refresh materialized view voler_mviews.silh_incentive;
    """

    query = pg_execute(query)
    print('lens_silh done')


def refresh_insurance_feedback_conversion():

    query = """
    refresh materialized view voler_mviews.insurance_feedback_conversion;
    """

    query = pg_execute(query)
    
