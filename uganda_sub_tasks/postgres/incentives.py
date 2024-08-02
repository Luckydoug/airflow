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


from sub_tasks.data.connect_mawingu import (pg_execute, engine) 
# from sub_tasks.api_login.api_login import(login_uganda)
conn = psycopg2.connect(host="10.40.16.19",database="mawingu", user="postgres", password="@Akb@rp@$$w0rtf31n")
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def create_incentive_cash():

    query = """
    refresh materialized view mawingu_mviews.m_cash_payment_details;
    refresh materialized view mawingu_mviews.m_cash_or_insurance_order;
    refresh materialized view mawingu_mviews.m_ojdt_details;
    refresh materialized view mawingu_mviews.m_discount_details;
    refresh materialized view mawingu_mviews.incentive_cash; 
    refresh materialized view mawingu_mviews.errors_deductible;
    """

    query = pg_execute(query)
    print('cash incentive done')

def create_incentive_insurance():
    query = """
    refresh materialized view mawingu_mviews.incentive_insurance2;
    insert into mawingu_dw.update_log(table_name, update_time) values('incentives', default);
    """

    query = pg_execute(query)
    print('insurance incentive done')
    
   
def refresh_lens_silh():

    query = """
    refresh materialized view mawingu_mviews.lens_incentive; 
    refresh materialized view mawingu_mviews.silh_incentive;
    """

    query = pg_execute(query)
    print('lens_silh done')

def refresh_insurance_rejections():
    query = """
    refresh materialized view mawingu_mviews.insurance_rejections; 
    refresh materialized view mawingu_mviews.insurance_rejections_summary;
    """

    query = pg_execute(query)
    print('refresh_insurance_rejections done')


def refresh_sunglass_sales_summary():
    query = """
    refresh materialized view mawingu_mviews.sunglass_sales_summary; 
    """

    query = pg_execute(query)
    print('refresh_sunglass_sales_summary done')

def refresh_google_reviews_summary():
    query = """
    refresh materialized view mawingu_mviews.google_reviews_summary; 
    """

    query = pg_execute(query)
    print('refresh_google_reviews_summary done')

def refresh_insurance_feedback_conversion():
    query = """
    refresh materialized view mawingu_mviews.insurance_feedback_conversion; 
    """

    query = pg_execute(query)
    print('refresh_insurance_feedback_conversion done')

def refresh_sop():
    query = """
    refresh materialized view mawingu_mviews.sop; 
    refresh materialized view mawingu_mviews.sop_summary; 
    """

    query = pg_execute(query)
    print('refresh_sop done')

def refresh_nps_summary():
    query = """
    refresh materialized view mawingu_mviews.nps_surveys; 
    refresh materialized view mawingu_mviews.nps_summary; 
    """

    query = pg_execute(query)
    print('refresh_nps_summary done')


def refresh_all_activity():
    query = """
    refresh materialized view mawingu_mviews.all_activity; 
    """

    query = pg_execute(query)
    print('refresh_all_activity done')