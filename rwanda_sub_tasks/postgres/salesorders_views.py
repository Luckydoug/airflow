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
conn = psycopg2.connect(host="10.40.16.19",database="voler", user="postgres", password="@Akb@rp@$$w0rtf31n")
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def refresh_order_line_with_details():

    query = """
    refresh materialized view voler_mviews.source_orders_line_with_item_details;
    """

    query = pg_execute(query)


def refresh_salesorders_line_cl_and_rr():

    query = """
    refresh materialized view voler_mviews.salesorders_line_cl_and_rr;
    """

    query = pg_execute(query)


def refresh_fact_orders_header():

    query = """
    refresh materialized view voler_dw.fact_orders_header;
    """

    query = pg_execute(query)


# def refresh_order_contents():

#     query = """
#     refresh materialized view voler_mviews.order_contents;
#     """

#     query = pg_execute(query)




