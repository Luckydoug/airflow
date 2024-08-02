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


def refresh_et_conv():

    query = """
    refresh materialized view mawingu_mviews.et_conv;
    """

    query = pg_execute(query)






