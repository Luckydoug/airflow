import sys
import numpy as np
sys.path.append(".")

# Import Libraries
import json
import psycopg2
import requests
import pandas as pd
from pandas.io.json._normalize import nested_to_record 
from sqlalchemy import create_engine
from airflow.models import Variable
from airflow.exceptions import AirflowException
from pandas.io.json._normalize import nested_to_record 
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from datetime import date
import datetime
import pytz
import businesstimedelta
import pandas as pd
import holidays as pyholidays
from workalendar.africa import Kenya
import pygsheets
import mysql.connector as database
import urllib.parse

# PG Execute(Query)
from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.api_login.api_login import(login)
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")

SessionId = login()

##Define the days that is yesterday
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)

def update_calculated_field():
    departments = """
    SELECT dept, status, "Order Criteria", "Doc Entry", "Doc No", "start", finish, "Time Min"
    FROM mabawa_mviews.v_orderefficiencydata
    where finish::date >= '2023-01-01'
    """
    departments = pd.read_sql_query(departments,con=conn)    
    
    query = """truncate mabawa_dw.dim_orderefficiencydata;"""
    query = pg_execute(query) 

    print('truncate finished')
    departments.to_sql('dim_orderefficiencydata', con=engine, schema='mabawa_dw', if_exists = 'append', index=False) 

def get_calculated_field():
    orders = """
    SELECT dept, status, "Order Criteria", "Doc Entry", "Doc No", "start", finish, "Time Min"
    FROM mabawa_dw.dim_orderefficiencydata 
    """    
    orders = pd.read_sql_query(orders,con=conn)
    print('no orders')
    print(orders)

# get_calculated_field()    