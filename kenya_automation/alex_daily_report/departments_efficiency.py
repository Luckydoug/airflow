import sys
import numpy as np
sys.path.append(".")

# Import Libraries
import json
import psycopg2
import requests
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import Variable
from airflow.exceptions import AirflowException
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

##Others
import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from sub_tasks.libraries.styles import styles, properties
from sub_tasks.libraries.utils import get_todate,send_report,assert_date_modified, create_initial_file, return_sent_emails, fetch_gsheet_data, record_sent_branch, fourth_week_start, fourth_week_end


from reports.draft_to_upload.utils.utils import return_report_daterange
from reports.draft_to_upload.utils.utils import get_report_frequency

# PG Execute(Query)
from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.api_login.api_login import(login)
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")



##Define the days that is yesterday
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)

def update_calculated_field():
    OrdersWithIssues = fetch_gsheet_data()["orders_with_issues"]
    orders_cutoff = fetch_gsheet_data()["orders_cutoff"]
    departments = """
    SELECT dept, status, "Order Criteria", "Doc Entry", "Doc No", "start", finish, "Time Min"
    FROM mabawa_mviews.v_orderefficiencydata
    where finish::date = '{yesterday}' 
    """
    departments = pd.read_sql_query(departments,con=conn)    
    

    """ G-sheet with Orders to Drop"""
    gc = pygsheets.authorize(
    service_file=r"/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json")
    sh = gc.open_by_key('1cnpNo85Hncf9cdWBfkQ1dn0TYnGfs-PjVGp1XMjk2Wo')
    OrdersWithIssues = pd.DataFrame(sh[0].get_all_records())
    print(OrdersWithIssues)
    OrdersWithIssues["DATE"] = pd.to_datetime(OrdersWithIssues.DATE, dayfirst=True, errors="coerce")
    OrdersWithIssues = OrdersWithIssues[(OrdersWithIssues.DATE >= '{yesterday}') & (OrdersWithIssues.DATE <= '{yesterday}')]
    

    # """ITR Cut off"""
    print(orders_cutoff)

    
    dept_orders = departments[departments['dept'].isin(['Control', 'Designer', 'Main Store', 'Packaging', 'Lens Store'])]
    dept_orders['finish'] = dept_orders['finish'].astype(str)
    dept_orders[['Day', 'Time']] = dept_orders['finish'].str.split(' ', expand=True)
    dept_orders['Time'] = pd.to_datetime(dept_orders['Time'], format='%H:%M:%S')
    dept_orders['Hour'] = dept_orders['Time'].dt.strftime('%H')
    # dept_orders_matrix = pd.merge(dept_orders, orders_cutoff, on='Order Criteria', how='left')
    # dept_orders_matrix['cut off'] = np.where(dept_orders_matrix['dept'] == 'Control', dept_orders_matrix['Control Room'],
    #                                           (np.where(dept_orders_matrix['dept'] == 'Designer',dept_orders_matrix['Designer Store'], 
    #                                                     (np.where(dept_orders_matrix['dept'] == 'Main Store', dept_orders_matrix['Main Store'],
    #                                                                (np.where(dept_orders_matrix['dept'] == 'Lens Store', dept_orders_matrix['Lens Store'],
    #                                                                           (np.where(dept_orders_matrix['dept'] == 'Packaging', dept_orders_matrix['Packaging'], 10)))))))))
    
    # dept_orders_matrix['Delay'] = np.where(dept_orders_matrix['Time Min'] > dept_orders_matrix['cut off'], 1, 0)

    """Control Room Efficiency"""
    controlIssues = OrdersWithIssues[OrdersWithIssues["DEPARTMENT"] == "CONTROL ROOM"]
    controlIssuesOrders = controlIssues["ORDER NUMBER"].tolist()

    control = dept_orders[dept_orders['dept'] == 'Control']
    control = control[~control["Doc No"].isin(controlIssuesOrders)]
    controlpivot1 = pd.pivot_table(control, index=['Hour'], values=[
                                'Doc No'], aggfunc='count', fill_value=0, margins=True, margins_name='Total')
    controlpivot2 = pd.pivot_table(control, index=['Hour'], values=[
                                'Time Min'], aggfunc='mean', fill_value=0, margins=True, margins_name='Total')
    controlpivot3 = pd.pivot_table(control, index=['Hour'], values=[
                                'Delay'], aggfunc='sum', fill_value=0, margins=True, margins_name='Total')
    
    controlpivot4 = pd.merge(controlpivot1, controlpivot2, on=['Hour'], how='left')
    controlpivot = pd.merge(controlpivot4, controlpivot3, on=['Hour'], how='left')
    print(controlpivot)
    controlpivot['% of Efficiency'] = (controlpivot['Doc No'] - controlpivot['Delay'])/controlpivot['Doc No']

    controlpivot['Time Min'] = controlpivot['Time Min'].round(2)
    controlpivot['% of Efficiency'] = controlpivot['% of Efficiency'].map('{:.2%}'.format)
    controlpivot = np.transpose(controlpivot)

    print(controlpivot)

    """ Delayed Orders"""
    # control_delay = control[control['Delay'] == 0]
    # controldelay = pd.pivot_table(control_delay, index=['Hour', 'Doc No'], values='Time Min', aggfunc='mean', fill_value=0)

    """Cut Off"""
    # control['Time Taken'] = control.apply 
    # control['15 min'] = control['Time Min'].apply(lambda x: 1 if x > 15 else 0)
    # control['12 min'] = control['Time Min'].apply(lambda x: 1 if x > 12 else 0)
    # control['10 min'] = control['Time Min'].apply(lambda x: 1 if x > 10 else 0)
    # control['7 min'] = control['Time Min'].apply(lambda x: 1 if x > 7 else 0)

print('Control Room Calculated')
update_calculated_field()    