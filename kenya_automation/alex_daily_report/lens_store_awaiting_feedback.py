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
from sub_tasks.libraries.utils import get_todate,send_report,assert_date_modified, create_initial_file, return_sent_emails, fetch_gsheet_data, record_sent_branch, fourth_week_start, fourth_week_end


# PG Execute(Query)
from sub_tasks.data.connect import (pg_execute, engine) 
# from sub_tasks.api_login.api_login import(login)
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")


#Define the days that is yesterday
today = datetime.date.today()
# today = datetime.date(2024,1,25)
yesterday = today - datetime.timedelta(days=1)
formatted_date = yesterday.strftime('%Y-%m-%d')
print(yesterday)


def awaiting_feedback():
    status = """ 
                select odsc_createdby as "Created User",odsc_date::date as "Date", odsc_time,
                to_char(odsc_time, 'FM999:09:99'::text)::time without time zone AS "Time",
                odsc_status as "Status", so.doc_entry as "DocEntry", so.odsc_doc_no,so2.doc_no as "DocNum",so2.ods_ordercriteriastatus as "OrderCriteria Status"
                from mabawa_staging.source_orderscreenc1 so
                left join mabawa_staging.source_orderscreen so2 on so2.doc_entry = so.doc_entry 
                where odsc_date::date >= '2023-08-01'
                """.format(yesterday=yesterday)    
    
    status = pd.read_sql_query(status,con=conn) 
    print(status)
    print('data printed')
    dateyesterday = pd.to_datetime(yesterday)
    printed = status[status['Status']=='PF & PL Sent to Lens Store']
    printed["Date"]=pd.to_datetime(printed["Date"],dayfirst = True).dt.date    
    printed['Date Time order printed'] = printed['Date'].astype(str)+' '+printed['Time'].astype(str)
    printed = printed.sort_values(by = ["Date Time order printed"],ascending=False)
    printed['Date Time order printed'] = pd.to_datetime(printed['Date Time order printed'], format = '%Y/%m/%d %H:%M:%S')
    print(printed)

    status['Date'] = pd.to_datetime(status['Date'],dayfirst=True).dt.date
    awaiting = status[status["Date"] == dateyesterday] 
    awaiting = awaiting[awaiting['Status']== 'Lens Store Awaiting Branch feedback from Client']
    print(awaiting)     
    awaiting['Date Time Awaiting feedback'] = awaiting['Date'].astype(str)+' '+awaiting['Time'].astype(str)
    # awaiting = awaiting.sort_values(by = ["Date Time Awaiting feedback"],ascending=True)
    awaiting['Date Time Awaiting feedback'] = pd.to_datetime(awaiting['Date Time Awaiting feedback'], format = '%Y/%m/%d %H:%M:%S')

    final = pd.merge(awaiting,printed[['DocNum','Date Time order printed']], on='DocNum', how='left')
    final = final[final['Date Time order printed'] !='']
    print(final)
    print('lets calculate the time taken')

    ##Define a working day
    ####Days of the week
    workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(19),
        working_days=[0,1, 2, 3, 4])

    cal = Kenya()
    hl = cal.holidays()
    vic_holidays = pyholidays.KE() 
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    businesshrs = businesstimedelta.Rules([workday, holidays], hl)

    def BusHrs(start, end):
        if end>=start:
            return businesshrs.difference(start,end).hours+float(businesshrs.difference(start,end).seconds)/float(3600)
        else:
            ""
        
    RejectedSentWk_hrs=final.apply(lambda row: BusHrs(row['Date Time order printed'], row['Date Time Awaiting feedback']), axis=1)

    # Define a working weekend day(Saturday)
    Saturday = businesstimedelta.WorkDayRule(start_time=datetime.time(9),end_time=datetime.time(17),working_days=[5])

    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    businesshrs = businesstimedelta.Rules([Saturday, holidays])

    def SatHrs(start, end):
        if end>=start:
            return businesshrs.difference(start,end).hours+float(businesshrs.difference(start,end).seconds)/float(3600)
        else:
            ""

    RejectedSentSat_hrs=final.apply(lambda row: BusHrs(row['Date Time order printed'], row['Date Time Awaiting feedback']), axis=1)                

    if not final.empty:
        final["delay"] = (RejectedSentWk_hrs + RejectedSentSat_hrs) * 60
        final.drop(columns={'Created User', 'Date', 'Time', 'DocEntry'}, inplace=True)
        final = final.rename(columns={'delay': 'Time taken'})
        final = final.sort_values(by=['Time taken'], ascending=False)

    else:
        columns = ['Status', 'DocNum', 'OrderCriteria Status', 'Datetime', 'Datetimeout']
        final = pd.DataFrame(columns=columns) 


    df_q = """      
    select 
        case when rcvng_to_lnsstr::date = rcvd_lnsstr::date then extract('hour' from rcvng_to_lnsstr) else 0 end as hr,
        case
            when tm_to_rcv between 0 and 10 then '0 - 10 Mins'
            when tm_to_rcv between 11 and 20 then '11 - 20 Mins'
            when tm_to_rcv between 21 and 30 then '21 - 30 Mins'
            else '+30 Mins'
        end as queue_time_category,
        count(*) as "Total Orders"
    from mabawa_mviews.lensstore_efficiency_from_receiving
    where rcvd_lnsstr::date = %(From)s
    group by hr, queue_time_category
    """

    df = pd.read_sql(df_q,con=conn,params={'From':yesterday})

    pvt = pd.pivot_table(df,values='Total Orders',index='hr',columns='queue_time_category',aggfunc='sum',margins=True)
    pvt1 = pd.pivot_table(df,values='Total Orders',index='hr',aggfunc='sum',margins=True)

    df = pd.merge(pvt1,(pvt.add_suffix(' %').div(pvt1.iloc[:, 0],axis=0) * 100).round(0),left_index=True,right_index=True).fillna(0).drop('All %',axis=1)
    df = df[['Total Orders', '0 - 10 Mins %', '11 - 20 Mins %', '21 - 30 Mins %', '+30 Mins %']]

    with pd.ExcelWriter("/home/opticabi/Documents/optica_reports/order_efficiency/lensstoreawaiting.xlsx", engine='xlsxwriter') as writer:
        final.to_excel(writer, sheet_name='Awaiting feedback', index=False)
        df.to_excel(writer, sheet_name='From Receiving',index=True)
    writer.save()    

    def save_xls(list_dfs, xls_path):
        with ExcelWriter(xls_path) as writer:
            for n, df in enumerate(list_dfs):
                df.to_excel(writer,'sheet%s' % n)
            writer.save()
  
# awaiting_feedback()