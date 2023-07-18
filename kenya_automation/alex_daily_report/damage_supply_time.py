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
from sub_tasks.api_login.api_login import(login)
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")


##Define the days that is yesterday
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
formatted_date = yesterday.strftime('%Y-%m-%d')

def damage_suppy_time():
    status = """ select odsc_createdby as "Created User",odsc_date as "Date", odsc_time,
                    to_char(odsc_time, 'FM999:09:99'::text)::time without time zone AS "Time",
                    odsc_status as "Status", so.doc_entry as "DocEntry", so.odsc_doc_no,so2.doc_no as "DocNum",so2.ods_ordercriteriastatus as "OrderCriteria Status"
                    from mabawa_staging.source_orderscreenc1 so
                    left join mabawa_staging.source_orderscreen so2 on so2.doc_entry = so.doc_entry 
                    where odsc_date::date between '{yesterday}'and '{yesterday}'
                    """.format(yesterday=yesterday)
    
    status = pd.read_sql_query(status,con=conn)   
    status['Date']=pd.to_datetime(status['Date'],dayfirst=True )
    status["Datetime"]=pd.to_datetime(status.Date.astype(str) + ' ' + status.Time.astype(str), format="%Y%m%d %H:%M:%S", errors='coerce')
    # status = status.sort_values(by = ["Datetime"],ascending=False)
    # print(status[status['DocNum'] == '23701635'])
    
    """Designer Store Damage Supply time"""
    designerrejected = status[status['Status']== 'Rejected Frame sent to Frame Store']    
    designerrejected1 = designerrejected[status['Created User']== 'control1']
    designerrejected2 = designerrejected[status['Created User']== 'control2']
    designerrejected=pd.concat([designerrejected1,designerrejected2])

    reissuedframedes = status[status['Status']== 'ReIssued Frame for Order']
    reissuedframedes1 = status[status['Created User']== 'designer1']
    reissuedframedes2 = status[status['Created User']== 'designer2']
    reissuedframedes=pd.concat([reissuedframedes1,reissuedframedes2])    

    reissuedframedes = reissuedframedes.rename(columns={'Datetime':'Datetimeout'})    
    reissueframedes= pd.merge(designerrejected,reissuedframedes[['DocNum','Datetimeout']], on='DocNum', how='left')
    # reissueframedes = reissueframedes[reissueframedes['Datetimeout'] == yesterday]
    print(reissueframedes)

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
        
    RejectedSentWk_hrs=reissueframedes.apply(lambda row: BusHrs(row['Datetime'], row['Datetimeout']), axis=1)

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

    RejectedSentSat_hrs=reissueframedes.apply(lambda row: BusHrs(row['Datetime'], row['Datetimeout']), axis=1)

    reissueframedes["delay"]=(RejectedSentWk_hrs+RejectedSentSat_hrs)*60

    reissueframedes =reissueframedes.replace("", np.nan)
    reissueframedes=reissueframedes.dropna()
    reissueframedes['delayed dept']=reissueframedes['delay'].apply(lambda x: 'Delayed at Designer Store' if x>15 else 'Did not delay at Designer Store' )
    designerdamage_reissued=reissueframedes

    """Main Store Damage Supply time"""
    mainrejected = status[status['Status']== 'Rejected Frame sent to Frame Store']
    control = ('control1','control2')
    mainrejected= mainrejected[status['Created User'].isin(control)]

    reissuedframemain = status[status['Status']== 'ReIssued Frame for Order']
    main = ('main1','main2')
    reissuedframemain=status[status['Created User'].isin(main)]
    reissuedframemain.rename(columns={'Datetime':'Datetimeout'}, inplace=True)
    reissueframemain= pd.merge(mainrejected,reissuedframemain[['DocNum','Datetimeout']], on='DocNum', how='left')

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
        
    RejectedSentWk_hrs=reissueframemain.apply(lambda row: BusHrs(row['Datetime'], row['Datetimeout']), axis=1)

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

    RejectedSentSat_hrs=reissueframemain.apply(lambda row: BusHrs(row['Datetime'], row['Datetimeout']), axis=1)

    
    reissueframemain["delay"]=(RejectedSentWk_hrs+RejectedSentSat_hrs)*60

    reissueframemain =reissueframemain.replace("", np.nan)
    reissueframemain=reissueframemain.dropna()
    reissueframemain['delayed dept']=reissueframemain['delay'].apply(lambda x: 'Delayed at Main store' if x>15 else 'Did not delay at Main store' )
    mainstoredamage_reissued=reissueframemain


    """Lens Store Damage Supply Time"""
    lensrejected = status[status['Status']== 'Rejected Lenses sent to Lens Store']
    lensrejected=lensrejected[status['Created User'].isin(control)]
    reissuedlens = status[status['Status']== 'ReIssued Lens for Order']
    reissuedlens.rename(columns={'Datetime':'Datetimeout'}, inplace=True)
    reissuedlens= pd.merge(lensrejected,reissuedlens[['DocNum','Datetimeout']], on='DocNum', how='left')

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
        
    RejectedSentWk_hrs=reissuedlens.apply(lambda row: BusHrs(row['Datetime'], row['Datetimeout']), axis=1)

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

    RejectedSentSat_hrs=reissuedlens.apply(lambda row: BusHrs(row['Datetime'], row['Datetimeout']), axis=1)

    reissuedlens["delay"]=(RejectedSentWk_hrs+RejectedSentSat_hrs)*60

    reissuedlens =reissuedlens.replace("", np.nan)
    reissuedlens=reissuedlens.dropna()
    reissuedlens['delayed dept']=reissuedlens['delay'].apply(lambda x: 'Delayed at Lens store' if x>15 else 'Did not delay at Lens store' )
    lenstoredamage_reissued=reissuedlens
    print(lenstoredamage_reissued)

    """Dropping What's needed """
    designerdamage_reissued = designerdamage_reissued.drop(columns={'delayed dept'})
    mainstoredamage_reissued= mainstoredamage_reissued.drop(columns={'delayed dept'})
    lenstoredamage_reissued= lenstoredamage_reissued.drop(columns={'delayed dept'})

    """Renaming & Sorting"""
    designerdamage_reissued = designerdamage_reissued.rename(columns={'delay':'Time taken'})
    mainstoredamage_reissued= mainstoredamage_reissued.rename(columns={'delay':'Time taken'})
    lenstoredamage_reissued= lenstoredamage_reissued.rename(columns={'delay':'Time taken'})

    designerdamage_reissued = designerdamage_reissued.sort_values(by='Time taken',ascending=False)
    mainstoredamage_reissued= mainstoredamage_reissued.sort_values(by='Time taken',ascending=False)
    lenstoredamage_reissued= lenstoredamage_reissued.sort_values(by='Time taken',ascending=False)


    """"Control Room Damage Supply Time"""
    data = status
    data['Date'] = pd.to_datetime(data['Date'], dayfirst=True).dt.date
    data['Date time'] = data['Date'].astype(str)+' '+data['Time'].astype(str)
    data['Date time'] = pd.to_datetime(data['Date time'], format='%Y/%m/%d %H:%M:%S',errors='coerce')

    lens_data = data
    lens_data=lens_data.drop_duplicates(subset=['DocEntry','Status'], keep="first")
    lens_in = lens_data[(lens_data['Status']=='Rejected Order Sent To Control Room')
                | (lens_data['Status']=='Surfacing Damage/Reject Sent to Control Room')
                | (lens_data['Status']=='Rejected Order Sent To Control Room')]
    lens_in = lens_in.rename(columns={'Date time':'Timein'})
   
    lens_out = lens_data[(lens_data['Status']=='Rejected Lenses sent to Lens Store')
              | (lens_data['Status']=='Rejected Frame sent to Frame Store')]
    lens_out = lens_out.rename(columns={'Date time':'Timeout'})

    lens_duration = pd.merge(lens_in,lens_out[['DocEntry','Timeout']], on='DocEntry',how='left')
    lens_duration = lens_duration.dropna(subset=['Timeout'])

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
        
    RejectedSentWk_hrs=lens_duration.apply(lambda row: BusHrs(row['Timein'], row['Timeout']), axis=1)

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

    RejectedSentSat_hrs=lens_duration.apply(lambda row: BusHrs(row['Timein'], row['Timeout']), axis=1)

    lens_duration["Time Taken"]=(RejectedSentWk_hrs+RejectedSentSat_hrs)*60
    lens_duration =lens_duration.replace("", np.nan)
    lens_duration = lens_duration.sort_values(by='Time Taken',ascending=False)
    
    control_data = data
    control_data=control_data.drop_duplicates(subset=['DocEntry','Status'], keep="last")
    control_in = control_data[(control_data['Status']=='ReIssued Lens for Order')
                | (control_data['Status']=='ReIssued Frame for Order')]
    control_in = control_in.rename(columns={'Date time':'Timein'})

    control_data2 = data
    control_data2=control_data2.drop_duplicates(subset=['DocEntry','Status'], keep="last")
    control_out1 = control_data2[control_data2['Status']=='Sent to Surfacing']
    control_out2 = control_data2[control_data2['Status']=='Sent to Pre Quality']

    control_out = pd.concat([control_out1,control_out2])
    control_out = control_out.drop_duplicates(subset='DocEntry')
    control_out = control_out.rename(columns={'Date time':'Timeout'})

    control_duration = pd.merge(control_in,control_out[['DocEntry','Timeout']], on='DocEntry',how='left')
    control_duration = control_duration.dropna(subset=['Timeout'])

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

    RejectedSentWk_hrs=control_duration.apply(lambda row: BusHrs(row['Timein'], row['Timeout']), axis=1)

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
    RejectedSentSat_hrs=control_duration.apply(lambda row: BusHrs(row['Timein'], row['Timeout']), axis=1)

    control_duration["Time Taken"]=(RejectedSentWk_hrs+RejectedSentSat_hrs)*60
    control_duration =control_duration.replace("", np.nan)
    control_duration = control_duration.sort_values(by='Time Taken',ascending=False)

    lens_duration = lens_duration[['Status','DocEntry','DocNum','OrderCriteria Status','Timein','Timeout','Time Taken']]
    control_duration=control_duration[['Status','DocEntry','DocNum','OrderCriteria Status','Timein','Timeout','Time Taken']]


    """"Copy the data to an excel sheet """
    with pd.ExcelWriter(r"/home/opticabi/Documents/optica_reports/order_efficiency\newdamagesupplytime.xlsx", engine='xlsxwriter') as writer:
        designerdamage_reissued.to_excel(writer, sheet_name='designer',index=False)
        mainstoredamage_reissued.to_excel(writer, sheet_name='mainstore',index=False)
        lenstoredamage_reissued.to_excel(writer, sheet_name='lensstore',index=False)
        lens_duration.to_excel(writer,sheet_name='control to store', index=False)        
        control_duration.to_excel(writer,sheet_name='store to control', index=False)
    writer.save()

    def save_xls(list_dfs, xls_path):
        with ExcelWriter(xls_path) as writer:
            for n, df in enumerate(list_dfs):
                df.to_excel(writer,'sheet%s' % n)
            writer.save() 
            
# damage_suppy_time()