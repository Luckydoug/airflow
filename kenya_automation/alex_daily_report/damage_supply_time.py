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
dateyesterday = pd.to_datetime(yesterday)

def damage_suppy_time():
    status = """ select odsc_createdby as "Created User",odsc_date as "Date", odsc_time,
                    to_char(odsc_time, 'FM999:09:99'::text)::time without time zone AS "Time",
                    odsc_status as "Status", so.doc_entry as "DocEntry", so.odsc_doc_no,so2.doc_no as "DocNum",so2.ods_ordercriteriastatus as "OrderCriteria Status"
                    from mabawa_staging.source_orderscreenc1 so
                    left join mabawa_staging.source_orderscreen so2 on so2.doc_entry = so.doc_entry 
                    where odsc_date::date >= '2023-08-01'
                    """
    
    status = pd.read_sql_query(status,con=conn)   
    status['Date']=pd.to_datetime(status['Date'],dayfirst=True )
    status["Datetime"]=pd.to_datetime(status.Date.astype(str) + ' ' + status.Time.astype(str), format="%Y%m%d %H:%M:%S", errors='coerce')
    
    ##Define control room rejection status
    # controlrejectionstatus = ('Rejected Frame sent to Frame Store')

    """Designer Store Damage Supply time"""
    designerrejected = status[status['Status']=='Rejected Frame sent to Frame Store']      
    designerrejected1 = designerrejected[designerrejected['Created User']== 'control1']
    designerrejected2 = designerrejected[designerrejected['Created User']== 'control2']
    designerrejected=pd.concat([designerrejected1,designerrejected2])
    designerrejected = designerrejected.sort_values(by = ['Datetime'],ascending =  False)

    reissuedframedes = status[status['Status']== 'ReIssued Frame for Order']
    reissuedframedes = reissuedframedes[reissuedframedes["Date"] == dateyesterday]  
    reissuedframedes1 = reissuedframedes[reissuedframedes['Created User']== 'designer1']
    reissuedframedes2 = reissuedframedes[reissuedframedes['Created User']== 'designer2']
    reissuedframedes=pd.concat([reissuedframedes1,reissuedframedes2])    

    reissuedframedes = reissuedframedes.rename(columns={'Datetime':'Datetimeout'})    
    reissueframedes= pd.merge(designerrejected,reissuedframedes[['DocNum','Datetimeout']], on='DocNum', how='right')
    # reissueframedes = reissueframedes[reissueframedes['Datetimeout'].notna()]
    print(reissueframedes)

    ##Define a working day
    ####Days of the week
    workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(18),
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

    if not reissueframedes.empty:
        reissueframedes["delay"]=(RejectedSentWk_hrs+RejectedSentSat_hrs)*60
        print(reissueframedes)
        # reissueframedes['delayed dept']=reissueframedes['delay'].apply(lambda x: 'Delayed at Designer Store' if x>15 else 'Did not delay at Designer Store' )
        designerdamage_reissued=reissueframedes.copy()
        # designerdamage_reissued = designerdamage_reissued.drop(columns={'delayed dept'})
        designerdamage_reissued = designerdamage_reissued.rename(columns={'delay':'Time taken'})
        designerdamage_reissued = designerdamage_reissued.sort_values(by='Time taken',ascending=False)
        designerdamage_reissued = designerdamage_reissued[['Created User', 'Date', 'Time', 'Status','DocNum','OrderCriteria Status','Datetime','Datetimeout','Time taken']]
        print('Designer Store Printed')
    else:
        columns = ['Created User', 'Date', 'Time', 'Status','DocNum','OrderCriteria Status','Datetime','Datetimeout','Time taken'] 
        designerdamage_reissued = pd.DataFrame(columns=columns) 

    """Main Store Damage Supply time"""
    """First Rejection """
    print('Main Store Damage Supply time')
    mainrejected = status[status['Status']=='Rejected Frame sent to Frame Store']    
    control = ('control1','control2')
    mainrejected= mainrejected[status['Created User'].isin(control)]
    mainrejected["Main Store"] = mainrejected.groupby('DocNum').cumcount()
    mainrejected['Main Store'] += 1
    mainrejected = mainrejected[mainrejected['Main Store'] == 1]
    mainrejected = mainrejected.sort_values(by = ['Datetime'],ascending =  False)

    reissuedframemain = status[status['Status']== 'ReIssued Frame for Order']
    main = ('main1','main2')
    reissuedframemain=reissuedframemain[reissuedframemain['Created User'].isin(main)]
    reissuedframemain["Main Store"] = reissuedframemain.groupby('DocNum').cumcount()
    reissuedframemain['Main Store'] += 1
    reissuedframemain = reissuedframemain[reissuedframemain['Main Store'] == 1]
    reissuedframemain = reissuedframemain[reissuedframemain["Date"] == dateyesterday]
    reissuedframemain.rename(columns={'Datetime':'Datetimeout'}, inplace=True)

    reissueframemain= pd.merge(mainrejected,reissuedframemain[['DocNum','Datetimeout']], on='DocNum', how='left')
    reissueframemain['Datetimeout'] = pd.to_datetime(reissueframemain['Datetimeout'])
    reissueframemain['Datetime'] = pd.to_datetime(reissueframemain['Datetime'],format="%Y%m%d %H:%M:%S",errors = 'coerce')    
    reissueframemain = reissueframemain[reissueframemain['Datetimeout'].notna()]
    print(reissueframemain.columns)

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

    if not reissueframemain.empty:
        reissueframemain["delay"]=(RejectedSentWk_hrs+RejectedSentSat_hrs)*60
        # reissueframemain['delayed dept']=reissueframemain['delay'].apply(lambda x: 'Delayed at Main store' if x>15 else 'Did not delay at Main store' )
        mainstoredamage_reissued=reissueframemain.copy()
        # mainstoredamage_reissued= mainstoredamage_reissued.drop(columns={'delayed dept'})
        mainstoredamage_reissued= mainstoredamage_reissued.rename(columns={'delay':'Time taken'})
        mainstoredamage_reissued= mainstoredamage_reissued.sort_values(by='Time taken',ascending=False)
        mainstoredamage_reissued = mainstoredamage_reissued[['Created User', 'Date', 'Time', 'Status','DocNum','OrderCriteria Status','Datetime','Datetimeout','Time taken']]
        print(mainstoredamage_reissued)
        print('Main Store Printed')
    else:
        columns = ['Created User', 'Date', 'Time', 'Status','DocNum','OrderCriteria Status','Datetime','Datetimeout','Time taken']
        mainstoredamage_reissued = pd.DataFrame(columns=columns)  

    """Main Store Second Damage Supply Time"""
    """Second Rejection """
    print('Main Store Damage Supply time')
    mainrejectedsec = status[status['Status']=='Rejected Frame sent to Frame Store']    
    control = ('control1','control2')
    mainrejectedsec= mainrejectedsec[status['Created User'].isin(control)]
    mainrejectedsec["Main Store"] = mainrejectedsec.groupby('DocNum').cumcount()
    mainrejectedsec['Main Store'] += 1
    mainrejectedsec = mainrejectedsec[mainrejectedsec['Main Store'] == 2]
    mainrejectedsec = mainrejectedsec.sort_values(by = ['Datetime'],ascending =  False)

    reissuedframemainsec = status[status['Status']== 'ReIssued Frame for Order']
    main = ('main1','main2')
    reissuedframemainsec=reissuedframemainsec[reissuedframemainsec['Created User'].isin(main)]
    reissuedframemainsec["Main Store"] = reissuedframemainsec.groupby('DocNum').cumcount()
    reissuedframemainsec['Main Store'] += 1
    reissuedframemainsec = reissuedframemainsec[reissuedframemainsec['Main Store'] == 2]
    reissuedframemainsec = reissuedframemainsec[reissuedframemainsec["Date"] == dateyesterday]
    reissuedframemainsec.rename(columns={'Datetime':'Datetimeout'}, inplace=True)

    reissueframemainsec= pd.merge(mainrejectedsec,reissuedframemainsec[['DocNum','Datetimeout']], on='DocNum', how='left')
    reissueframemainsec['Datetimeout'] = pd.to_datetime(reissueframemainsec['Datetimeout'])
    print(reissueframemainsec.columns)
    reissueframemainsec['Datetime'] = pd.to_datetime(reissueframemainsec['Datetime'],format="%Y%m%d %H:%M:%S",errors = 'coerce')    
    reissueframemainsec = reissueframemainsec[reissueframemainsec['Datetimeout'].notna()]
    print(reissueframemainsec.columns)

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
        
    RejectedSentWk_hrs=reissueframemainsec.apply(lambda row: BusHrs(row['Datetime'], row['Datetimeout']), axis=1)

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

    RejectedSentSat_hrs=reissueframemainsec.apply(lambda row: BusHrs(row['Datetime'], row['Datetimeout']), axis=1)

    if not reissueframemainsec.empty:
        reissueframemainsec["delay"]=(RejectedSentWk_hrs+RejectedSentSat_hrs)*60
        # reissueframemain['delayed dept']=reissueframemain['delay'].apply(lambda x: 'Delayed at Main store' if x>15 else 'Did not delay at Main store' )
        mainstoredamage_reissuedsec=reissueframemainsec.copy()
        # mainstoredamage_reissued= mainstoredamage_reissued.drop(columns={'delayed dept'})
        mainstoredamage_reissuedsec= mainstoredamage_reissuedsec.rename(columns={'delay':'Time taken'})
        mainstoredamage_reissuedsec= mainstoredamage_reissuedsec.sort_values(by='Time taken',ascending=False)
        mainstoredamage_reissuedsec = mainstoredamage_reissuedsec[['Created User', 'Date', 'Time', 'Status','DocNum','OrderCriteria Status','Datetime','Datetimeout','Time taken']]
        print(mainstoredamage_reissuedsec)
        print('Main Store Printed')
    else:
        columns = ['Created User', 'Date', 'Time', 'Status','DocNum','OrderCriteria Status','Datetime','Datetimeout','Time taken']
        mainstoredamage_reissuedsec = pd.DataFrame(columns=columns)    

    """Lens Store Damage Supply Time"""
    """ First Rejection DST """
    lensrejected = status[status['Status']=='Rejected Lenses sent to Lens Store']   
    lensrejected = lensrejected.sort_values(by = ['Datetime'],ascending =  True) 
    lensrejected=lensrejected[lensrejected['Created User'].isin(control)]
    lensrejected["Lens Store"] = lensrejected.groupby('DocNum').cumcount()
    lensrejected['Lens Store'] += 1
    lensrejected = lensrejected[lensrejected['Lens Store'] == 1]
    lensrejected = lensrejected.sort_values(by = ['Datetime'],ascending =  True)
    
    reissuedlens = status[status['Status']== 'ReIssued Lens for Order']
    reissuedlens = reissuedlens.sort_values(by = ['Datetime'],ascending =  True)
    reissuedlens["Lens Store"] = reissuedlens.groupby('DocNum').cumcount()
    reissuedlens['Lens Store'] += 1
    reissuedlens = reissuedlens[reissuedlens['Lens Store'] == 1]
    reissuedlens = reissuedlens[reissuedlens["Date"] == dateyesterday]
    reissuedlens.rename(columns={'Datetime':'Datetimeout'}, inplace=True)
    reissuedlens= pd.merge(lensrejected,reissuedlens[['DocNum','Datetimeout']], on='DocNum', how='right')
    print(reissuedlens)
    print('printed reissuedlens')

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

    if not reissuedlens.empty:
        reissuedlens["delay"]=(RejectedSentWk_hrs+RejectedSentSat_hrs)*60
        # reissuedlens['delayed dept']=reissuedlens['delay'].apply(lambda x: 'Delayed at Designer Store' if x>15 else 'Did not delay at Designer Store' )
        lenstoredamage_reissued=reissuedlens.copy()
        # lenstoredamage_reissued= lenstoredamage_reissued.drop(columns={'delayed dept'})
        lenstoredamage_reissued= lenstoredamage_reissued.rename(columns={'delay':'Time taken'})
        lenstoredamage_reissued= lenstoredamage_reissued.sort_values(by='Time taken',ascending=False)
        lenstoredamage_reissued = lenstoredamage_reissued[['Created User', 'Date', 'Time', 'Status','DocNum','OrderCriteria Status','Datetime','Datetimeout','Time taken']]
    else:
        columns = ['Created User', 'Date', 'Time', 'Status','DocNum','OrderCriteria Status','Datetime','Datetimeout','Time taken']
        reissuedlens = pd.DataFrame(columns=columns) 
        print('Lens Store Printed')


    """ Second Lens Store Damage Supply Time"""
    lensrejectedsec = status[status['Status']=='Rejected Lenses sent to Lens Store']    
    lensrejectedsec=lensrejectedsec[lensrejectedsec['Created User'].isin(control)]
    lensrejectedsec = lensrejectedsec.sort_values(by = ['Datetime'],ascending =  True)
    lensrejectedsec["Lens Store"] = lensrejectedsec.groupby('DocNum').cumcount()
    lensrejectedsec['Lens Store'] += 1
    lensrejectedsec = lensrejectedsec[lensrejectedsec['Lens Store'] == 2]

    reissuedlenssec = status[status['Status']== 'ReIssued Lens for Order']
    reissuedlenssec = reissuedlenssec.sort_values(by = ['Datetime'],ascending =  True)
    reissuedlenssec["Lens Store"] = reissuedlenssec.groupby('DocNum').cumcount()
    reissuedlenssec['Lens Store'] += 1
    reissuedlenssec = reissuedlenssec[reissuedlenssec['Lens Store'] == 2]
    reissuedlenssec = reissuedlenssec[reissuedlenssec["Date"] == dateyesterday]
    reissuedlenssec.rename(columns={'Datetime':'Datetimeout'}, inplace=True)
    reissuedlenssec= pd.merge(lensrejectedsec,reissuedlenssec[['DocNum','Datetimeout']], on='DocNum', how='right')
     
    """ Calculation of Time"""
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
        
    RejectedSentWk_hrs=reissuedlenssec.apply(lambda row: BusHrs(row['Datetime'], row['Datetimeout']), axis=1)

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

    RejectedSentSat_hrs=reissuedlenssec.apply(lambda row: BusHrs(row['Datetime'], row['Datetimeout']), axis=1)

    if not reissuedlenssec.empty:
        reissuedlenssec["delay"]=(RejectedSentWk_hrs+RejectedSentSat_hrs)*60
        lenstoredamage_reissuedsec=reissuedlenssec.copy()
        lenstoredamage_reissuedsec= lenstoredamage_reissuedsec.rename(columns={'delay':'Time taken'})
        lenstoredamage_reissuedsec= lenstoredamage_reissuedsec.sort_values(by='Time taken',ascending=False)
        lenstoredamage_reissuedsec = lenstoredamage_reissuedsec[['Created User', 'Date', 'Time', 'Status','DocNum','OrderCriteria Status','Datetime','Datetimeout','Time taken']]
    else:
        columns = ['Created User', 'Date', 'Time', 'Status','DocNum','OrderCriteria Status','Datetime','Datetimeout','Time taken']
        lenstoredamage_reissuedsec = pd.DataFrame(columns=columns) 
        print('Lens Store Printed')


    """"Control Room Damage Supply Time"""
    print('Control Room Damage Supply Time')
    data = status
    data['Date'] = pd.to_datetime(data['Date'], dayfirst=True).dt.date
    data['Date time'] = data['Date'].astype(str)+' '+data['Time'].astype(str)
    data['Date time'] = pd.to_datetime(data['Date time'], format='%Y/%m/%d %H:%M:%S',errors='coerce')

    lens_data = data   
    # lens_data=lens_data.drop_duplicates(subset=['DocEntry','Status'], keep="first")
    lens_in = lens_data[(lens_data['Status']=='Rejected Order Sent To Control Room')| (lens_data['Status']=='Surfacing Damage/Reject Sent to Control Room')| (lens_data['Status']=='Rejected Order Sent To Control Room')| (lens_data['Status']=='Branch Rejected Order Sent To Control Room')]
    lens_in["Date"] = pd.to_datetime(lens_in["Date"])   
    lens_in = lens_in.rename(columns={'Date time':'Timein'})
    lens_in = lens_in.sort_values(by = ['Timein'],ascending =  True)
    lens_in["Control Room"] = lens_in.groupby('DocNum').cumcount()
    lens_in['Control Room'] += 1
    lens_in = lens_in[lens_in['Control Room'] == 1]  
    lens_in = lens_in.rename(columns={'Date time':'Timein'})


    lens_out = lens_data[(lens_data['Status']=='Rejected Lenses sent to Lens Store') | (lens_data['Status']=='Rejected Frame sent to Frame Store') | (lens_data['Status']=='Sent to Workshop for Damage/Reject Analysis') | (lens_data['Status']=='Overseas Rejected at Later Stage')]
    lens_out["Date"] = pd.to_datetime(lens_out["Date"])
    lens_out = lens_out[lens_out["Date"] == dateyesterday]    
    lens_out = lens_out.rename(columns={'Date time':'Timeout'})
    lens_out = lens_out.sort_values(by = ['Datetime'],ascending =  True)
    lens_out["Control Room"] = lens_out.groupby('DocNum').cumcount()
    lens_out['Control Room'] += 1
    lens_out = lens_out[lens_out['Control Room'] == 1]    
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
    control_in = control_data[(control_data['Status']=='ReIssued Lens for Order')| (control_data['Status']=='ReIssued Frame for Order')]
    control_in = control_in.sort_values(by = ['Date time'],ascending =  True)
    control_in = control_in.rename(columns={'Date time':'Timein'})
    control_in["Control Room"] = control_in.groupby('DocNum').cumcount()
    control_in['Control Room'] += 1
    control_in = control_in[control_in['Control Room'] == 1]  

    control_data2 = data
    # control_data2 = control_data2[control_data2["Date"] == dateyesterday]
    control_out = control_data2[(control_data2['Status']=='Sent to Surfacing') & (control_data2['Status']=='Blanks Sent to Control Room')]
    control_out = control_out.sort_values(by = ['Date time'],ascending =  True)
    control_out["Control Room"] = control_out.groupby('DocNum').cumcount()
    control_out['Control Room'] += 1
    control_out = control_out[control_out['Control Room'] == 2]   
    control_out = control_out.rename(columns={'Date time':'Timeout'})
    control_duration = pd.merge(control_in,control_out[['DocEntry','Timeout']], on='DocEntry',how='right')
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

    if not control_duration.empty:
        control_duration["Time Taken"]=(RejectedSentWk_hrs+RejectedSentSat_hrs)*60
        control_duration =control_duration.replace("", np.nan)
        control_duration = control_duration.sort_values(by='Time Taken',ascending=False)
        # control_duration['delayed dept']=control_duration['delay'].apply(lambda x: 'Delayed at Main store' if x>15 else 'Did not delay at Main store' )
        control_duration=control_duration.copy()
        control_duration = control_duration[['Status', 'DocEntry', 'DocNum', 'OrderCriteria Status','Timein','Timeout','Time Taken']]
        print('Control Room Printed')
    else:
        columns = ['Status', 'DocEntry', 'DocNum', 'OrderCriteria Status','Timein','Timeout','Time taken']
        control_duration = pd.DataFrame(columns=columns) 
   
    """ Store to Control: Excluding Surfacing """
    control_datapre = data
    control_datapre=control_datapre.drop_duplicates(subset=['DocEntry','Status'], keep="last")
    control_in_pre = control_datapre[(control_datapre['Status']=='ReIssued Lens for Order')| (control_datapre['Status']=='ReIssued Frame for Order')]
    control_in_pre = control_in_pre.sort_values(by = ['Date time'],ascending =  True)
    control_in_pre = control_in_pre.rename(columns={'Date time':'Timein'})
    control_in_pre["Control Room"] = control_in_pre.groupby('DocNum').cumcount()
    control_in_pre['Control Room'] += 1
    control_in_pre = control_in_pre[control_in_pre['Control Room'] == 1]  

    control_data2_pre = data
    # control_data2_pre = control_data2_pre[(control_data2_pre["Date"] >= lastmonday) & (control_data2_pre["Date"] <= dateyesterday)]
    control_out_pre = control_data2_pre[control_data2_pre['Status']=='Sent to Pre Quality']
    surfacing = ('Surfacing Lens from KE with PF to Follow Glazed Other Country','Surfacing Lens with HQ Frame','Surfacing Lens with PF','Surfacing Lens with Branch Frame',
    'Surfacing Lens with PF Glazed at Branch','Surfacing Lens with Branch Frame Glazed at Branch','PF and Surfacing Lens from KE Glazed Other Country',
    'HQ Frame and Surfacing Lens from KE Glazed Other Country')
    control_out_pre = control_out_pre[~control_out_pre['OrderCriteria Status'].isin(surfacing)]
    control_out_pre = control_out_pre.sort_values(by = ['Date time'],ascending =  True)
    control_out_pre["Control Room"] = control_out_pre.groupby('DocNum').cumcount()
    control_out_pre['Control Room'] += 1
    control_out_pre = control_out_pre[control_out_pre['Control Room'] == 2]   
    control_out_pre = control_out_pre.rename(columns={'Date time':'Timeout'})
    control_duration_pre = pd.merge(control_in_pre,control_out_pre[['DocEntry','Timeout']], on='DocEntry',how='right')

    """ Calculation of Time"""
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

    RejectedSentWk_hrs=control_duration_pre.apply(lambda row: BusHrs(row['Timein'], row['Timeout']), axis=1)

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
    RejectedSentSat_hrs=control_duration_pre.apply(lambda row: BusHrs(row['Timein'], row['Timeout']), axis=1)

    if not control_duration_pre.empty:
        control_duration_pre["Time Taken"]=(RejectedSentWk_hrs+RejectedSentSat_hrs)*60
        control_duration_pre =control_duration_pre.replace("", np.nan)
        control_duration_pre = control_duration_pre.sort_values(by='Time Taken',ascending=False)
        control_duration_pre=control_duration_pre.copy()
        control_duration_pre = control_duration_pre[['Status', 'DocEntry', 'DocNum', 'OrderCriteria Status','Timein','Timeout','Time Taken']]
        print('Control Room Printed')
    else:
        columns = ['Status', 'DocEntry', 'DocNum', 'OrderCriteria Status','Timein','Timeout','Time taken']
        control_duration_pre = pd.DataFrame(columns=columns) 

    control_duration = pd.concat([control_duration,control_duration_pre])

    """"Copy the data to an excel sheet """
    with pd.ExcelWriter(r"/home/opticabi/Documents/optica_reports/order_efficiency\newdamagesupplytime.xlsx", engine='xlsxwriter') as writer:
        designerdamage_reissued.to_excel(writer, sheet_name='designer',index=False)
        mainstoredamage_reissued.to_excel(writer, sheet_name='mainstore',index=False)
        mainstoredamage_reissuedsec.to_excel(writer, sheet_name='mainstoresec',index=False)
        lenstoredamage_reissued.to_excel(writer, sheet_name='lensstore',index=False)
        lenstoredamage_reissuedsec.to_excel(writer, sheet_name='lensstoresec',index=False)
        lens_duration.to_excel(writer,sheet_name='control to store', index=False)        
        control_duration.to_excel(writer,sheet_name='store to control', index=False)
    writer.save()

    def save_xls(list_dfs, xls_path):
        with ExcelWriter(xls_path) as writer:
            for n, df in enumerate(list_dfs):
                df.to_excel(writer,'sheet%s' % n)
            writer.save() 

# damage_suppy_time()


                
        
