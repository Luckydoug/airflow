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
import calendar

##Others
import os
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from sub_tasks.libraries.styles import styles, properties
from sub_tasks.libraries.utils import get_todate,send_report,assert_date_modified, return_sent_emails, record_sent_branch
import smtplib
from email.mime.application import MIMEApplication

# PG Execute(Query)
from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.api_login.api_login import(login)
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")

SessionId = login()


def create_net_sales():
    net = """
    SELECT branch_code,warehouse_name as "Branch name", month_year, "year", case when month like '%March%' then replace(month,'March','Mar')
        when month like '%Sept%' then replace(month,'Sept','Sep') else month end as month, new_mode_of_payment, amount
    FROM mabawa_dw.gross_payments
    where "year" >= 2020;
    """
    net = pd.read_sql_query(net,con = conn)
    net['net amount'] = net['amount']*(100/116)

    # get the current year and month
    current_year = datetime.datetime.now().year
    lastyear = current_year-1
    current_month = datetime.datetime.now().month-1
    previous_month_name = calendar.month_name[current_month].title()
    # create a list of month names for the current year and months up to and including the current month
    month_names = [datetime.date(lastyear, month_num, 1).strftime('%b') for month_num in range(1, current_month+1)]
    print(month_names)
    ##Get Last month
    today = datetime.date.today()
    last_month1 = datetime.date(today.year, today.month - 1, 1)
    last_month = last_month1.strftime('%b')
    ##Create a df with only the months we have covered in the current year
    net= net[net['month'].isin(month_names)]
    

    ##GET THE YTD NET SALES
    ytdnet = net.pivot_table(index = 'year',columns = 'new_mode_of_payment',aggfunc = {'net amount':np.sum})
    ytdnet = ytdnet.droplevel([0],axis = 1).reset_index()
    ytdnet["YTD Total"] = ytdnet["Cash"] + ytdnet["Insurance"]
    ##get the % changes 
    yoy_growth = ytdnet.pct_change(periods=1) * 100
    ytdnet = ytdnet.assign(YoY_GrowthCash=yoy_growth['Cash'],YoY_GrowthInsurance=yoy_growth['Insurance'],YoY_Growth = yoy_growth['YTD Total'])
    ytdnet = ytdnet.rename(columns={"YoY_GrowthCash":"% Cash Gain (Drop)","YoY_GrowthInsurance":"% Insurance Gain (Drop)","YoY_Growth":"% Overall Gain (Drop)"}).fillna(0)
    ytdnet = ytdnet.astype('int64')

    ##GET THE CURRENT MONTH SUMMARY
    currentmonth = net[net["month"] == last_month]
    currentmonthnet = currentmonth.pivot_table(index = 'year',columns = 'new_mode_of_payment',aggfunc = {'net amount':np.sum}) 
    print(currentmonthnet)
    currentmonthnet = currentmonthnet.droplevel([0],axis = 1).reset_index()
    currentmonthnet["Month Total"] = currentmonthnet["Cash"] + currentmonthnet["Insurance"]
    yoy_growth = currentmonthnet.pct_change(periods=1) * 100
    currentmonthnet = currentmonthnet.assign(YoY_GrowthCash=yoy_growth['Cash'],YoY_GrowthInsurance=yoy_growth['Insurance'],YoY_Growth = yoy_growth['Month Total'])
    currentmonthnet = currentmonthnet.rename(columns={"YoY_GrowthCash":"% Cash Gain (Drop)","YoY_GrowthInsurance":"% Insurance Gain (Drop)","YoY_Growth":"% Overall Gain (Drop)"}).fillna(0)
    currentmonthnet =currentmonthnet.astype('int64')

    ##COMPARING SHOPS OPENED AS FROM 2020
    net2020 = net[(net["year"] == 2020) & (net["month"] == 'Jan')]
    ##Branches opened since Jan 2020
    branches2020 = net2020["Branch name"].unique()   
    shopnetcomparison = net[net['Branch name'].isin(branches2020)]
    ################################
    shopnetcomparison_summary = shopnetcomparison.pivot_table(index = 'year',columns = 'new_mode_of_payment',aggfunc = {'net amount':np.sum})
    shopnetcomparison_summary = shopnetcomparison_summary.droplevel([0],axis = 1).reset_index()
    shopnetcomparison_summary["YTD Total"] = shopnetcomparison_summary["Cash"] + shopnetcomparison_summary["Insurance"]
    yoy_growth = shopnetcomparison_summary.pct_change(periods=1) * 100
    shopnetcomparison_summary = shopnetcomparison_summary.assign(YoY_GrowthCash=yoy_growth['Cash'],YoY_GrowthInsurance=yoy_growth['Insurance'],YoY_Growth = yoy_growth['YTD Total'])
    shopnetcomparison_summary = shopnetcomparison_summary.rename(columns={"YoY_GrowthCash":"% Cash Gain (Drop)","YoY_GrowthInsurance":"% Insurance Gain (Drop)","YoY_Growth":"% Overall Gain (Drop)"}).fillna(0)
    shopnetcomparison_summary = shopnetcomparison_summary.astype('int64')
    
    
    ##COMPARING SHOPS OPENED IN THE CURRENT MONTH SINCE 2020
    net2020_currentmnth = net[(net["year"] == 2020) & (net["month"] == last_month)]
    ##Branches opened since Jan 2020
    br2020_currentmnth = net2020_currentmnth["Branch name"].unique()
    shopnetcomp_samemnth = net[net['Branch name'].isin(br2020_currentmnth)]
    ##Define a df with only march data
    shopnetcomp_samemnth = shopnetcomp_samemnth[shopnetcomp_samemnth["month"] == last_month]
    ################################
    shopnetcomp_samemnth_summary = shopnetcomp_samemnth.pivot_table(index = 'year',columns = 'new_mode_of_payment',aggfunc = {'net amount':np.sum})
    shopnetcomp_samemnth_summary = shopnetcomp_samemnth_summary.droplevel([0],axis = 1).reset_index()
    shopnetcomp_samemnth_summary["Month Total"] = shopnetcomp_samemnth_summary["Cash"] + shopnetcomp_samemnth_summary["Insurance"]
    yoy_growth = shopnetcomp_samemnth_summary.pct_change(periods=1) * 100
    shopnetcomp_samemnth_summary = shopnetcomp_samemnth_summary.assign(YoY_GrowthCash=yoy_growth['Cash'],YoY_GrowthInsurance=yoy_growth['Insurance'],YoY_Growth = yoy_growth['Month Total'])
    shopnetcomp_samemnth_summary = shopnetcomp_samemnth_summary.rename(columns={"YoY_GrowthCash":"% Cash Gain (Drop)","YoY_GrowthInsurance":"% Insurance Gain (Drop)","YoY_Growth":"% Overall Gain (Drop)"}).fillna(0)
    shopnetcomp_samemnth_summary = shopnetcomp_samemnth_summary.astype('int64')
   
    

    ##GET SHOPS WHOSE YTD DROPPED BY 10% - SAVE THIS OUTPUT IN EXCEL
    year = ((datetime.datetime.now().year-1),(datetime.datetime.now().year))
    net_ytd= net[(net['month'].isin(month_names)) & (net['year'].isin(year))]
    ytd_net = net_ytd.pivot_table(index = ['Branch name'],columns = ['year'],aggfunc = {'net amount':np.sum})
    ytd_net = ytd_net.droplevel([0],axis = 1).reset_index().fillna(0)
    ytd_net["GAIN (DROP)"] = (ytd_net.iloc[:,2]-ytd_net.iloc[:,1])
    ytd_net["% GAIN (DROP)"] = ((((ytd_net.iloc[:,2]-ytd_net.iloc[:,1])/ytd_net.iloc[:,2]))*100).round()
    ytd_net["% GAIN (DROP)"]= ytd_net["% GAIN (DROP)"].replace(-np.inf,0)
    cols = ytd_net.select_dtypes(exclude=['object']).columns
    ytd_net[cols] = ytd_net[cols].astype('int64')
    
    ##
    ytd_netdrop = ytd_net[ytd_net['% GAIN (DROP)'] <= -10]
    if ytd_netdrop.empty:
        overallmessage = "No Branch dropped their YTD Net Amount by 10%"
    else:
        #PUTTING COMMA SEPERATION:get a dictionary of column names and their respective data types    
        ytd_netdrop_dict = ytd_netdrop.dtypes.to_dict()
        ytd_netdrop_dict = {col: "{:,d}" for col in ytd_netdrop_dict.keys() if ytd_netdrop_dict[col] == 'int64'}
        overallmessage = ytd_netdrop.style.hide_index().set_properties(**properties).set_table_styles(styles).format(ytd_netdrop_dict)
        overallmessage_html = overallmessage.to_html(doctype_html=True)  

    ##GET SHOPS WHOSE YTD Cash DROPPED BY 10% - SAVE THE OUTPUT IN EXCEL
    net_ytdcash= net[(net['month'].isin(month_names)) & (net['year'].isin(year)) & (net['new_mode_of_payment'] == 'Cash')]
    ytd_netcash = net_ytdcash.pivot_table(index = ['Branch name'],columns = ['year'],aggfunc = {'net amount':np.sum})
    ytd_netcash = ytd_netcash.droplevel([0],axis = 1).reset_index().fillna(0)
    ytd_netcash["GAIN (DROP)"] = (ytd_netcash.iloc[:,2]-ytd_netcash.iloc[:,1])
    ytd_netcash["% GAIN (DROP)"] = ((((ytd_netcash.iloc[:,2]-ytd_netcash.iloc[:,1])/ytd_netcash.iloc[:,2]))*100).round()
    ytd_netcash["% GAIN (DROP)"]= ytd_netcash["% GAIN (DROP)"].replace(-np.inf,0)
    cols1 = ytd_netcash.select_dtypes(exclude=['object']).columns
    ytd_netcash[cols1] = ytd_netcash[cols1].astype('int64')
    ##
    ytd_netcashdrop = ytd_netcash[ytd_netcash['% GAIN (DROP)'] <= -10]
    if ytd_netcashdrop.empty:
        cashmessage = "No Branch dropped their YTD net Cash by 10%"
    else:
        #PUTTING COMMA SEPERATION:get a dictionary of column names and their respective data types    
        ytd_netcashdrop_dict = ytd_netcashdrop.dtypes.to_dict()
        ytd_netcashdrop_dict = {col: "{:,d}" for col in ytd_netcashdrop_dict.keys() if ytd_netcashdrop_dict[col] == 'int64'}
        cashmessage = ytd_netcashdrop.style.hide_index().set_properties(**properties).set_table_styles(styles).format(ytd_netcashdrop_dict)
        cashmessage_html = cashmessage.to_html(doctype_html=True)        

    ##GET SHOPS WHOSE YTD Insurance DROPPED BY 10 - SAVE OUTPUT IN EXCEL
    net_ytdinsurance= net[(net['month'].isin(month_names)) & (net['year'].isin(year)) & (net['new_mode_of_payment'] == 'Insurance')]
    net_ytdinsurance = net_ytdinsurance.pivot_table(index = ['Branch name'],columns = ['year'],aggfunc = {'net amount':np.sum})
    net_ytdinsurance = net_ytdinsurance.droplevel([0],axis = 1).reset_index().fillna(0)
    net_ytdinsurance["GAIN (DROP)"] = (net_ytdinsurance.iloc[:,2]-net_ytdinsurance.iloc[:,1])
    net_ytdinsurance["% GAIN (DROP)"] = ((((net_ytdinsurance.iloc[:,2]-net_ytdinsurance.iloc[:,1])/net_ytdinsurance.iloc[:,2]))*100).round()
    net_ytdinsurance["% GAIN (DROP)"]= net_ytdinsurance["% GAIN (DROP)"].replace(-np.inf,0)
    cols2 = net_ytdinsurance.select_dtypes(exclude=['object']).columns
    net_ytdinsurance[cols2] = net_ytdinsurance[cols2].astype('int64')
    ##
    net_ytdinsurancedrop = net_ytdinsurance[net_ytdinsurance['% GAIN (DROP)'] <= -10]
    # net_ytdinsurancedrop = net_ytdinsurancedrop.round(0).astype('int64')    

    ##Create conditions to return the word or the df incase of a drop
    if net_ytdinsurancedrop.empty:
        insurancemessage = "No Branch dropped their YTD net Insurance by 10%"
    else:
        #PUTTING COMMA SEPERATION:get a dictionary of column names and their respective data types    
        net_ytdinsurancedrop_dict = net_ytdinsurancedrop.dtypes.to_dict()
        # create a dictionary that maps the column names to the desired formatting string
        net_ytdinsurancedrop_dict = {col: "{:,d}" for col in net_ytdinsurancedrop_dict.keys() if net_ytdinsurancedrop_dict[col] == 'int64'}
        insurancemessage = net_ytdinsurancedrop.style.hide_index().set_properties(**properties).set_table_styles(styles).format(net_ytdinsurancedrop_dict)
        insurancemessage_html = insurancemessage.to_html(doctype_html=True)                                                                               
    
    ytdnet['year'] = ytdnet['year'].astype(str)
    #----------------------------------------------------------------------------------------------------------------
    ###SAVE TO EXCEL
    #Save the data in an excel file
    import xlsxwriter
    print(xlsxwriter.__version__)

    #WRITE TO EXCEL
    with pd.ExcelWriter(r"/home/opticabi/Documents/optica_reports/Yearly Branch Cash and Ins Net Sales.xlsx", engine='xlsxwriter') as writer:
        # Write the dataframe to the Excel file
        
        ytdnet.to_excel(writer,sheet_name="Summary", index=False)
        ytd_net.to_excel(writer,sheet_name="CASH & INSURANCE Gain (Loss)", index=False)
        ytd_netcash.to_excel(writer,sheet_name="CASH Gain (Loss)", index=False)
        net_ytdinsurance.to_excel(writer,sheet_name="INSURANCE Gain (Loss)", index=False)

        # Get the workbook and worksheet objects
        workbook = writer.book        

        #---------------------------------------------------------------------------------------------------------------
        worksheet = writer.sheets['Summary']
        # Add borders to the cells
        border_format = workbook.add_format({'border': 1})
        worksheet.conditional_format('A1:BW247', {'type': 'no_errors', 'format': border_format})
        
        # Autofit the column widths
        worksheet.set_column(0, ytdnet.shape[1] - 1, None, None, {'autofit': True})    
        
        # Set the number format for the columns
        num_format = workbook.add_format({'num_format': '#,##0'})
        num_format.set_align('right')
        worksheet.set_column('A:BW', None, num_format)

        #-------------------------------------------------------------------------------------------------------------------
        worksheet1 = writer.sheets['CASH & INSURANCE Gain (Loss)']
        # Add borders to the cells
        border_format = workbook.add_format({'border': 1})
        worksheet1.conditional_format('A1:BW247', {'type': 'no_errors', 'format': border_format})
        
        # Autofit the column widths
        worksheet1.set_column(0, ytd_net.shape[1] - 1, None, None, {'autofit': True})    
        
        # Set the number format for the columns
        num_format = workbook.add_format({'num_format': '#,##0'})
        num_format.set_align('right')
        worksheet1.set_column('A:BW', None, num_format) 

        #-------------------------------------------------------------------------------------------------------------------
        worksheet2 = writer.sheets['CASH Gain (Loss)']
        # Add borders to the cells
        border_format = workbook.add_format({'border': 1})
        worksheet2.conditional_format('A1:BW247', {'type': 'no_errors', 'format': border_format})
        
        # Autofit the column widths
        worksheet2.set_column(0, ytd_netcash.shape[1] - 1, None, None, {'autofit': True})    
        
        # Set the number format for the columns
        num_format = workbook.add_format({'num_format': '#,##0'})
        num_format.set_align('right')
        worksheet2.set_column('A:BW', None, num_format) 

        #-------------------------------------------------------------------------------------------------------------------
        worksheet3 = writer.sheets['CASH Gain (Loss)']
        # Add borders to the cells
        border_format = workbook.add_format({'border': 1})
        worksheet3.conditional_format('A1:BW247', {'type': 'no_errors', 'format': border_format})
        
        # Autofit the column widths
        worksheet3.set_column(0, net_ytdinsurance.shape[1] - 1, None, None, {'autofit': True})    
        
        # Set the number format for the columns
        num_format = workbook.add_format({'num_format': '#,##0'})
        num_format.set_align('right')
        worksheet3.set_column('A:BW', None, num_format)

    #-------------------------------------------------------------------------------------------------------------
    #PUTTING COMMA SEPERATION
    # get a dictionary of column names and their respective data types
    ytdnet['year'] = ytdnet['year'].astype(str)
    ytdnet_dict = ytdnet.dtypes.to_dict()
    # create a dictionary that maps the column names to the desired formatting string
    ytdnet_dict = {col: "{:,d}" for col in ytdnet_dict.keys() if ytdnet_dict[col] == 'int64'}
    

    # get a dictionary of column names and their respective data types
    currentmonthnet['year'] = currentmonthnet['year'].astype(str)
    currentmonthnet_dict = currentmonthnet.dtypes.to_dict()
    # create a dictionary that maps the column names to the desired formatting string
    currentmonthnet_dict = {col: "{:,d}" for col in currentmonthnet_dict.keys() if currentmonthnet_dict[col] == 'int64'}

    # get a dictionary of column names and their respective data types
    shopnetcomparison_summary['year'] = shopnetcomparison_summary['year'].astype(str)
    shopnetcomparison_summary_dict = shopnetcomparison_summary.dtypes.to_dict()
    # create a dictionary that maps the column names to the desired formatting string
    shopnetcomparison_summary_dict = {col: "{:,d}" for col in shopnetcomparison_summary_dict.keys() if shopnetcomparison_summary_dict[col] == 'int64'}
    
    # get a dictionary of column names and their respective data types
    shopnetcomp_samemnth_summary['year'] = shopnetcomp_samemnth_summary['year'].astype(str)
    shopnetcomp_samemnth_summary_dict = shopnetcomp_samemnth_summary.dtypes.to_dict()
    # create a dictionary that maps the column names to the desired formatting string
    shopnetcomp_samemnth_summary_dict = {col: "{:,d}" for col in shopnetcomp_samemnth_summary_dict.keys() if shopnetcomp_samemnth_summary_dict[col] == 'int64'}
    

   #--------------------------------------------------------------------------------------------------------------
    ###Styling
    ytdnet = ytdnet.style.hide_index().set_properties(**properties).set_table_styles(styles).format(ytdnet_dict)
    ###Convert the dataframe to html
    ytdnet_html = ytdnet.to_html(doctype_html=True)
    
    ###Styling    
    currentmonthnet = currentmonthnet.style.hide_index().set_properties(**properties).set_table_styles(styles).format(currentmonthnet_dict)                                                                                     
    ###Convert the dataframe to html
    currentmonthnet_html = currentmonthnet.to_html(doctype_html=True)   

    ###Styling
    shopnetcomparison_summary = shopnetcomparison_summary.style.hide_index().set_properties(**properties).set_table_styles(styles).format(shopnetcomparison_summary_dict)                                                                                      
    ###Convert the dataframe to html
    shopnetcomparison_summary_html = shopnetcomparison_summary.to_html(doctype_html=True) 

    ###Styling
    shopnetcomp_samemnth_summary = shopnetcomp_samemnth_summary.style.hide_index().set_properties(**properties).set_table_styles(styles).format(shopnetcomp_samemnth_summary_dict)                                                                                       
    ###Convert the dataframe to html
    shopnetcomp_samemnth_summary_html = shopnetcomp_samemnth_summary.to_html(doctype_html=True) 

    #HTML
    ##Create the SMTP for the table above
    html = """
    <!DOCTYPE html>
    <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta http-equiv="X-UA-Compatible" content="IE=edge">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>MTD & Daily Net Sales</title>

            <style>
                table {{border-collapse: collapse;font-family:Comic Sans MS; font-size:9;}}
                th {{text-align: left;font-family:Comic Sans MS; font-size:9; padding: 6px;}}
                body, p, h3, div, span, var {{font-family:Comic Sans MS; font-size:11}}
                td {{text-align: left;font-family:Comic Sans MS; font-size:9; padding: 8px;}}
                h4 {{font-size: 12px; font-family: Comic Sans MS, sans-serif;}}
                ul {{list-style-type: none;}}
            

                .salutation {{
                    width: 20%;
                    margin: 0 auto;
                    text-align: left;
                }}
                
            </style>
        </head>

        <body>
            <div>
                <div class="inner-content">
                    <p><b>Dear All,</b></p>
                    <p>
                        This report shows the gross and net sales received since 2020 to date. We also have a month - month breakdown of the current year as shown below
                    </p>
                    <div>                       
                        <ul>
                            <h4>1. {previous_month_name} Net Sales </h4>
                            <li>                                
                                <table>{currentmonthnet_html}</table>
                            </li>
                            <h4>2. {previous_month_name} Net Sales Comparison for Shops opened during this period </h4>
                            <li>                                
                                <table>{shopnetcomp_samemnth_summary_html}</table>
                            </li>
                            <h4>3. YTD Net Sales </h4>
                            <li>
                                <table>{ytdnet_html}</table>
                            </li>                             
                            <h4>4. YTD Net Sales Comparison for Shops opened during this time </h4>
                            <li>                                
                                <table>{shopnetcomparison_summary_html}</table>
                            </li>                              
                            <h4>5. Shops whose YTD dropped by 10% and more </h4>
                            <li>                                
                                <table>{overallmessage_html}</table>
                            </li> 
                            <h4>6. Shops whose Cash YTD dropped by '10%' and more </h4>
                            <li>                                
                                <table>{cashmessage_html}</table>
                            </li> 
                            <h4>7. Shops whose Insurance YTD dropped by '10%' and more </h4>
                            <li>                                
                                <table>{insurancemessage_html}</table>
                            </li> 
                        <ul>     
                        
                        <ul>
                                                                 
                        </ul>
                    </div>                   
                </div><br />
                <hr style = "margin: 0 auto; color: #F8F8F8;"/>
            </div>
            <br>
            <div class = "salutation">
                <p><b>Kind Regards, <br> Data Team<b></p>
            </div>
        </body>
    </html>
        """.format(
            ytdnet_html=ytdnet_html,currentmonthnet_html=currentmonthnet_html,shopnetcomparison_summary_html=shopnetcomparison_summary_html,
            shopnetcomp_samemnth_summary_html=shopnetcomp_samemnth_summary_html,cashmessage_html=cashmessage_html,overallmessage_html=overallmessage_html,
            insurancemessage_html=insurancemessage_html,previous_month_name=previous_month_name
        )
    
    to_date = get_todate()
    sender_email = 'wairimu@optica.africa'    
    receiver_email = ['yuri@optica.africa','kush@optica.africa','wazeem@optica.africa','giri@optica.africa']
    # receiver_email = ['yuri@optica.africa','shyam@optica.africa']
    # receiver_email = ['wairimu@optica.africa']
    email_message = MIMEMultipart()
    email_message["From"] = sender_email
    email_message["To"] = r','.join(receiver_email)
    email_message["Subject"] = f"Net & Gross Sales, Year to Date {previous_month_name} {current_year}"
    email_message.attach(MIMEText(html, "html"))

    # Open the Excel file and attach it to the email
    with open('/home/opticabi/Documents/optica_reports/Gross Sales.xlsx', 'rb') as attachment:
        excel_file = MIMEApplication(attachment.read(), _subtype='xlsx')
        excel_file.add_header('Content-Disposition', 'attachment', filename='Gross Sales.xlsx')
        email_message.attach(excel_file)

    # Open the Excel file and attach it to the email
    with open('/home/opticabi/Documents/optica_reports/Net Sales.xlsx', 'rb') as attachment:
        excel_file = MIMEApplication(attachment.read(), _subtype='xlsx')
        excel_file.add_header('Content-Disposition', 'attachment', filename='Net Sales.xlsx')
        email_message.attach(excel_file)    

    # Open the Excel file and attach it to the email
    with open('/home/opticabi/Documents/optica_reports/Yearly Branch Cash and Ins Net Sales.xlsx', 'rb') as attachment:
        excel_file = MIMEApplication(attachment.read(), _subtype='xlsx')
        excel_file.add_header('Content-Disposition', 'attachment', filename='Yearly Branchwise Net Sales.xlsx')
        email_message.attach(excel_file)

    # Open the Excel file and attach it to the email
    with open('/home/opticabi/Documents/optica_reports/YTD Branch Cash and Ins Net Sales.xlsx', 'rb') as attachment:
        excel_file = MIMEApplication(attachment.read(), _subtype='xlsx')
        excel_file.add_header('Content-Disposition', 'attachment', filename='Branchwise Cash and Ins Net Sales.xlsx')
        email_message.attach(excel_file)    

    smtp_server = smtplib.SMTP("smtp.gmail.com", 587)
    smtp_server.starttls()
    smtp_server.login(sender_email, "maureen!!3636")
    text = email_message.as_string()
    smtp_server.sendmail(sender_email, receiver_email, text)
    smtp_server.quit()
if __name__ == '__main__': 
    create_net_sales()   


 







