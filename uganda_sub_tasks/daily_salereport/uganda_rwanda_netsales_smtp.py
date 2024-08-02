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
from sub_tasks.libraries.utils import (clean_folder_uganda,clean_folder_rwanda,get_todate,send_report,assert_date_modified, 
                                       create_initial_file, return_sent_emails,record_sent_branch)

# PG Execute(Query)
from sub_tasks.data.connect_mawingu import (pg_execute, pg_fetch_all, engine)  
# from sub_tasks.api_login.api_login import(login_uganda)
conn = psycopg2.connect(host="10.40.16.19",database="mawingu", user="postgres", password="@Akb@rp@$$w0rtf31n")


def daily_netsales_email_uganda_rwanda():

    netsales =pd.read_excel(r"/home/opticabi/Documents/uganda_reports/Final Summary Net Sales.xlsx",sheet_name = 'daily_monthly')    
    ##Select Columns
    netsales[" "] = range(1, len(netsales) + 1)
    netsales = netsales[[" ","Branch","MTD_Total Cash","MTD_Insurance","MTD_Total Cash Net Amount","MTD_Total Insurance Net Amount",
                         "Total Cash","Insurance","Total Cash Net Amount","Total Insurance Net Amount"]]    
    
    netsales = netsales.fillna(0)
    newnetsales = netsales.copy()

    netsales[["MTD_Total Cash","MTD_Insurance","MTD_Total Cash Net Amount","MTD_Total Insurance Net Amount",
                         "Total Cash","Insurance","Total Cash Net Amount","Total Insurance Net Amount"]] = netsales[["MTD_Total Cash","MTD_Insurance","MTD_Total Cash Net Amount","MTD_Total Insurance Net Amount",
                         "Total Cash","Insurance","Total Cash Net Amount","Total Insurance Net Amount"]].round(0).astype(int)
    netsales = netsales.rename(columns = {"MTD_Total Cash Net Amount":"MTD Cash","MTD_Total Insurance Net Amount":"MTD Insurance",
                                    "Total Cash Net Amount":"Day Cash","Insurance":"Insurance1","Total Insurance Net Amount":"Day Insurance"})
    print('Columns renamed')
    print(netsales.columns)
    netsales['MTD Net Sales'] = netsales['MTD Cash'] + netsales['MTD Insurance']
    netsales['Day Net Sales'] = netsales['Day Cash'] + netsales['Day Insurance']
    print('Summation completed')
    netsales = netsales[["Branch","MTD Cash","MTD Insurance","MTD Net Sales","Day Cash","Day Insurance","Day Net Sales"]]
    netsales[["MTD Cash","MTD Insurance","MTD Net Sales","Day Cash","Day Insurance","Day Net Sales"]] = netsales[["MTD Cash","MTD Insurance","MTD Net Sales","Day Cash","Day Insurance","Day Net Sales"]].astype(int)
    print(netsales)


    #-------------------------------------------------------------------------------------------------------------------------------------------------
    ##Create Total    
    newnetsales.loc['Total'] = newnetsales[["MTD_Total Cash","MTD_Insurance","MTD_Total Cash Net Amount","MTD_Total Insurance Net Amount",
                         "Total Cash","Insurance","Total Cash Net Amount","Total Insurance Net Amount"]].sum()
    newnetsales["Branch"] = newnetsales["Branch"].fillna('Total')
    total = newnetsales[newnetsales['Branch'] == 'Total']
    total[" "] = range(1, len(total) + 1)
    total = total.fillna(0)
    total[["MTD_Total Cash","MTD_Insurance","MTD_Total Cash Net Amount","MTD_Total Insurance Net AmountMTD_Total Insurance Net Amount",
                         "Total Cash","Insurance","Total Cash Net Amount","Total Insurance Net Amount"]] = total[["MTD_Total Cash","MTD_Insurance","MTD_Total Cash Net Amount","MTD_Total Insurance Net Amount",
                         "Total Cash","Insurance","Total Cash Net Amount","Total Insurance Net Amount"]].round(0).astype(int)
    total = total.rename(columns = {"MTD_Total Cash Net Amount":"MTD Cash","MTD_Total Insurance Net Amount":"MTD Insurance",
                                    "Total Cash Net Amount":"Day Cash","Insurance":"Insurance1","Total Insurance Net Amount":"Day Insurance"})
    print('total summation')
    total['MTD Net Sales'] = total['MTD Cash'] + total['MTD Insurance']
    total['Day Net Sales'] = total['Day Cash'] + total['Day Insurance']
    total = total[["Branch","MTD Cash","MTD Insurance","MTD Net Sales","Day Cash","Day Insurance","Day Net Sales"]]
    total[["MTD Cash","MTD Insurance","MTD Net Sales","Day Cash","Day Insurance","Day Net Sales"]] = total[["MTD Cash","MTD Insurance","MTD Net Sales","Day Cash","Day Insurance","Day Net Sales"]].astype(int)
    ##################################################################################################   
    ###Styling
    netsales = netsales.style.hide_index().set_properties(**properties).set_table_styles(styles).format({"MTD Cash": "{:,d}",
                                                                                           "MTD Insurance":"{:,d}",
                                                                                           "MTD Net Sales":"{:,d}",
                                                                                           "Day Cash":"{:,d}",
                                                                                           "Day Insurance":"{:,d}",
                                                                                           "Day Net Sales":"{:,d}"}
                                                                                           )
    ###Convert the dataframe to html
    netsales_html = netsales.to_html(doctype_html=True)

    total = total.style.hide_index().set_table_styles(styles).set_properties(**properties).format({"MTD Cash": "{:,d}",
                                                                                           "MTD Insurance":"{:,d}",
                                                                                           "MTD Net Sales":"{:,d}",
                                                                                           "Day Cash":"{:,d}",
                                                                                           "Day Insurance":"{:,d}",
                                                                                           "Day Net Sales":"{:,d}"}
                                                                                           )
    
    ###Convert the dataframe to html
    total_html = total.to_html(doctype_html=True)    
    
    ###############################################################################################
                        #RWANDA


    netsales_rwanda =pd.read_excel(r"/home/opticabi/Documents/rwanda_reports/Final Summary Net Sales.xlsx",sheet_name = 'daily_monthly')    
    ##Select Columns
    netsales_rwanda[" "] = range(1, len(netsales_rwanda) + 1)
    netsales_rwanda = netsales_rwanda[[" ","Branch","MTD_Total Cash","MTD_Insurance","MTD_Total Cash Net Amount","MTD_Total Insurance Net Amount",
                         "Total Cash","Insurance","Total Cash Net Amount","Total Insurance Net Amount"]]    
    
    netsales_rwanda = netsales_rwanda.fillna(0)

    newnetsales_rwanda = netsales_rwanda.copy()

    netsales_rwanda[["MTD_Total Cash","MTD_Insurance","MTD_Total Cash Net Amount","MTD_Total Insurance Net Amount",
                         "Total Cash","Insurance","Total Cash Net Amount","Total Insurance Net Amount"]] = netsales_rwanda[["MTD_Total Cash","MTD_Insurance","MTD_Total Cash Net Amount","MTD_Total Insurance Net Amount",
                         "Total Cash","Insurance","Total Cash Net Amount","Total Insurance Net Amount"]].round(0).astype(int)
    netsales_rwanda = netsales_rwanda.rename(columns = {"MTD_Total Cash Net Amount":"MTD Cash","MTD_Total Insurance Net Amount":"MTD Insurance",
                                    "Total Cash Net Amount":"Day Cash","Insurance":"Insurance1","Total Insurance Net Amount":"Day Insurance"})
    print('Columns renamed')
    print(netsales_rwanda.columns)
    netsales_rwanda['MTD Net Sales'] = netsales_rwanda['MTD Cash'] + netsales_rwanda['MTD Insurance']
    netsales_rwanda['Day Net Sales'] = netsales_rwanda['Day Cash'] + netsales_rwanda['Day Insurance']
    print('Summation completed')
    netsales_rwanda = netsales_rwanda[["Branch","MTD Cash","MTD Insurance","MTD Net Sales","Day Cash","Day Insurance","Day Net Sales"]]
    netsales_rwanda[["MTD Cash","MTD Insurance","MTD Net Sales","Day Cash","Day Insurance","Day Net Sales"]] = netsales_rwanda[["MTD Cash","MTD Insurance","MTD Net Sales","Day Cash","Day Insurance","Day Net Sales"]].astype(int)
    print(netsales_rwanda)


    #-------------------------------------------------------------------------------------------------------------------------------------------------
    ##Create Total    
    newnetsales_rwanda.loc['Total'] = newnetsales_rwanda[["MTD_Total Cash","MTD_Insurance","MTD_Total Cash Net Amount","MTD_Total Insurance Net Amount",
                         "Total Cash","Insurance","Total Cash Net Amount","Total Insurance Net Amount"]].sum()
    newnetsales_rwanda["Branch"] = newnetsales_rwanda["Branch"].fillna('Total')
    total_rwanda = newnetsales_rwanda[newnetsales_rwanda['Branch'] == 'Total']
    total_rwanda[" "] = range(1, len(total_rwanda) + 1)
    total_rwanda = total_rwanda.fillna(0)
    total_rwanda[["MTD_Total Cash","MTD_Insurance","MTD_Total Cash Net Amount","MTD_Total Insurance Net AmountMTD_Total Insurance Net Amount",
                         "Total Cash","Insurance","Total Cash Net Amount","Total Insurance Net Amount"]] = total_rwanda[["MTD_Total Cash","MTD_Insurance","MTD_Total Cash Net Amount","MTD_Total Insurance Net Amount",
                         "Total Cash","Insurance","Total Cash Net Amount","Total Insurance Net Amount"]].round(0).astype(int)
    total_rwanda = total_rwanda.rename(columns = {"MTD_Total Cash Net Amount":"MTD Cash","MTD_Total Insurance Net Amount":"MTD Insurance",
                                    "Total Cash Net Amount":"Day Cash","Insurance":"Insurance1","Total Insurance Net Amount":"Day Insurance"})
    print('total summation')
    total_rwanda['MTD Net Sales'] = total_rwanda['MTD Cash'] + total_rwanda['MTD Insurance']
    total_rwanda['Day Net Sales'] = total_rwanda['Day Cash'] + total_rwanda['Day Insurance']
    total_rwanda = total_rwanda[["Branch","MTD Cash","MTD Insurance","MTD Net Sales","Day Cash","Day Insurance","Day Net Sales"]]
    total_rwanda[["MTD Cash","MTD Insurance","MTD Net Sales","Day Cash","Day Insurance","Day Net Sales"]] = total_rwanda[["MTD Cash","MTD Insurance","MTD Net Sales","Day Cash","Day Insurance","Day Net Sales"]].astype(int)
    ##################################################################################################   
    # group1_styles = [{'selector': f'th.col{i}', 'props': [('background-color', 'red')]} for i in range(1, 6)] 
    ###Styling
    print(netsales_rwanda.columns)
    print(total_rwanda.columns)
    netsales_rwanda = netsales_rwanda.style.hide_index().set_properties(**properties).set_table_styles(styles).format({"MTD Cash": "{:,d}",
                                                                                           "MTD Insurance":"{:,d}",
                                                                                           "MTD Net Sales":"{:,d}",
                                                                                           "Day Cash":"{:,d}",
                                                                                           "Day Insurance":"{:,d}",
                                                                                           "Day Net Sales":"{:,d}"}
                                                                                           )
    ###Convert the dataframe to html
    netsales_html_rwanda = netsales_rwanda.to_html(doctype_html=True)

    total_rwanda = total_rwanda.style.hide_index().set_table_styles(styles).set_properties(**properties).format({"MTD Cash": "{:,d}",
                                                                                           "MTD Insurance":"{:,d}",
                                                                                           "MTD Net Sales":"{:,d}",
                                                                                           "Day Cash":"{:,d}",
                                                                                           "Day Insurance":"{:,d}",
                                                                                           "Day Net Sales":"{:,d}"}
                                                                                           )
    
    ###Convert the dataframe to html
    total_html_rwanda = total_rwanda.to_html(doctype_html=True)    
    
    ###############################################################################################    

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
                        This report shows the MTD and Daily Sales made across all Branches. Kindly find the attached file with the daywise sales.
                    </p>
                    <div> <b> UGANDA (UGX) </b>                      
                        <ul>
                         <h4>1. Total Branch Sales </h4>
                            <li>
                                <table>{total_html}</table>
                            </li>

                            <h4>2. MTD & Daily Sales </h4>
                            <li>                                
                                <table>{netsales_html}</table>
                            </li> 
                        <ul>   
                        <ul>                                                                 
                        </ul>
                    </div>
                    <div> <b> RWANDA (RWF)</b>                      
                        <ul>
                         <h4>1. Total Branch Sales </h4>
                            <li>
                                <table>{total_html_rwanda}</table>
                            </li>

                            <h4>2. MTD & Daily Sales </h4>
                            <li>                                
                                <table>{netsales_html_rwanda}</table>
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
            netsales_html=netsales_html,total_html=total_html,total_html_rwanda = total_html_rwanda, netsales_html_rwanda = netsales_html_rwanda
        )

    to_date = get_todate()
    # to_date = '2024-07-31'
  
    sender_email = os.getenv("wairimu_email")
    # receiver_email = ['wairimu@optica.africa']
    receiver_email = ['wairimu@optica.africa','yuri@optica.africa','kush@optica.africa','raghav@optica.africa']
    email_message = MIMEMultipart()
    email_message["From"] = sender_email
    email_message["To"] = r','.join(receiver_email)
    email_message["Subject"] = f"Uganda & Rwanda MTD & Daily Net Sales Report for {to_date}"
    email_message.attach(MIMEText(html, "html"))

    # Open the Excel file and attach it to the email
    with open('/home/opticabi/Documents/uganda_reports/Uganda BranchWise Net Sales Report.xlsx', 'rb') as attachment:
        excel_file = MIMEApplication(attachment.read(), _subtype='xlsx')
        excel_file.add_header('Content-Disposition', 'attachment', filename='Uganda BranchWise Net Sales Report.xlsx')
        email_message.attach(excel_file)

    # Open the Excel file and attach it to the email
    with open('/home/opticabi/Documents/rwanda_reports/Rwanda BranchWise Net Sales Report.xlsx', 'rb') as attachment:
        excel_file = MIMEApplication(attachment.read(), _subtype='xlsx')
        excel_file.add_header('Content-Disposition', 'attachment', filename='Rwanda BranchWise Net Sales Report.xlsx')
        email_message.attach(excel_file)

    smtp_server = smtplib.SMTP("smtp.gmail.com", 587)
    smtp_server.starttls()
    smtp_server.login(sender_email, os.getenv("wairimu_password"))
    text = email_message.as_string()
    smtp_server.sendmail(sender_email, receiver_email, text)
    smtp_server.quit()
if __name__ == '__main__':
    daily_netsales_email_uganda_rwanda()  
    clean_folder_rwanda()
    clean_folder_uganda()    