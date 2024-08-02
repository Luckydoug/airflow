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

##Others
import os
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from sub_tasks.libraries.styles import styles, properties
from sub_tasks.libraries.utils import get_todate,send_report,assert_date_modified, create_initial_file, return_sent_emails, record_sent_branch
import smtplib

# PG Execute(Query)
from sub_tasks.data.connect import (pg_execute, engine) 
# from sub_tasks.api_login.api_login import(login)
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")

# SessionId = login()

def create_gross_payments():
    gross = """
    SELECT branch_code, month_year,"month","year",new_mode_of_payment, amount
    FROM mabawa_mviews.v_gross_payments;
    """
    gross = pd.read_sql_query(gross,con = conn)

    query = """truncate mabawa_dw.gross_payments;"""
    query = pg_execute(query) 
    print('truncate finished')
    gross.to_sql('gross_payments', con=engine, schema='mabawa_dw', if_exists = 'append', index=False)    

def summary_gross_payments():
    #####################Get the gross payments for the current year only
    grosspayments = """
    SELECT branch_code as "Branch", month_year,case when month like '%March%' then replace(month,'March','Mar')
        when month like '%Sept%' then replace(month,'Sept','Sep') else month end as month,year,
        new_mode_of_payment, amount
    FROM mabawa_dw.gross_payments
    where "year" = (SELECT EXTRACT(YEAR FROM CURRENT_DATE));
    """
    grosspayments = pd.read_sql_query(grosspayments,con=conn)
    
    ###Create a pivot table
    grosspayments_pivot = grosspayments.pivot_table(index = 'Branch',columns = ["month","new_mode_of_payment"],aggfunc = {"amount":np.sum})
    grosspayments_pivot = grosspayments_pivot.droplevel([0],axis = 1).reset_index()
    grosspayments_pivot = grosspayments_pivot.fillna(0)
    (grosspayments_pivot.iloc[:, grosspayments_pivot.columns != ('Branch',          '')]) = grosspayments_pivot.iloc[:, grosspayments_pivot.columns != ('Branch',          '')].astype("int64")
    
    ##Add the column Total
    for month in grosspayments_pivot.columns.levels[0][:-1]:
        col_name = (month, 'Total')
        result = (grosspayments_pivot[(month, 'Cash')] + grosspayments_pivot[(month, 'Insurance')])
        grosspayments_pivot.insert(grosspayments_pivot.columns.get_loc((month, 'Insurance')) + 1, col_name, result)
    
    
    
    ####################Get the gross payments since 2019
    grosspaymentsall = """
    SELECT branch_code, month_year,case when month like '%March%' then replace(month,'March','Mar')
        when month like '%Sept%' then replace(month,'Sept','Sep') else month end as month,year, 
        new_mode_of_payment, amount
    FROM mabawa_dw.gross_payments;
    """
    grosspaymentsall = pd.read_sql_query(grosspaymentsall,con=conn)
    
    ###Create a pivot table
    grosspaymentsall_pivot = grosspaymentsall.pivot_table(index = 'year',columns = ["month"],aggfunc = {"amount":np.sum})
    grosspaymentsall_pivot = grosspaymentsall_pivot.droplevel([0],axis = 1).reset_index()
    grosspaymentsall_pivot = grosspaymentsall_pivot.fillna(0)


    # get the current year and month
    current_year = datetime.datetime.now().year
    lastyear = current_year-1
    current_month = datetime.datetime.now().month
    # create a list of month names for the current year and months up to and including the current month
    month_names = [datetime.date(lastyear, month_num, 1).strftime('%b') for month_num in range(1, current_month+1)]
    ##add a column to your list
    extra_column = 'year'
    month_names.append(extra_column)
    ##Sum all the months we have covered so far
    grosspaymentsall_pivotnew = grosspaymentsall_pivot[month_names]
    grosspaymentsall_pivotnew['year'] = grosspaymentsall_pivotnew['year'].astype('object').astype('object')
    grosspaymentsall_pivotnew["Year to Date"] = grosspaymentsall_pivotnew.iloc[:, grosspaymentsall_pivotnew.columns != 'year'].sum(axis=1)
    ##merge with the original table
    grosspaymentsall_pivot = pd.merge(grosspaymentsall_pivot,grosspaymentsall_pivotnew[['year','Year to Date']],on ='year',how = 'left')
    ##Calculate the Year on Year Growth
    yoy_growth = grosspaymentsall_pivot.pct_change(periods=1) * 100
    grosspaymentsall_pivot = grosspaymentsall_pivot.assign(YoY_Growth=yoy_growth['Year to Date'])
    grosspaymentsall_pivot = grosspaymentsall_pivot.rename(columns={"YoY_Growth":"YTD Growth"}).fillna(0)
    grosspaymentsall_pivot['YTD Growth'] = grosspaymentsall_pivot['YTD Growth'].replace(np.inf,0)
    grosspaymentsall_pivot["year"] = grosspaymentsall_pivot["year"].astype('object')
    grosspaymentsall_pivot = grosspaymentsall_pivot.rename(columns = {"year":"Year"})
    grosspaymentsall_pivot = grosspaymentsall_pivot[["Year","Jan","Feb","Mar","Apr","May","June","July","Aug","Sep","Oct","Nov","Dec","Year to Date","YTD Growth"]]
    grosspaymentsall_pivot[["Jan","Feb","Mar","Apr","May","June","July","Aug","Sep","Oct","Nov","Dec","Year to Date","YTD Growth"]] = grosspaymentsall_pivot[["Jan","Feb","Mar","Apr","May","June","July","Aug","Sep","Oct","Nov","Dec","Year to Date","YTD Growth"]].round(0).astype("int64")
    print(grosspaymentsall_pivot.dtypes)


    ###########################################SMTP##################################################
    # get a dictionary of column names and their respective data types
    dtype_dict = grosspaymentsall_pivot.dtypes.to_dict()
    # create a dictionary that maps the column names to the desired formatting string
    format_dict = {col: "{:,d}" for col in dtype_dict.keys() if dtype_dict[col] == 'int64'}
    print(format_dict)
    # get a dictionary of column names and their respective data types
    dtype_dict1 = grosspayments_pivot.dtypes.to_dict()
    # create a dictionary that maps the column names to the desired formatting string
    format_dict1 = {col: "{:,d}" for col in dtype_dict1.keys() if dtype_dict1[col] == 'int64'}
    print(format_dict1)

    ###Styling
    grosspayments_pivot = grosspayments_pivot.style.hide_index().set_properties(**properties).set_table_styles(styles).format(format_dict1)
    ###Convert the dataframe to html
    grosspayments_pivot_html = grosspayments_pivot.to_html(doctype_html=True)
    
    ###Styling
    grosspaymentsall_pivot = grosspaymentsall_pivot.style.hide_index().set_properties(**properties).set_table_styles(styles).format(format_dict)                                                                                          
    ###Convert the dataframe to html
    grosspaymentsall_pivot_html = grosspaymentsall_pivot.to_html(doctype_html=True)   


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
                        This report shows the gross sales received since 2019 to date. We also have a month - month breakdown of the current year as shown below
                    </p>
                    <div>                       
                        <ul>
                         <h4>1. Annual Gross Sales </h4>
                            <li>
                                <table>{grosspaymentsall_pivot_html}</table>
                            </li>

                            <h4>2. Branch Monthly Gross Sales </h4>
                            <li>                                
                                <table>{grosspayments_pivot_html}</table>
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
            grosspaymentsall_pivot_html=grosspaymentsall_pivot_html,grosspayments_pivot_html=grosspayments_pivot_html
        )
    
    to_date = get_todate()
    sender_email = os.getenv("wairimu_email")
    # receiver_email = ['yuri@optica.africa','kush@optica.africa','wazeem@optica.africa']
    receiver_email = ['wairimu@optica.africa']
    email_message = MIMEMultipart()
    email_message["From"] = sender_email
    email_message["To"] = r','.join(receiver_email)
    email_message["Subject"] = f"Gross Sales per Year by Branch"
    email_message.attach(MIMEText(html, "html"))
    smtp_server = smtplib.SMTP("smtp.gmail.com", 587)
    smtp_server.starttls()
    smtp_server.login(sender_email,os.getenv("wairimu_password"))
    text = email_message.as_string()
    smtp_server.sendmail(sender_email, receiver_email, text)
    smtp_server.quit()
if __name__ == '__main__': 
    summary_gross_payments()  
    


 



