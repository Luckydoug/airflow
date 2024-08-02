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
import time

##Others
import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from sub_tasks.libraries.styles import styles, properties

# PG Execute(Query)
from sub_tasks.data.connect_mawingu import (pg_execute, pg_fetch_all, engine)  
# from sub_tasks.api_login.api_login import(login_uganda)
from sub_tasks.libraries.utils import (get_rm_srm_total, get_rm_srm_total_multiindex, get_comparison_months,check_date_range,
arrange_dateranges,get_todate,fourth_week_start,fourth_week_end)

conn = psycopg2.connect(host="10.40.16.19",database="mawingu", user="postgres", password="@Akb@rp@$$w0rtf31n")


def google_reviews_automation():
    googlereviews = """
    SELECT location_id, location_name, store_code as "Branch", review_id, reviewer, star_rating, review_comment, createdat, updatedat, 
    review_reply, reply_time FROM mawingu_staging.google_reviews;
    """
    googlereviews = pd.read_sql_query(googlereviews, con=conn) 
    ##
    googlereviews['updatedat'] = pd.to_datetime(googlereviews['updatedat'])

    """
    Branchwise Summary - Google Reviews
    """
    googlereviews["week range"] = googlereviews.apply(lambda row: check_date_range(row, "updatedat"), axis=1)
    googlereviews = googlereviews[googlereviews["week range"] != 'None']

    #####
    googlereviews["star_rating"]=np.where(googlereviews["star_rating"]=="FIVE",5,np.where(googlereviews["star_rating"]=="FOUR",4,
                                                                                        np.where(googlereviews["star_rating"]=="THREE",3,
                                                                                                    np.where(googlereviews["star_rating"]=="TWO",2,1)))) 

    # googlereviews["star_rating"] = pd.to_numeric(googlereviews["star_rating"])

    googlereviews["Category"]=np.where(googlereviews.star_rating<=3,"Poor",
                                    np.where(googlereviews.star_rating<=4,"Average","Great"))
    
    googlereviews["Poor Reviews"] = googlereviews.apply(lambda row: 1 if row['star_rating'] <= 3 else np.nan, axis = 1)


    
    googlereviews_pivot = googlereviews.pivot_table(index = 'Branch',columns = ['week range'],
                                    values =  ["location_id","star_rating","Poor Reviews"],
                                    aggfunc={
                                         "location_id":["count"] , "star_rating":["mean"],"Poor Reviews":["count"]
                                    }
                                   )
    
    googlereviews_pivot = googlereviews_pivot.droplevel([1],axis = 1)
    googlereviews_pivot = googlereviews_pivot.swaplevel(0,1,axis = 1)
    cols = arrange_dateranges(googlereviews_pivot)

    googlereviews_pivot = googlereviews_pivot.reindex(cols, level = 0, axis = 1).rename(
    columns={"Poor Reviews":"Poor Reviews","location_id": "Total Reviews","star_rating":"Average Rating (Target = 4.9)"}, level=1)  

    for column in googlereviews_pivot.columns:        
        if googlereviews_pivot[column].dtype in ['int64', 'float64']:            
            googlereviews_pivot[column] = googlereviews_pivot[column].round(2)
            googlereviews_pivot[column] = googlereviews_pivot[column].astype(str)

    googlereviews_pivot = googlereviews_pivot.reset_index()
    

    """
    Overall Summary - Google Reviews
    """

    googlereviews["Country"] = "Uganda"
    googlereviews_pivot_br = googlereviews.pivot_table(index = "Country",columns = ['week range'],
                                        values = ["location_id","star_rating","Poor Reviews"],
                                        aggfunc={
                                            "location_id":["count"] , "star_rating":["mean"],"Poor Reviews":["count"]
                                        }
                                    )
    googlereviews_pivot_br = googlereviews_pivot_br.droplevel([1],axis = 1)
    googlereviews_pivot_br = googlereviews_pivot_br.swaplevel(0,1,axis = 1)

    cols = arrange_dateranges(googlereviews_pivot_br)

    googlereviews_pivot_br = googlereviews_pivot_br.reindex(cols, level = 0, axis = 1).rename(
        columns={"Poor Reviews":"Poor Reviews","location_id": "Total Reviews","star_rating":"Average Rating (Target = 4.9)"}, level=1)
    
    for column in googlereviews_pivot_br.columns:        
        if googlereviews_pivot_br[column].dtype in ['int64', 'float64']:            
            googlereviews_pivot_br[column] = googlereviews_pivot_br[column].round(2)
            googlereviews_pivot_br[column] = googlereviews_pivot_br[column].astype(str)

    googlereviews_pivot_br = googlereviews_pivot_br.reset_index()
    #______________________________________________________________________________________________________________________________________________
    ###Styling
    googlereviews_pivot_br = googlereviews_pivot_br.style.hide_index().set_properties(**properties).set_table_styles(styles)

    ###Convert the dataframe to html
    googlereviews_pivot_br_html = googlereviews_pivot_br.to_html(doctype_html=True)

    googlereviews_pivot = googlereviews_pivot.style.hide_index().set_table_styles(styles).set_properties(**properties)
    
    ###Convert the dataframe to html
    googlereviews_pivot_html = googlereviews_pivot.to_html(doctype_html=True) 


    """
    Poor Reviews Attachment
    """
    poor_reviews = googlereviews[googlereviews['star_rating'] <= 3]
    with pd.ExcelWriter(r"/home/opticabi/Documents/uganda_reports/Uganda Poor Reviews.xlsx", engine='xlsxwriter') as writer:    
        poor_reviews.to_excel(writer,sheet_name='Poor Reviews', index=False)        
    
#________________________________________________________________________________________________________________________________________________

    ###### SMTP
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
                            This report shows a summary of all the google reviews received. Kindly find the attached file to review poor google reviews
                        </p>
                        <div> <b> Key </b> <br>
                            <i>  Poor Reviews - Google rating from 0 to 3 </i> <br>
                            <i>  Average Reviews - Google rating of 4 </i> <br>   
                            <i>  Great Reviews - Google rating of 5 </i>                       
                            <ul>
                            <h4>1. Weekly Overall Comparison </h4>
                                <li>
                                    <table>{googlereviews_pivot_br_html}</table>
                                </li>

                                <h4>2. Branch Wise Weekly Comparison </h4>
                                <li>                                
                                    <table>{googlereviews_pivot_html}</table>
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
                googlereviews_pivot_br_html = googlereviews_pivot_br_html,googlereviews_pivot_html = googlereviews_pivot_html
            )

    to_date = get_todate()
    sender_email = os.getenv("wairimu_email")
    # receiver_email = ['wairimu@optica.africa','douglas.kathurima@optica.africa']
    receiver_email = ['wairimu@optica.africa','yuri@optica.africa','raghav@optica.africa','douglas.kathurima@optica.africa','larry.larsen@optica.africa']
    email_message = MIMEMultipart()
    email_message["From"] = sender_email
    email_message["To"] = r','.join(receiver_email)
    email_message["Subject"] = f"Uganda Google Ratings Summary from {fourth_week_start} to {fourth_week_end}"
    email_message.attach(MIMEText(html, "html"))

    # Open the Excel file and attach it to the email
    with open('/home/opticabi/Documents/uganda_reports/Uganda Poor Reviews.xlsx', 'rb') as attachment:
        excel_file = MIMEApplication(attachment.read(), _subtype='xlsx')
        excel_file.add_header('Content-Disposition', 'attachment', filename='Uganda Poor Google Reviews.xlsx')
        email_message.attach(excel_file)

    smtp_server = smtplib.SMTP("smtp.gmail.com", 587)
    smtp_server.starttls()
    smtp_server.login(sender_email, os.getenv("wairimu_password"))
    text = email_message.as_string()
    smtp_server.sendmail(sender_email, receiver_email, text)
    smtp_server.quit()
if __name__ == '__main__':
    google_reviews_automation()    

        







