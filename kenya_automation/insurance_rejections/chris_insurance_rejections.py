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
import dotenv
import os

##Others
import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from sub_tasks.libraries.styles import styles, properties
from sub_tasks.libraries.utils import get_todate,send_report,assert_date_modified, create_initial_file, return_sent_emails, record_sent_branch, fetch_gsheet_data, fourth_week_start, fourth_week_end

from reports.draft_to_upload.utils.utils import return_report_daterange
from reports.draft_to_upload.utils.utils import get_report_frequency

# PG Execute(Query)
from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.api_login.api_login import(login)
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")


selection = get_report_frequency()
print(selection)
if selection == 'Daily':
    start_date = return_report_daterange(selection)
    end_date = start_date
elif selection == 'Weekly':
    start_date = fourth_week_start
    end_date = fourth_week_end
start_date = return_report_daterange(selection)

print(datetime.datetime.today())
print(end_date)

# start_date = '2023-08-01'
# end_date = '2023-08-31'

def rejections():
    branch_data = fetch_gsheet_data()["branch_data"]
    orderscrnc1 = f"""
        with rej as (
            SELECT os.doc_entry,
        os.odsc_date, 
        os.odsc_time, 
        os.odsc_status, 
        os.odsc_createdby,
        os.odsc_remarks,
        s.ods_createdon as "Order Date",
        s.doc_no as "Order Number",
        s.ods_status,
        s.ods_outlet as "Branch",
        s.ods_creator,
        su.user_name,
        soh.draft_orderno
    FROM mabawa_staging.source_orderscreenc1 os
    left join mabawa_staging.source_orderscreen s on s.doc_entry = os.doc_entry
    left join mabawa_staging.source_users su on su.user_signature = s."user"    
    left join mabawa_staging.source_orders_header soh on soh.draft_orderno::text = s.doc_no::text
    where odsc_date::date BETWEEN '{start_date}' and '{end_date}'
    and os.odsc_status in ('Rejected by Approvals Team','Rejected by Optica Insurance'))
    select 
    	rej.doc_entry,
        rej.odsc_date, 
        rej.odsc_time, 
        rej.odsc_status, 
        rej.odsc_createdby,
        rej.odsc_remarks,
        suu.user_name,
        rej."Order Date",
        rej."Order Number",
        rej.ods_status,
        rej."Branch",
        rej.ods_creator,
        rej.user_name,
        rej.draft_orderno
        from rej 
    left join mabawa_staging.source_users suu on suu.user_code = rej.odsc_createdby
    """
    ##################################
    orderlog = pd.read_sql_query(orderscrnc1,con=conn).sort_values('odsc_date')       
    
    srm_rm = branch_data
    srm_rm_new = srm_rm
    
    srm_rm_new ['Rejected Orders'] = 0
    srm_rm_new = srm_rm_new[['Outlet','RM','SRM']]    
    
    orderlog = pd.merge(orderlog,srm_rm[['Outlet','RM','SRM']],left_on = 'Branch',right_on = 'Outlet',how = 'left')
    
    orderlog_pivot = orderlog.pivot_table(index = ['Outlet','RM','SRM'],aggfunc = {'doc_entry':np.count_nonzero,'draft_orderno':np.count_nonzero}).reset_index().rename(columns = {'doc_entry':'Rejected Orders'}).drop_duplicates(subset = 'Outlet', keep = 'first').fillna(0)

    rejection = pd.merge(orderlog_pivot,srm_rm_new,on = ['Outlet','RM','SRM'],how = 'right').sort_values(by = 'Rejected Orders',ascending = True).rename(columns = {'draft_orderno':'Converted Orders'}).fillna(0)
    print(rejection)



    #-----------------------------------Total orders-----------------------------------------
    query = f"""
    SELECT doc_entry, doc_no,"ods_creator", ods_createdon, ods_createdat,ods_status,
    ods_outlet,ods_insurance_order,ods_ordercriteriastatus,"user"
    FROM mabawa_staging.source_orderscreen
    WHERE ods_createdon::date BETWEEN '{start_date}' and '{end_date}'
    and ods_insurance_order = 'Yes'
    and ods_status <> 'Cancel'
    """
    ############################################
    orders = pd.read_sql_query(query,con=conn).sort_values('ods_createdon')
    
    orders_pivot = orders.pivot_table(index = 'ods_outlet',aggfunc = {'doc_no':np.count_nonzero}).reset_index().rename(columns = {'doc_no':'Total Ins Orders','ods_outlet':'Outlet'})
    
    branch_rejections = pd.merge(rejection,orders_pivot,on = 'Outlet',how = 'left')
    branch_rejections['% Rejected Orders'] = (branch_rejections['Rejected Orders']/branch_rejections['Total Ins Orders'])*100
    branch_rejections['% Converted Orders'] = (branch_rejections['Converted Orders']/branch_rejections['Rejected Orders'])*100
    branch_rejections = branch_rejections[['Outlet','RM','SRM','Total Ins Orders','Rejected Orders','% Rejected Orders','Converted Orders','% Converted Orders']]
    branch_rejections = branch_rejections.fillna(0).replace(np.inf,0)
    print(branch_rejections)



    """ Approval's Rejections """
    approval = ('approvals2', 'approvals1','approvalsoh1','approvalsoh2','approvalsyh1','approvalsyh2')
    approvals = orderlog[orderlog['odsc_createdby'].isin(approval)]
    
    approvals_pivot = approvals.pivot_table(index = ['Outlet','RM','SRM'],aggfunc = {'doc_entry':np.count_nonzero,'draft_orderno':np.count_nonzero}).reset_index().rename(columns = {'doc_entry':'Rejected Orders'}).drop_duplicates(subset = 'Outlet', keep = 'first').fillna(0)

    approvals_rej = pd.merge(approvals_pivot,srm_rm_new,on = ['Outlet','RM','SRM'],how = 'right').sort_values(by = 'Rejected Orders',ascending = True).rename(columns = {'draft_orderno':'Converted Orders'}).fillna(0)

    approvals_rej = pd.merge(approvals_rej,orders_pivot,on = 'Outlet',how = 'left')
    approvals_rej['% Rejected Orders'] = (approvals_rej['Rejected Orders']/approvals_rej['Total Ins Orders'])*100
    approvals_rej['% Converted Orders'] = (approvals_rej['Converted Orders']/approvals_rej['Rejected Orders'])*100
    approvals_rej = approvals_rej[['Outlet','RM','SRM','Total Ins Orders','Rejected Orders','% Rejected Orders','Converted Orders','% Converted Orders']]
    approvals_rej = approvals_rej.fillna(0).replace(np.inf,0)
    print(approvals_rej)



    """ Insurance Rejections """
    insurance = ('insbr1','insbr2','insbr3', 'insyh')
    insurance_desk = orderlog[orderlog['odsc_createdby'].isin(insurance)]
    
    insurance_desk_pivot = insurance_desk.pivot_table(index = ['Outlet','RM','SRM'],aggfunc = {'doc_entry':np.count_nonzero,'draft_orderno':np.count_nonzero}).reset_index().rename(columns = {'doc_entry':'Rejected Orders'}).drop_duplicates(subset = 'Outlet', keep = 'first').fillna(0)

    insurance_desk_rej = pd.merge(insurance_desk_pivot,srm_rm_new,on = ['Outlet','RM','SRM'],how = 'right').sort_values(by = 'Rejected Orders',ascending = True).rename(columns = {'draft_orderno':'Converted Orders'}).fillna(0)

    insurance_desk_rej = pd.merge(insurance_desk_rej,orders_pivot,on = 'Outlet',how = 'left')
    insurance_desk_rej['% Rejected Orders'] = (insurance_desk_rej['Rejected Orders']/insurance_desk_rej['Total Ins Orders'])*100
    insurance_desk_rej['% Converted Orders'] = (insurance_desk_rej['Converted Orders']/insurance_desk_rej['Rejected Orders'])*100
    insurance_desk_rej = insurance_desk_rej[['Outlet','RM','SRM','Total Ins Orders','Rejected Orders','% Rejected Orders','Converted Orders','% Converted Orders']]
    insurance_desk_rej = insurance_desk_rej.fillna(0).replace(np.inf,0)
    print(insurance_desk_rej)

    """ Save to an Excel File"""
    import xlsxwriter
    print(xlsxwriter.__version__)
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    with pd.ExcelWriter(r"/home/opticabi/Documents/optica_reports/insurance_rejections/Insurance Rejections.xlsx", engine='xlsxwriter') as writer:    
        orderlog.to_excel(writer,sheet_name='All Rejections', index=False)    
        approvals.to_excel(writer,sheet_name='Approvals Rejections', index=False)
        insurance_desk.to_excel(writer,sheet_name='Insurance Desk Rejections', index=False)
        approvals_rej.to_excel(writer,sheet_name='% Approval Desk Rejections', index=False)
        insurance_desk_rej.to_excel(writer,sheet_name='% Insurance Desk Rejections', index=False)


    """Create the SMTP for the table above"""

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
                    <p><b>Dear Christopher,</b></p>
                    <p>
                        This report shows the rejections made for the stated period above
                    </p>                                        
                </div>           
            </div>
            <br>
            <div class = "salutation">
                <p><b>Kind Regards, <br> Data Team<b></p>
            </div>
        </body>
    </html>
        """

    to_date = get_todate()
    # to_date = '2023-08-01'
    # till_date = '2023-08-31'
    sender_email = os.getenv("wairimu_email")
    # receiver_email = 'wairimu@optica.africa'
    receiver_email = ['wairimu@optica.africa','christopher@optica.africa','andrew@optica.africa']
    email_message = MIMEMultipart()
    email_message["From"] = sender_email
    email_message["To"] = r','.join(receiver_email)
    email_message["Subject"] = f"Insurance Desk and Approval's Rejections for {to_date}"
    email_message.attach(MIMEText(html, "html"))

    # Open the Excel file and attach it to the email
    with open('/home/opticabi/Documents/optica_reports/insurance_rejections/Insurance Rejections.xlsx', 'rb') as attachment:
        excel_file = MIMEApplication(attachment.read(), _subtype='xlsx')
        excel_file.add_header('Content-Disposition', 'attachment', filename='Kenya Insurance Rejections.xlsx')
        email_message.attach(excel_file)


    smtp_server = smtplib.SMTP("smtp.gmail.com", 587)
    smtp_server.starttls()
    smtp_server.login(sender_email, os.getenv("wairimu_password"))
    text = email_message.as_string()
    smtp_server.sendmail(sender_email, receiver_email, text)
    smtp_server.quit()
if __name__ == '__main__': 
    rejections()  

