import sys
import numpy as np
sys.path.append(".")

# Import Libraries
import json
import psycopg2
import requests
import pandas as pd
from airflow.models import variable
import pandas as pd
import datetime, dateutil
from dotenv import load_dotenv
import smtplib
import ssl
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from sub_tasks.libraries.utils import (
    uganda_path,  
    assert_date_modified,
    record_sent_branch,
    return_sent_emails,
    create_initial_file,
    create_unganda_engine, 
    fetch_gsheet_data
)

engine = create_unganda_engine()

def fetch_et_non_conversions():
    today = datetime.date.today()
    monthstart = datetime.date(today.year, today.month, 1)
    datefrom = (monthstart - dateutil.relativedelta.relativedelta(months=3))

    et_q = """           
    select 
            code, create_date, create_time, optom, optom_name, rx_type, branch_code, a.cust_code, status,
            patient_to_ophth, "RX", sales_employees, handed_over_to, view_doc_entry, view_date, view_creator, 
            last_viewed_by, branch_viewed, order_converted, a.ods_insurance_order, order_converted_mode, date_converted, on_after,  on_after_status,
            case when "RX" = 'High Rx' then 1 else 0 end as high_rx,
            case when (a.days >= %(Days)s or on_after is null or (on_after_status in ('Draft Order Created','Pre-Auth Initiated For Optica Insurance','Customer to Revert','Cancel Order') and order_converted is null)) then 1 else 0 end as non_conversion
    from
    (select row_number() over(partition by cust_code, create_date order by days, rx_type, code desc) as r, *
    from mawingu_mviews.et_conv
    where status not in ('Cancel','Unstable')
    and (patient_to_ophth not in ('Yes') or patient_to_ophth is null)) as a 
    left join mawingu_staging.source_users b on a.optom::text = b.se_optom::text
    where a.r = 1
    and a.create_date::date >=  %(From)s
    and a.create_date::date <= %(To)s
    and a.branch_code not in ('0MA','HOM','null')
    and "RX" = 'High Rx'
    and (a.days >= %(Days)s or on_after is null or (on_after_status in ('Draft Order Created','Pre-Auth Initiated For Optica Insurance','Customer to Revert','Cancel Order') and order_converted is null))
    """
    conv = pd.read_sql_query(et_q,con=engine,params={'From':datefrom,'To':today,'Days':14})
    return conv


def manipulate_et_non_conversions():
    conv = fetch_et_non_conversions()
    #create daterange
    current_date = datetime.datetime.now()
    if current_date.weekday() == 0:  # 0 means Monday
        days_back_to_report_day = datetime.timedelta(days=2)
    else:
        days_back_to_report_day = datetime.timedelta(days=1)
    pwfrom = current_date - days_back_to_report_day
    pwto = current_date - days_back_to_report_day

    conv['ET Date'] = pd.to_datetime(conv['create_date'],yearfirst=True)
    #get date et non conversions of the date range
    non_q = conv[(conv['ET Date'].dt.date>=pwfrom.date()) & (conv['ET Date'].dt.date<=pwto.date())]
    non_q = non_q[non_q["non_conversion"] > 0]
    non_q = non_q[non_q["order_converted"].isna()]

    non_q.rename({
        'code':'ET Code',
        'create_time':'ET Time',
        'branch_code':'Branch',
        'optom_name':'Optom',
        'cust_code':'Customer Code',
        'mode_of_pay':'Customer Type',
        'handed_over_to':'Handed Over To',
        'last_viewed_by':'EWC Name',
        'view_date':'View Date'
    },axis=1,inplace=True)

    non_q_cols = ['ET Date','ET Time','Branch','Customer Code','Optom','EWC Name']
    non_q  = non_q[non_q_cols]
    non_q["Remarks"] = ""
    non_q["EWC Sign"] = ""

    with pd.ExcelWriter(f"{uganda_path}et_non_conversions/et_non_conversions.xlsx", engine='xlsxwriter') as writer:  
            non_q.to_excel(writer, sheet_name='Master',index=False)
            for group, dataframe in non_q.groupby('Branch'):
                name = f'{group}'
                dataframe.to_excel(writer,sheet_name=name, index=False)



def smtp():
    ug_srm_rm = fetch_gsheet_data()["ug_srm_rm"]
    log_file=f"{uganda_path}et_non_conversions/branch_log.txt"
    load_dotenv()
    your_email = os.getenv("wairimu_email")
    password = os.getenv("wairimu_password")
    create_initial_file(log_file)
    et_non_q_file = f"{uganda_path}et_non_conversions/et_non_conversions.xlsx"

    # et_non_q = pd.ExcelFile(et_non_q_file)

    today = datetime.date.today()
    report_date = today

    if today.weekday() == 0:  # 0 means Monday
        report_date = (today - dateutil.relativedelta.relativedelta(days=2)).strftime('%d-%m-%Y')
    else:
        report_date = (today - dateutil.relativedelta.relativedelta(days=1)).strftime('%d-%m-%Y')

    if not assert_date_modified([et_non_q_file]):
        return
    else:
        for index, row in ug_srm_rm.iterrows():
            branchcode = row['Outlet']
            branchname = row['Branch']
            branchemail = row['Email']
            RMemail = row['RM Email']
            try:
                report_table = pd.read_excel(et_non_q_file, index_col=False, sheet_name=branchcode)
            except:
                print(branchname + " is missing.")
                continue
            # print(report_table)
            # break
            html = '''
                <html>
                    <head>
                        <style>
                            body, p, div, span, var{{font-family:Times New Roman,serif}}
                        </style>
                    </head>
                    <body>
                        <p>Dear {branchname} Team,</p>
                        <p>Please provide feedback for the Non Converted Eye Tests done on the {report_date} in the attachment.</p>
                    </body>      
                </html>
                '''.format(branchname = branchname,
                        report_date = report_date
                        )

            # receiver_email = ["tstbranch@gmail.com"]
            # receiver_email = ["wairimu@optica.africa"]
            receiver_email = [branchemail,RMemail,"raghav@optica.africa","wairimu@optica.africa"]
           
        # Create a MIMEMultipart class, and set up the From, To, Subject fields
            email_message = MIMEMultipart()
            email_message['From'] = your_email
            email_message['To'] = ','.join(receiver_email)
            email_message['Subject'] = f'{branchcode} High RX Non Conversions - {report_date}'
            email_message.attach(MIMEText(html, "html"))

            def attach_file(email_message, filename, name):
                with open(filename, "rb") as f:
                    file_attachment = MIMEApplication(f.read())

                    file_attachment.add_header(
                        "Content-Disposition",
                        f"attachment; filename= {name}"
                    )

                    email_message.attach(file_attachment)
                    
            NonConversionsfilename = uganda_path + f'et_non_conversions/branches/{branchcode} Non Conversions Remarks - {report_date}.xlsx'

            with pd.ExcelWriter(NonConversionsfilename,engine='xlsxwriter') as writer:
                report_table.to_excel(writer,sheet_name='NonConversions', index=False)

            attach_file(email_message,NonConversionsfilename, f"{branchcode} Non Conversions Remarks - {report_date}.xlsx")

            # Convert it as a string
            email_string = email_message.as_string()

            if branchemail not in return_sent_emails(log_file):
                context = ssl.create_default_context()
                with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                    server.login(your_email, password)
                    server.sendmail(
                        your_email, 
                        receiver_email, 
                        email_string
                    )
                    record_sent_branch(
                        branchemail, 
                        log_file
                    )

            else:
                continue

def clean_folder(dir_name=f"{uganda_path}et_non_conversions/branches/"):
    files = os.listdir(dir_name)
    for file in files:
        if file.endswith(".xlsx"):
            os.remove(os.path.join(dir_name, file))

# conv = fetch_et_non_conversions()
# non_con_et = manipulate_et_non_conversions()
if __name__ == '__main__':
    smtp()
    clean_folder()
        


