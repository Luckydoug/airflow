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
    path,  
    assert_date_modified,
    record_sent_branch,
    return_sent_emails,
    create_initial_file,
    createe_engine, 
    fetch_gsheet_data
)

engine = createe_engine()

def fetch_et_non_conversions():
    today = datetime.date.today()
    # yesterday = today - datetime.timedelta(days=1)
    monthstart = datetime.date(today.year, today.month, 1)
    datefrom = (monthstart - dateutil.relativedelta.relativedelta(months=3))
    days = 7

    et_q = f"""           
        SELECT code, create_date, create_time, optom, optom_name, rx_type, branch_code, cust_code, status, patient_to_ophth, "RX", 
        sales_employees, handed_over_to, view_doc_entry, view_date, view_creator, last_viewed_by, branch_viewed, order_converted,
        ods_insurance_order, order_converted_mode, date_converted, on_after, on_after_status, conversion_reason, conversion_remarks,
        high_rx, non_conversion, draft_orderno1, creation_date, item_desc1, item_brand_name, "OTC", gross_price_after_disc, r           
        FROM report_views.non_conversions_otcorders
        where create_date::date between '{datefrom}' and '{today}'
        """
    conv = pd.read_sql_query(et_q,con=engine)
    return conv
fetch_et_non_conversions()
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
        'view_date':'View Date',
        'conversion_reason':'Conversion Reason',
        'conversion_remarks':'Conversion Remarks',
        'draft_orderno1' : 'Order Number',
        'item_desc1' : 'Item Description'
    },axis=1,inplace=True)

    non_q_cols = ['ET Date','ET Time','Branch','Customer Code','Optom','EWC Name','Conversion Reason','Conversion Remarks','Order Number','Item Description']
    non_q  = non_q[non_q_cols]
    non_q["Remarks"] = ""
    non_q["EWC Sign"] = ""
    print(non_q)

    with pd.ExcelWriter(f"{path}et_non_conversions/et_non_conversions.xlsx", engine='xlsxwriter') as writer:  
            non_q.to_excel(writer, sheet_name='Master',index=False)
            for group, dataframe in non_q.groupby('Branch'):
                name = f'{group}'
                dataframe.to_excel(writer,sheet_name=name, index=False)


manipulate_et_non_conversions()
def smtp():
    branch_data = fetch_gsheet_data()["branch_data"]
    log_file=f"{path}et_non_conversions/branch_log.txt"
    load_dotenv()
    your_email = os.getenv("wairimu_email")
    password = os.getenv("wairimu_password")
    create_initial_file(log_file)
    et_non_q_file = f"{path}et_non_conversions/et_non_conversions.xlsx"
    print(et_non_q_file)

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
        # selectedBranches = ["DIA","KII","NAR","OHO","COR","YOR","JUN"]
        # selectedBranches = 'THI'
        targetbranches = branch_data[branch_data['Outlet']=='THI']
        # targetbranches = branch_data[branch_data['Outlet'].isin(selectedBranches)]
        for index, row in targetbranches.iterrows():
            branchcode = row['Outlet']
            branchname = row['Branch']
            branchemail = row['Email']
            srmemail = row['SRM Email']
            
            # if branchcode not in ["OHO","COR","YOR"]:
            #     continue
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
            # receiver_email = ["cavin@optica.africa","wairimu@optica.africa"]
            receiver_email = [branchemail,srmemail,'cavin@optica.africa ']

            
            if branchcode == "OHO":
                receiver_email = [branchemail,srmemail,'kush@optica.africa','wazeem@optica.africa',"susan@optica.africa","wairimu@optica.africa"]
            if branchcode == "YOR":
                receiver_email = [srmemail,'kush@optica.africa','wazeem@optica.africa',"yh.manager@optica.africa","wairimu@optica.africa"]
            if branchcode == "JUN":
                receiver_email = [branchemail,"cavin@optica.africa","wairimu@optica.africa"]

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
                    
            NonConversionsfilename = path + f'et_non_conversions/branches/{branchcode} Non Conversions Remarks - {report_date}.xlsx'

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

def clean_folder(dir_name=f"{path}et_non_conversions/branches/"):
    files = os.listdir(dir_name)
    for file in files:
        if file.endswith(".xlsx"):
            os.remove(os.path.join(dir_name, file))

# conv = fetch_et_non_conversions()
# non_con_et = manipulate_et_non_conversions()
if __name__ == '__main__':
    smtp()
    clean_folder()
        


