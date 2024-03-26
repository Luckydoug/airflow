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
import datetime
import dateutil
from datetime import date, timedelta, datetime, time
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
    fetch_gsheet_data,
    get_todate
)
from sub_tasks.libraries.styles import styles, properties
from sub_tasks.libraries.styles import ug_styles
from sub_tasks.libraries.utils import return_evn_credentials
import datetime

engine = createe_engine()

def fetch_et_non_conversions():
    today = date.today()
    monthstart = date(today.year, today.month, 1)
    datefrom = (monthstart - dateutil.relativedelta.relativedelta(months=3))
    days = 7

    et_q = f"""           
        SELECT a.code,
            a.create_date,
            lpad(a.create_time::text,4,'0')::time as create_time,
            a.optom,
            a.optom_name,
            a.rx_type,
            a.branch_code,
            a.cust_code,
            a.status,
            a.patient_to_ophth,
            a."RX",
            a.sales_employees,
            a.handed_over_to,
            a.view_doc_entry,
            a.view_date,
            a.view_creator,
            a.last_viewed_by,
            a.branch_viewed,
            a.order_converted,
            a.ods_insurance_order,
            a.order_converted_mode,
            a.date_converted,
            a.on_after,
            a.on_after_status,
            a.conversion_reason,
            a.conversion_remarks,
                CASE
                    WHEN a."RX" = 'High Rx'::text THEN 1
                    ELSE 0
                END AS high_rx,
                CASE
                    WHEN a.days > 7 OR a.on_after IS NULL OR (a.on_after_status = ANY (ARRAY['Draft Payments Posted','Draft Order Created'::text, 'Pre-Auth Initiated For Optica Insurance'::text, 'Customer to Revert'::text, 'Cancel Order'::text])) AND a.order_converted IS NULL THEN 1
                    ELSE 0
                END AS non_conversion
           FROM ( SELECT row_number() OVER (PARTITION BY et_conv.cust_code, et_conv.create_date ORDER BY et_conv.days, et_conv.rx_type, et_conv.code DESC) AS r,
                    et_conv.code,
                    et_conv.create_date,
                    et_conv.create_time,
                    et_conv.optom,
                    et_conv.optom_name,
                    et_conv.rx_type,
                    et_conv.branch_code,
                    et_conv.cust_code,
                    et_conv.status,
                    et_conv.patient_to_ophth,
                    et_conv."RX",
                    et_conv.plano_rx,
                    et_conv.sales_employees,
                    et_conv.handed_over_to,
                    et_conv.view_doc_entry,
                    et_conv.view_date,
                    et_conv.view_creator,
                    et_conv.last_viewed_by,
                    et_conv.branch_viewed,
                    et_conv.order_converted,
                    et_conv.ods_insurance_order,
                    et_conv.order_converted_mode,
                    et_conv.date_converted,
                    et_conv.days,
                    et_conv.on_after,
                    et_conv.on_after_createdon,
                    et_conv.on_after_cancelled,
                    et_conv.on_after_status,
                    et_conv.on_after_mode,
                    et_conv.on_after_days,
                    et_conv.ods_insurance_actaprvamt1,
                    et_conv.ods__insurance_actaprvamt2,
                    et_conv.ods_total_amt,
                    et_conv.approved_amount,
                    et_conv.insurance_feedback,
                    et_conv.ods_insurance_rejectrsn1,
                    et_conv.scheme_type,
                    et_conv.on_before,
                    et_conv.on_before_cancelled,
                    et_conv.on_before_createdon,
                    et_conv.on_before_prescription_order,
                    et_conv.on_before_mode,
                    et_conv.reg_cust_type,
                    et_conv.mode_of_pay,
                    et_conv.insurance_name,
                    et_conv.insurance_scheme,
                    et_conv.conversion_reason,
                    et_conv.conversion_remarks
                   FROM mabawa_mviews.et_conv
                  WHERE (et_conv.status <> ALL (ARRAY['Cancel'::text, 'Unstable'::text])) AND (et_conv.patient_to_ophth <> 'Yes'::text OR et_conv.patient_to_ophth IS NULL)) a
             LEFT JOIN mabawa_dw.dim_users b ON a.optom = b.se_optom
          WHERE a.r = 1 AND (a.branch_code <> ALL (ARRAY['0MA'::text, 'HOM'::text, 'null'::text])) AND a."RX" = 'High Rx'::text AND (a.days >= 7 OR a.on_after IS NULL OR (a.on_after_status = ANY (ARRAY['Draft Order Created'::text, 'Pre-Auth Initiated For Optica Insurance'::text, 'Customer to Revert'::text, 'Cancel Order'::text])) AND a.order_converted IS NULL)        
          and create_date::date between '{datefrom}' and '{today}'
        """
    conv = pd.read_sql_query(et_q,con=engine)
    return conv

# fetch_et_non_conversions()

def manipulate_et_non_conversions():
    conv = fetch_et_non_conversions()
    #create daterange
    current_date = date.today()
    if current_date.weekday() == 0:  # 0 means Monday
        days_back_to_report_day = timedelta(days=2)
    else:
        days_back_to_report_day =timedelta(days=1)
    pwfrom = current_date - days_back_to_report_day
    pwto = current_date - days_back_to_report_day

    # pwfrom = pd.to_datetime('2024-02-15')
    # pwto = pd.to_datetime('2024-02-18')
    # print(pwfrom)
    # print(pwto)

    conv['ET Date'] = pd.to_datetime(conv['create_date'],yearfirst=True)
    #get date et non conversions of the date range
    non_q = conv[(conv['ET Date'].dt.date>=pwfrom) & (conv['ET Date'].dt.date<=pwto)]
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
        'last_viewed_by':'Last Viewed By',
        'view_date':'View Date',
        'conversion_reason':'Conversion Reason',
        'conversion_remarks':'Conversion Remarks'
        # 'draft_orderno' : 'Order Number',
        # 'item_desc' : 'Item Description'
    },axis=1,inplace=True)

    non_q_cols = ['ET Date','ET Time','Branch','Customer Code','Optom','Handed Over To','Last Viewed By','Conversion Reason','Conversion Remarks'
                #   ,'Order Number','Item Description'
                  ]
    non_q  = non_q[non_q_cols]
    non_q["Remarks"] = ""
    non_q["EWC Sign"] = ""
    print(non_q)

    with pd.ExcelWriter(f"{path}et_non_conversions/et_non_conversions.xlsx", engine='xlsxwriter') as writer:  
            non_q.to_excel(writer, sheet_name='Master',index=False)
            for group, dataframe in non_q.groupby('Branch'):
                name = f'{group}'
                dataframe.to_excel(writer,sheet_name=name, index=False)

# manipulate_et_non_conversions()               

def smtp():
    from reports.draft_to_upload.data.fetch_data import fetch_branch_data
    engine = createe_engine()
    branch_data = fetch_branch_data(engine=engine,database="reports_tables")

    log_file=f"{path}et_non_conversions/branch_log.txt"
    load_dotenv()

    create_initial_file(log_file)
    et_non_q_file = f"{path}et_non_conversions/et_non_conversions.xlsx"
    print(et_non_q_file)

    # et_non_q = pd.ExcelFile(et_non_q_file)

    today = date.today()
    report_date = today
    # report_from = '2024-02-15'
    # report_till = '2024-02-18'

    if today.weekday() == 0:  # 0 means Monday
        report_date = (today - dateutil.relativedelta.relativedelta(days=2)).strftime('%d-%m-%Y')
    else:
        report_date = (today - dateutil.relativedelta.relativedelta(days=1)).strftime('%d-%m-%Y')

    # if not assert_date_modified([et_non_q_file]):
    #     return
    # else:
    selectedBranches = ["OHO","VIA","NAK","NWE","INM","MUR","JUN","KAR","MAI","TRM","IPS","NAR","RON","KTL","TSQ",
                        "YOR","WGT","WES","WSQ","KIS","THI","ELD","LIK","NEX","MEG","BUS","TBZ","EMB","LAV","NYE",
                        "VMA","OIL"]     
    tst = branch_data[branch_data['Outlet'].isin(selectedBranches)] 
    test = tst.copy()
    targetbranches = test.set_index("Outlet")
    tst = tst[tst['Outlet'].isin(selectedBranches)]

    for branch in tst['Outlet']:
        print(branch_data)
        # branch_list = list(row['Outlet'])
        # branchcode = targetbranches.loc[branch,"Outlet"]
        branchname = targetbranches.loc[branch,'Branch']
        branchemail = targetbranches.loc[branch,'Email']
        rmemail = targetbranches.loc[branch,'RM Email']
        srmemail = targetbranches.loc[branch,'SRM Email']

        stephen_data = branch_data[branch_data["Retail Analyst"] == "Stephen"]
        stephen_branches = stephen_data["Outlet"].to_list()

        felix_data = branch_data[branch_data["Retail Analyst"] == "Felix"]
        felix_branches = felix_data["Outlet"].to_list()

        getrude_data = branch_data[branch_data["Retail Analyst"] == "Getrude"]
        getrude_branches = getrude_data["Outlet"].to_list()
        

        # if branchcode not in ["OHO","COR","YOR"]:
        #     continue
        try:
            report_table = pd.read_excel(et_non_q_file, index_col=False, sheet_name=branch)
        except:
            print(branchname + " is missing.")
            continue

        report_table['ET Date and Time'] = report_table['ET Date'].astype(str) + " " + report_table['ET Time'].astype(str)
        report_table = report_table[['ET Date and Time','Customer Code','Optom','Handed Over To','Last Viewed By','Conversion Reason','Conversion Remarks'
                                    # 'Order Number','Item Description'
                                    ]]
        report_table = report_table.replace(np.nan, " ")
        report_table = report_table.style.hide_index().set_properties(**properties).set_table_styles(styles)
        report_table_html = report_table.to_html(doctype_html=True)

        # break
        html = '''
            <html>
                <head>
                    <meta charset="UTF-8">
                    <meta http-equiv="X-UA-Compatible" content="IE=edge">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>Uganda Outlets Conversion Report</title>

                    <style>
                        table {{border-collapse: collapse;font-family:Trebuchet MS; font-size:9;}}
                        th {{text-align: left;font-family:Trebuchet MS; font-size:9; padding: 3px;}}
                        body, p, h3, div, span, var {{font-family:Trebuchet MS; font-size:13}}
                        td {{text-align: left;font-family:Trebuchet MS; font-size:9; padding: 8px;}}
                        h4 {{font-size: 12px; font-family: Verdana, sans-serif;}}

                        .content {{
                            margin-top: 20px !important;
                            border: none;
                            background-color: white;
                            padding: 4%;
                            width: 85%;
                            margin: 0 auto;
                            border-radius: 3px;
                        }}

                        .salutation {{
                            width: 20%;
                            margin: 0 auto;
                            margin-top: 20px;
                            font-size: 10px;
                        }}

                        .inner {{
                            margin: 5px 0px 5px 0px;
                            padding: 4px;
                        }}

                    </style>
                </head>
                <body>
                    <p>Dear {branchname} Team,</p>
                    <p>Kindly clarify on the Non Converted Eye Tests done for the stated period.</p>
                    <li>
                        <table>{report_table_html}</table
                    </li>    
                </body>      
            </html>
            '''.format(
                    branchname = branchname,
                    report_date = report_date,
                    report_table_html=report_table_html
                    )
        

        if branch in getrude_branches:
            sender_email, password = return_evn_credentials("getrude")
        elif branch in stephen_branches:
            sender_email, password = return_evn_credentials("stephen")
        elif branch in felix_branches:
            sender_email, password = return_evn_credentials("felix")
        else:
            return
        


        receiver_email = [branchemail,srmemail,'wairimu@optica.africa']
        # receiver_email = ['wairimu@optica.africa']
        if branch == "OHO":
            receiver_email = [branchemail,srmemail,'tiffany@optica.africa',"susan@optica.africa","duncan.muchai@optica.africa","wairimu@optica.africa"]
        if branch == "YOR":
            receiver_email = [srmemail,"yh.manager@optica.africa","wairimu@optica.africa"]

        
        email_message = MIMEMultipart()
        email_message['From'] = sender_email
        email_message['To'] = ','.join(receiver_email)
        email_message['Subject'] = f'Response Required: {branch} - High RX Non Conversions - {report_date}'
        email_message.attach(MIMEText(html, "html"))

        def attach_file(email_message, filename, name):
            with open(filename, "rb") as f:
                file_attachment = MIMEApplication(f.read())

                file_attachment.add_header(
                    "Content-Disposition",
                    f"attachment; filename= {name}"
                )

                email_message.attach(file_attachment)
                
        NonConversionsfilename = path + f'et_non_conversions/branches/{branch} Non Conversions Remarks - {report_date}.xlsx'

        with pd.ExcelWriter(NonConversionsfilename,engine='xlsxwriter') as writer:
            report_table.to_excel(writer,sheet_name='NonConversions', index=False)

        attach_file(email_message,NonConversionsfilename, f"{branch} Non Conversions Remarks - {report_date}.xlsx")

        # Convert it as a string
        email_string = email_message.as_string()

        if branchemail not in return_sent_emails(log_file):
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                server.login(sender_email, password)
                server.sendmail(
                sender_email, 
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
        


