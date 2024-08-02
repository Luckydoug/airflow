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
from datetime import date,timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from sub_tasks.libraries.utils import (
    rwanda_path,  
    assert_date_modified,
    record_sent_branch,
    return_sent_emails,
    create_initial_file,
    create_rwanda_engine, 
    fetch_gsheet_data
)
from kenya_automation.non_conversion_remarks.utils import generate_html_and_subject
from sub_tasks.libraries.styles import styles

engine = create_rwanda_engine()

def fetch_et_non_conversions():
    today = datetime.date.today()
    monthstart = datetime.date(today.year, today.month, 1)
    datefrom = (monthstart - dateutil.relativedelta.relativedelta(months=3))

    et_q = f"""           
   SELECT a.code,
            a.create_date,
            lpad(a.create_time::text,4,'0')::time as create_time,
            a.optom,
            a.optom_name,
            a.rx_type,
            a.branch_code,
            a.cust_code,
            a.mode_of_pay,
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
            a.insurance_name,
            a.insurance_scheme,
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
                    WHEN a.days > 7 or a.order_converted IS NULL THEN 1
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
                   FROM voler_mviews.et_conv
                  WHERE (et_conv.status <> ALL (ARRAY['CanceledEyeTest'::text,'Cancel'::text, 'Unstable'::text,'Hold'::text])) 
                  AND (et_conv.patient_to_ophth <> 'Yes'::text OR et_conv.patient_to_ophth IS NULL)
                  AND activity_no is null
                  ) a
             left join voler_staging.source_users b on a.optom::text = b.se_optom::text
             left join voler_staging.source_customers sc on sc.cust_code::text = a.cust_code::text
          WHERE a.r = 1 
          AND (a.branch_code <> ALL (ARRAY['0MA'::text, 'HOM'::text, 'null'::text])) 
          AND a."RX" = 'High Rx'::text 
          AND (a.days > 7 or a.order_converted IS null)   
          AND create_date::date between '{datefrom}' and '{today}'
          AND mode_of_pay = 'Cash'
          """
    conv = pd.read_sql_query(et_q,con=engine,params={'From':datefrom,'To':today,'Days':7})
    return conv

# fetch_et_non_conversions()

def smtp():
    engine = create_rwanda_engine()
    current_date = date.today()
    if current_date.weekday() == 0:  # 0 means Monday
        days_back_to_report_day = timedelta(days=1)
    else:
        days_back_to_report_day =timedelta(days=1)
    pwfrom = current_date - days_back_to_report_day
    pwto = current_date - days_back_to_report_day


    """Registered Customers """
    registeredcustomers = f"""select cust_outlet as "Outlet",count(cust_code) as "Registered Customers" from voler_staging.source_customers sc 
                            where cust_createdon::date between '{pwfrom}' and '{pwto}'
                            group by cust_outlet 
                            """
    dfregisteredcustomers = pd.read_sql_query(registeredcustomers,con = engine)
    

    """ Non Converted Registration"""
    non_reg_conv = f""" SELECT cust_code as "Customer Code", cust_createdon as "Date", cust_outlet as "Outlet", 
                    su.user_name as "Staff Name", conversion_reason as "Conversion Reason", conversion_remark as "Conversion Remark"
                FROM voler_mviews.v_reg_conv reg
                left join voler_staging.source_users su on reg.cust_sales_employeecode::text = su.se_optom::text
                where cust_createdon::date between '{pwfrom}' and '{pwto}'
                and days is null
            """
    dfnon_reg_conv = pd.read_sql_query(non_reg_conv,con = engine)
    dfnon_reg_conv['Branch Remark'] = ''


    """ Eye test older than 30 days """
    old_et_viewed = f"""  
                    WITH old_et_optom AS (
                    select * from  voler_mviews.optoms_older_than_30days_eyetest_viewed_conversion
                    where "View Date"::date between '{pwfrom}' and '{pwto}'
                    and "RX" = 'High Rx'
                    and (days > 7 or days is null)
                    ), old_et_salespersons AS (
                    select * from voler_mviews.salespersons_older_than_30days_eyetest_viewed_conversion
                    where "View Date"::date between '{pwfrom}' and '{pwto}'
                    and (days > 7 or days is null)
                    ), combined AS (
                    SELECT old_et_salespersons."Customer Code",
                        old_et_salespersons."View Date",
                        old_et_salespersons."Visit ID",
                        old_et_salespersons."RX",
                        old_et_salespersons."Eye Test Date",
                        old_et_salespersons.days_old,
                        old_et_salespersons."Viewed Code",
                        old_et_salespersons."Viewed By",
                        old_et_salespersons."Outlet",
                        old_et_salespersons."Branch Remark"
                    FROM old_et_salespersons
                    UNION ALL
                    SELECT old_et_optom."Customer Code",
                        old_et_optom."View Date"::date,
                        old_et_optom."Visit ID",
                        old_et_optom."RX",
                        old_et_optom."Eye Test Date",
                        old_et_optom.days_old,
                        old_et_optom."Viewed Code",
                        old_et_optom."Viewed By",
                        old_et_optom."Outlet",
                        old_et_optom."Branch Remark"
                    FROM old_et_optom
                    )
                    SELECT 
                        combined."Customer Code",
                        combined."View Date",
                        combined."Visit ID",
                        combined."RX",
                        combined."Eye Test Date",
                        combined.days_old,
                        combined."Viewed Code",
                        combined."Viewed By",
                        combined."Outlet",
                        NULL::text AS "Branch Remark"
                        from combined
            """
    dfold_et_viewed = pd.read_sql_query(old_et_viewed,con = engine)
    dfold_et_viewed['Branch Remark'] = ''


    """ High RX Non Converted """
    conv = fetch_et_non_conversions()

    conv['ET Date'] = pd.to_datetime(conv['create_date'],yearfirst=True)
    non_q = conv[(conv['ET Date'].dt.date>=pwfrom) & (conv['ET Date'].dt.date<=pwto)]
    non_q = non_q[non_q["non_conversion"] > 0]
    non_q = non_q[non_q["order_converted"].isna()]

    non_q.rename({
        'code':'ET Code',
        'create_time':'ET Time',
        'branch_code':'Outlet',
        'optom_name':'Optom',
        'cust_code':'Customer Code',
        'mode_of_pay':'Mode of Pay',
        'handed_over_to':'Handed Over To',
        'last_viewed_by':'Last Viewed By',
        'view_date':'View Date',
        'conversion_reason':'Conversion Reason',
        'conversion_remarks':'Conversion Remarks'
    },axis=1,inplace=True)

    non_q_cols = ['ET Date','ET Time','Outlet','Customer Code','Mode of Pay','Optom','Handed Over To','Last Viewed By','Conversion Reason','Conversion Remarks']
    non_q  = non_q[non_q_cols]
    non_q["Remarks"] = ""
    non_q["EWC Sign"] = ""
    non_q["Branch Remark"] = ""
    non_q['ET Date and Time'] = non_q['ET Date'].astype(str) + " " + non_q['ET Time'].astype(str)
    et_nonconv = non_q[['ET Date and Time','Outlet','Customer Code','Mode of Pay','Optom','Handed Over To','Last Viewed By','Conversion Reason','Conversion Remarks',"Branch Remark"]]
    et_nonconv = et_nonconv.replace(np.nan, " ")

#-------------------------
    from reports.draft_to_upload.data.fetch_data import fetch_branch_data
    engine = create_rwanda_engine()
    branch_data = fetch_branch_data(engine=engine,database="reports_tables")

    log_file=f"{rwanda_path}et_non_conversions/branch_log.txt"
    load_dotenv()
    create_initial_file(log_file)

    today = date.today()
    report_date = today
    if today.weekday() == 0:  # 0 means Monday
        report_date = (today - dateutil.relativedelta.relativedelta(days=1)).strftime('%d-%m-%Y')
    else:
        report_date = (today - dateutil.relativedelta.relativedelta(days=1)).strftime('%d-%m-%Y')

    tst = branch_data
    test = tst.copy()
    targetbranches = test.set_index("Outlet")
    tst = tst

    for branch in tst['Outlet']:
        branchname = targetbranches.loc[branch,'Branch']
        branchemail = targetbranches.loc[branch,'Email']
        rmemail = targetbranches.loc[branch,'RM Email']
        srmemail = targetbranches.loc[branch,'SRM Email']
        # retailanalyst = targetbranches.loc[branch,'Retail Analyst']

        dataframe_dict = {
            "High RX Non Conversion":et_nonconv,
            "Viewed Non Converted Eye test older than 30 days": dfold_et_viewed,
            "Registrated Customers":dfregisteredcustomers,
            "Registration Non Conversion":dfnon_reg_conv
            }

        html,subject = generate_html_and_subject(
            branch = branch, 
            branch_name = branchname,
            dataframe_dict = dataframe_dict,
            date = report_date,
            styles = styles
        )

        if html is None and subject is None:
            continue
        

        sender_email = os.getenv("getrude_email")
        password = os.getenv("getrude_password")

        # sender_email = 'wairimu@optica.africa'
        # password = 'Wairimu@Optica'

        receiver_email = [branchemail,"raghav@optica.africa","wairimu@optica.africa","felicity@optica.africa","lenah@optica.africa","john@optica.africa","nilesh@optica.africa"] 
        # receiver_email = 'tstbranch@gmail.com'

        email_message = MIMEMultipart()
        email_message['From'] = sender_email
        email_message['To'] = ','.join(receiver_email)
        email_message['Subject'] = subject #f'{branch} Response Required - Daily Non Conversion for {report_date}'
        email_message.attach(MIMEText(html, "html"))

        def attach_file(email_message, filename, name):
            with open(filename, "rb") as f:
                file_attachment = MIMEApplication(f.read())

                file_attachment.add_header(
                    "Content-Disposition",
                    f"attachment; filename= {name}"
                )

                email_message.attach(file_attachment)

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
            print(f"{branchname} Already sent")
            continue

def clean_folder(dir_name=f"{rwanda_path}et_non_conversions/branches/"):
    files = os.listdir(dir_name)
    for file in files:
        if file.endswith(".xlsx"):
            os.remove(os.path.join(dir_name, file))


if __name__ == '__main__':
    smtp()
    clean_folder()
        

        


