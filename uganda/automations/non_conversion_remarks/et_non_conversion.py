import sys
import numpy as np
sys.path.append(".")
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
    uganda_path,  
    assert_date_modified,
    record_sent_branch,
    return_sent_emails,
    create_initial_file,
    create_unganda_engine, 
    fetch_gsheet_data
)
from sub_tasks.libraries.styles import styles
from sub_tasks.libraries.utils import return_evn_credentials
from kenya_automation.non_conversion_remarks.utils import generate_html_and_subject
engine = create_unganda_engine()

def fetch_et_non_conversions():
    today = datetime.date.today()
    monthstart = datetime.date(today.year, today.month, 1)
    datefrom = (monthstart - dateutil.relativedelta.relativedelta(months=3))

    et_q = """           
    select 
            code, create_date, lpad(a.create_time::text,4,'0')::time as create_time, optom, optom_name, rx_type, branch_code, a.cust_code,a.mode_of_pay, status,
            patient_to_ophth, "RX", sales_employees, handed_over_to, view_doc_entry, view_date, view_creator, 
            last_viewed_by, branch_viewed, order_converted, a.ods_insurance_order, order_converted_mode, date_converted, on_after,  on_after_status,
            case when "RX" = 'High Rx' then 1 else 0 end as high_rx,conversion_reason,conversion_remarks,
            case when (a.days >= %(Days)s or on_after is null or (on_after_status in ('Draft Order Created','Pre-Auth Initiated For Optica Insurance','Customer to Revert','Cancel Order') and order_converted is null)) then 1 else 0 end as non_conversion
    from
    (select row_number() over(partition by cust_code, create_date order by days, rx_type, code desc) as r, *
    from mawingu_mviews.et_conv
    where status not in ('Cancel','Unstable')
    and (patient_to_ophth not in ('Yes') or patient_to_ophth is null)
    and optom not in ('data2','dennisnjo','ss','data6','yuri','data7','rm','data5','manager')) as a 
    left join mawingu_staging.source_users b on a.optom::text = b.se_optom::text
    where a.r = 1
    and a.create_date::date >=  %(From)s
    and a.create_date::date <= %(To)s
    and a.branch_code not in ('0MA','HOM','null')
    and "RX" = 'High Rx'
    and (a.days >= %(Days)s or on_after is null or (on_after_status in ('Draft Order Created','Pre-Auth Initiated For Optica Insurance','Customer to Revert','Cancel Order') and order_converted is null))
    """
    conv = pd.read_sql_query(et_q,con=engine,params={'From':datefrom,'To':today,'Days':7})
    return conv

# fetch_et_non_conversions()

def smtp():
    engine = create_unganda_engine()
    current_date = date.today()
    if current_date.weekday() == 0:  # 0 means Monday
        days_back_to_report_day = timedelta(days=2)
    else:
        days_back_to_report_day =timedelta(days=1)
    pwfrom = current_date - days_back_to_report_day
    pwto = current_date - days_back_to_report_day


    """Registered Customers """
    registeredcustomers = f"""select cust_outlet as "Outlet",count(cust_code) as "Registered Customers" from mawingu_staging.source_customers sc 
                            where cust_createdon::date between '{pwfrom}' and '{pwto}'
                            group by cust_outlet 
                            """
    dfregisteredcustomers = pd.read_sql_query(registeredcustomers,con = engine)

    """ Non Converted Registration"""
    non_reg_conv = f""" SELECT cust_code as "Customer Code", cust_createdon as "Date", cust_outlet as "Outlet", 
                    su.user_name as "Staff Name", conversion_reason as "Conversion Reason", conversion_remark as "Conversion Remark"
                FROM mawingu_mviews.v_reg_conv reg
                left join mawingu_staging.source_users su on reg.cust_sales_employeecode::text = su.se_optom::text
                where cust_createdon::date between '{pwfrom}' and '{pwto}'
                and days is null
            """
    dfnon_reg_conv = pd.read_sql_query(non_reg_conv,con = engine)


    """ Eye test older than 30 days """
    old_et_viewed = f""" with views as 
    (
    select 
            spc.code as "Visit ID",
            spc.view_date::date - ec.create_date::date AS days_old,
            case when spc.view_date::text = ec.create_date::text then 0 else 1 end as new_et_done,
            spc.view_date as "View Date",
            spc.view_time as "View Time",
            spc.requestedby,
            su.user_name as "Viewed By",
            ec.branch_code,
            aa.branch"Eye Test Branch",
            ec.create_date as "Eye Test Date",
            ec.optom,
            ec.optom_name,
            ec.rx_type as "RX Type",
            ec."RX",
            ec.cust_code as "Customer Code",
            ec.order_converted,
            ec.days,
            sp.code,sp.create_date
    from mawingu_staging.source_prescriptions_c1 spc
            left join (
                        select * 
                        from
                        (select row_number() over (partition by "date",branch,user_code order by ets desc) as r,
                        * 
                        from 
                        (select "date",
                        branch,user_code,staff_name,count(id) as ets from mawingu_mviews.all_activity ec
                        where branch not in ('0MA','null', 'HOM') 
                        and user_code is not null
                        group by branch,user_code,staff_name,"date") a) b
                        where b.r = 1
                        ) aa on aa.user_code = spc.requestedby and aa."date"::date = spc.view_date::date 
            left join mawingu_staging.source_users su on su.user_code = spc.requestedby 
            left join mawingu_mviews.et_conv ec on ec.code = spc.code
            left join mawingu_mviews.et_conv sp on sp.cust_code = ec.cust_code 
            where spc.view_date::date between '{pwfrom}' and '{pwto}'
            and (ec.days > 7 or ec.days is null)
            and ec.branch_code not in ('0MA','null', 'HOM') 
            and requestedby not in ('data7','data11','data5','data6','retman5')
    ),
    old_et as 
            (
            select 
            row_number() over (partition by "View Date","Customer Code" order by create_date desc ) as r,
            * from views
            where days_old > 30
            and new_et_done = 1
            ),
    old_et_optom as (		
            select 
            "Customer Code",
            "View Date"::date, 
            "Visit ID",
            "RX", 
            "Eye Test Date"::date, 
            days_old, 
            "Viewed By",  
            "Eye Test Branch" as "Outlet",
            null as "Branch Remark" 
            from old_et
            where r = 1
            and "RX" = 'High Rx'
            ),
    old_et_view_sp  as 
                (
                SELECT 
                    case when od.view_date::text = sp.create_date::text then 0 else 1 end as new_et_done,
                    od.cust_loyalty_code as "Customer Code",od.view_date as "View Date", od.visit_id as "Visit ID", od."RX", 
                    od.et_date as "Eye Test Date", sp.code,sp.create_date,
                    od.days_old, od.viewed_by as "Viewed By",  
                    od.branch as "Outlet",null as "Branch Remark"
                FROM mawingu_mviews.old_eyetest_viewed_conversion od
                    left join mawingu_mviews.et_conv sp on sp.cust_code = od.cust_loyalty_code 
                    where od.view_date::date between '{pwfrom}' and '{pwto}'
                    and (od.days > 7 or od.days is null)
                    and days_old > 30
                    and od.branch not in ('0MA','null', 'HOM') 
                ),
    old_et_sp as
                (
            select 
                row_number() over (partition by "View Date","Customer Code" order by create_date desc) as r,
                * 
            from old_et_view_sp
                where new_et_done = 1
                ),
    old_et_salespersons as                
                (select 
                    "Customer Code",
                    "View Date"::date,
                    "Visit ID",
                    "RX", 
                    "Eye Test Date"::date,
                    days_old,
                    "Viewed By",  
                    "Outlet",
                    null as "Branch Remark"
                from old_et_sp
                where r = 1
                and "RX" = 'High Rx'),
    combined as             
                (select * from old_et_salespersons
                union all 
                select * from old_et_optom)
    select 
            "Customer Code",
            "View Date"::date,
            "Visit ID",
            "RX", 
            "Eye Test Date"::date,
            days_old,
            "Viewed By",  
            "Outlet",
            null as "Branch Remark"
    from
    (select 
    row_number() over (partition by "Customer Code" order by "Eye Test Date" desc) as r,
    * from combined) a 
    where a.r = 1
            """
    dfold_et_viewed = pd.read_sql_query(old_et_viewed,con = engine)


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
    non_q['ET Date and Time'] = non_q['ET Date'].astype(str) + " " + non_q['ET Time'].astype(str)
    et_nonconv = non_q[['ET Date and Time','Outlet','Customer Code','Mode of Pay','Optom','Handed Over To','Last Viewed By','Conversion Reason','Conversion Remarks']]
    et_nonconv = et_nonconv.replace(np.nan, " ")

#-------------------------
    from reports.draft_to_upload.data.fetch_data import fetch_branch_data
    engine = create_unganda_engine()
    branch_data = fetch_branch_data(engine=engine,database="reports_tables")

    log_file=f"{uganda_path}et_non_conversions/branch_log.txt"
    load_dotenv()
    create_initial_file(log_file)

    today = date.today()
    report_date = today
    if today.weekday() == 0:  # 0 means Monday
        report_date = (today - dateutil.relativedelta.relativedelta(days=2)).strftime('%d-%m-%Y')
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
        

        sender_email = "felix.murithi@optica.africa"
        password = 'PetsOnViwan2#7'

        receiver_email = [branchemail,"raghav@optica.africa","wairimu@optica.africa","felicity@optica.africa","lenah@optica.africa","john@optica.africa","larry.larsen@optica.africa"] 

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
        


