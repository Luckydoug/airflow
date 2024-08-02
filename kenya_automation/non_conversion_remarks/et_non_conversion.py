import sys
import numpy as np
sys.path.append(".")
import pandas as pd
from airflow.models import variable
import pandas as pd
import dateutil
from datetime import date, timedelta
from dotenv import load_dotenv
import smtplib
import ssl
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pygsheets
from email.mime.application import MIMEApplication
from sub_tasks.libraries.utils import (
    path,  
    record_sent_branch,
    return_sent_emails,
    create_initial_file,
    createe_engine
)
from sub_tasks.libraries.styles import styles
from sub_tasks.libraries.utils import return_evn_credentials
from kenya_automation.non_conversion_remarks.utils import generate_html_and_subject
from reports.draft_to_upload.utils.utils import return_report_daterange
from reports.draft_to_upload.utils.utils import get_report_frequency
from sub_tasks.libraries.utils import fourth_week_start, fourth_week_end

selection = get_report_frequency()
selection == 'Weekly'
if selection == 'Weekly':
    start_date = fourth_week_start
    yesterday = fourth_week_end
elif selection == 'Daily':
    start_date = return_report_daterange(selection)
    yesterday = start_date



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
                   FROM mabawa_mviews.et_conv
                  WHERE (et_conv.status <> ALL (ARRAY['CanceledEyeTest'::text,'Cancel'::text, 'Unstable'::text,'Hold'::text])) 
                  AND (et_conv.patient_to_ophth <> 'Yes'::text OR et_conv.patient_to_ophth IS NULL)
                  AND activity_no is null
                  ) a
             LEFT JOIN mabawa_dw.dim_users b ON a.optom = b.se_optom
             left join mabawa_staging.source_customers sc on sc.cust_code::text = a.cust_code::text
          WHERE a.r = 1 
          AND (a.branch_code <> ALL (ARRAY['0MA'::text, 'HOM'::text, 'null'::text])) 
          AND a."RX" = 'High Rx'::text 
          AND (a.days > 7 or a.order_converted IS null)   
          AND create_date::date between '{datefrom}' and '{today}'
          AND mode_of_pay = 'Cash'
        """
    conv = pd.read_sql_query(et_q,con=engine)
    return conv

# fetch_et_non_conversions()             

def smtp():
    engine = createe_engine()
    current_date = date.today()
    if current_date.weekday() == 0:  # 0 means Monday
        days_back_to_report_day = timedelta(days=1)
    else:
        days_back_to_report_day =timedelta(days=1)
    pwfrom = current_date - days_back_to_report_day
    pwto = current_date - days_back_to_report_day

    """ Non Converted Registration"""
    non_reg_conv = f"""  
    SELECT cust_code as "Customer Code",
    cust_createdon as "Date", cust_outlet as "Outlet", su.user_name as "Staff Name", 
    case when cust_type = 'OTC' then 'Cash' else cust_type end as "Customer Type",
    insurance_name as "Insurance Name",cust_insurance_scheme as "Insurance Scheme",
    conversion_reason as "Conversion Reason", conversion_remark as "Conversion Remark",null as "Branch Remark"
    FROM mabawa_mviews.reg_conv reg
    left join 
    (
    select user_code,user_name,se_optom,user_department_name
    from mabawa_staging.source_users
    where user_code ~ '^[0-9]{4}$'
    ) su on reg.cust_sales_employeecode::text = su.se_optom::text
    where cust_createdon::date between '{pwfrom}' and '{pwto}'
    and cust_active_status <> 'N'
    and days is null
    and cust_outlet not in ('0MA','null', 'HOM')
    and cust_campaign_master <> 'GMC'
    """
    dfnon_reg_conv = pd.read_sql_query(non_reg_conv,con = engine)
    dfnon_reg_conv[['Conversion Remark','Conversion Reason','Branch Remark']] = dfnon_reg_conv[['Conversion Remark','Conversion Reason','Branch Remark']].fillna('')

    """ Eye test older than 30 days """
    old_et_viewed = f"""     
                        WITH old_et_optom AS (
                    select * from  mabawa_mviews.optoms_older_than_30days_eyetest_viewed_conversion                    
                    where "View Date"::date between '{pwfrom}' and '{pwto}'
                    and "RX" = 'High Rx'
                    and (days > 7 or days is null)
                    ), old_et_salespersons AS (
                    select * from mabawa_mviews.salespersons_older_than_30days_eyetest_viewed_conversion
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
                        old_et_optom."View Date",
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
    dfold_et_viewed = dfold_et_viewed.drop_duplicates(subset=['Visit ID'],keep = 'first')
    dfold_et_viewed['Branch Remark'] = dfold_et_viewed['Branch Remark'].fillna(" ")

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

    non_q_cols = ['ET Code','ET Date','ET Time','Outlet','Customer Code','Mode of Pay','Optom','Handed Over To','Last Viewed By','Conversion Reason','Conversion Remarks']
    non_q  = non_q[non_q_cols]
    non_q["Remarks"] = ""
    non_q["EWC Sign"] = ""
    non_q["Branch Remark"] = ""
    non_q['ET Date and Time'] = non_q['ET Date'].astype(str) + " " + non_q['ET Time'].astype(str)
    et_nonconv = non_q[['ET Code','ET Date and Time','Outlet','Customer Code','Mode of Pay','Optom','Handed Over To','Last Viewed By','Conversion Reason','Conversion Remarks','Branch Remark'
                                    ]]
    et_nonconv = et_nonconv.replace(np.nan, " ")


    """Blue Block Recommended Eye tests"""
    bbrecomm = f"""
            select 
        distinct
            sp.code as "ET Code",sp.create_date as "ET Date",sp.branch_code as "Outlet",sp.cust_code as "Customer Code",su.user_name as "Optom",
            sp.slaesperson_rmks as "SalesPerson Remarks",
            --,on_after as "Order Created",on_after_status as "Order Status",
            --insurance_feedback as "Insurance Feedback",
            sp.conversion_reason as "Conversion Reason",sp.conversion_remarks as "Conversion Remarks",
            null as "Branch Remark"
        from mabawa_staging.source_prescriptions sp 
        left join mabawa_staging.source_users su on sp.latest_updated_optomid = su.user_code 
        left join mabawa_mviews.et_conv ec on sp.code = ec.code 
        left join reports_tables.branch_data bd on sp.branch_code = bd.branch_code
        where sp.status <> 'Cancel'
        and "RX" = 'High Rx'
        and slaesperson_rmks ~~* any(array['%%BB%%','%%blue%%','%%blu%%','%%bluv%%','%%bb%%','%%Blue%%','%%Bluv%%','Blu'])
        and order_converted is null
        and sp.create_date::date between '{pwfrom}' and '{pwto}'
    """
    bbrecomm_df = pd.read_sql_query(bbrecomm,con = engine)
    bbrecomm_df['Branch Remark'] = bbrecomm_df['Branch Remark'].fillna(" ")

    """Branches below 80% High RX Conversion to share their non conversions"""
    highrxbelowthresh = f"""select * 
                        from 
                        (
                        select 
                            branch_code,
                            sum("High RX") as "High RX Eye Tests",
                            sum(high_converted) as "High RX Orders",
                            ROUND((SUM(high_converted)::numeric / NULLIF(SUM("High RX")::numeric, 0)) * 100, 0) AS "High RX Conversion"
                        FROM    
                        (
                        select 
                            date_trunc('Month',create_date::date)::date as "Month",
                            row_number() over(partition by create_date,et.cust_code order by days, rx_type, code desc) as rw,
                            code,create_date,create_time,optom,optom_name,rx_type,branch_code,et.cust_code,status,patient_to_ophth,"RX",plano_rx,
                            sales_employees,handed_over_to,view_doc_entry,view_date,view_creator,last_viewed_by,branch_viewed,
                            order_converted,ods_creator,days,
                            coalesce(ods_creator,view_creator,sales_employees) as staff_code,
                            case when days <= 7 then 1 else 0 end as cnvrtd,
                            case when "RX" = 'Low Rx'  then 1 else 0 end as "Low RX",
                            case when "RX" = 'High Rx'  then 1 else 0 end as "High RX",
                            case when (days <= 7 and "RX" = 'High Rx') then 1 else 0 end as high_converted,
                            case when (days <= 7 and "RX" = 'Low Rx') then 1 else 0 end as low_converted
                        from mabawa_mviews.et_conv et
                        left join mabawa_staging.source_orderscreen so on et.order_converted = so.doc_no::text
                        where status = 'Close'
                        and (patient_to_ophth not in ('Yes') or patient_to_ophth is null)
                        and et.cust_code not  in (select cust_code from mabawa_staging.source_customers sc 
                        where cust_campaign_master = 'GMC')
                        and create_date::date >= '2024-06-17' 
                        and create_date::date <= '2024-06-23' 
                        and branch_code not in ('0MA','HOM')
                        ) a
                        where rw=1
                        group by branch_code
                        ) aa
                        where "High RX Conversion" < 80
                        """
    highrxbelowthresh_df = pd.read_sql_query(highrxbelowthresh,con = engine)    
#-------------------------
    from reports.draft_to_upload.data.fetch_data import fetch_branch_data
    engine = createe_engine()
    branch_data = fetch_branch_data(engine=engine,database="reports_tables")
    toshareonly = highrxbelowthresh_df['branch_code'].to_list()
    # toshareonly = ['NAN','MER',	'RIV','OHO','ROS','EBK','UNI','MAL','AGR','IMA','NGO','NWE','BUS','CAP','TUF','INM','JUN','KAU','YOR','KAR','OIL','THI']

    log_file=f"{path}et_non_conversions/branch_log.txt"
    load_dotenv()
    create_initial_file(log_file)

    today = date.today()
    report_date = today
    if today.weekday() == 0:  # 0 means Monday
        report_date = (today - dateutil.relativedelta.relativedelta(days=1)).strftime('%Y-%m-%d')
    else:
        report_date = (today - dateutil.relativedelta.relativedelta(days=1)).strftime('%Y-%m-%d')

  
    tst = branch_data
    test = tst.copy()
    targetbranches = test.set_index("Outlet")
    tst = tst


    for branch in tst['Outlet']:
        branchname = targetbranches.loc[branch,'Branch']
        branchemail = targetbranches.loc[branch,'Email']
        rmemail = targetbranches.loc[branch,'RM Email']
        srmemail = targetbranches.loc[branch,'SRM Email']
        escalation_email = targetbranches.loc[branch,'Escalation Email']
        retailanalyst = targetbranches.loc[branch,'Retail Analyst']

        if branch in toshareonly:
            dataframe_dict = {
                "High RX Non Conversion":et_nonconv,
                "Viewed Eye test older than 30 days and did not convert": dfold_et_viewed,
                "Registration Non Conversion":dfnon_reg_conv,
                "Blue Block Recommendations":bbrecomm_df}
            
        else:
            dataframe_dict = {
                "Viewed Eye test older than 30 days and did not convert": dfold_et_viewed,
                "Registration Non Conversion":dfnon_reg_conv,
                "Blue Block Recommendations":bbrecomm_df}

        html,subject = generate_html_and_subject(
            branch = branch, 
            branch_name = branchname,
            dataframe_dict = dataframe_dict,
            date = report_date,
            styles = styles
        )

        if html is None and subject is None:
            continue
        
 
        sender_email,password = return_evn_credentials(retailanalyst.lower())
        print(retailanalyst.lower(),branchname)
        # sender_email = 'wairimu@optica.africa'
        # password = 'Wairimu@Optica'
              
        # receiver_email = 'tstbranch@gmail.com'
        receiver_email = [branchemail,escalation_email]
        if branch == "OHO":
            receiver_email = [branchemail,escalation_email,'tiffany@optica.africa',"susan@optica.africa","duncan.muchai@optica.africa","wairimu@optica.africa","kush@optica.africa","wazeem@optica.africa","ian.gathumbi@optica.africa"]
        if branch == "YOR":
            receiver_email = [branchemail,escalation_email,"yh.manager@optica.africa","wairimu@optica.africa","kush@optica.africa","wazeem@optica.africa","ian.gathumbi@optica.africa"]
        if branch == "COR":
            receiver_email = [branchemail,escalation_email,"wairimu@optica.africa","kush@optica.africa","wazeem@optica.africa","ian.gathumbi@optica.africa"]
        if branch == "TRM":
            receiver_email = [branchemail,escalation_email,"wairimu@optica.africa","kush@optica.africa","wazeem@optica.africa","ian.gathumbi@optica.africa", "douglas.kathurima@optica.africa"]

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
        
        email_subjects = {
            "Date": [],
            "Branch": [],
            "Email": [],
            "Subject": [],
            "Retail Analyst": [],
            "Status": []
        }

        if branchemail not in return_sent_emails(log_file):
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                print(sender_email,password)
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
        
                email_subjects["Branch"].append(branch)
                email_subjects["Email"].append(branchemail)
                email_subjects["Date"].append(report_date)
                email_subjects["Status"].append("Open")
                email_subjects["Subject"].append(subject)
                email_subjects["Retail Analyst"].append(retailanalyst.capitalize())
        else:
            print(f"{branchname} Already sent")
            continue

        email_dataframe = pd.DataFrame(email_subjects)
        gc = pygsheets.authorize(service_file=r"/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json")
        sh = gc.open_by_key('1m-CCmDtvnqGv6FWuZuzQLLdG5jxLaXoMH6VStZyd32s')
        worksheet = sh.worksheet_by_title("Insurance")
        existing_data = pd.DataFrame(worksheet.get_all_records())

        new_data = pd.concat([existing_data, email_dataframe], ignore_index=True)
        new_data = new_data.fillna("")
        new_data = new_data.drop_duplicates(subset = ["Subject"], keep = "first")
        worksheet.set_dataframe(new_data, start="A1")



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
        


