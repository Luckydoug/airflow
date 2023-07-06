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
    createe_engine,
    fetch_gsheet_data
)

engine = createe_engine()
rn = datetime.datetime.now()

def fetch_delayed_and_pending_ITRs():

    pending_itr = """           
    select su.user_code as "User Code",doc_no as "Document Number", ITR.internal_no as "Internal Number",exchange_type,
    filler as "Filter", to_warehouse_code as "Branch", createdon,creationtime_incl_secs,
    statuses.created_user as "Created User",statuses.post_date::date as "Date",
        case
            when length(statuses.post_time::text) in (1,2) then null
            else
                (left(statuses.post_time::text,(length(statuses.post_time::text)-2))||':'||right(statuses.post_time::text, 2))::time
            end
                as "Time",statuses.status as "ITR Status",statuses.item_code,sales_orderno,sales_order_entry,sales_order_branch,draft_order_entry,draft_orderno,draft_order_branch
    FROM mabawa_staging.source_itr itr
    left join mabawa_staging.source_users su 
    on itr.user_signature = su.user_signature
    inner join (
        SELECT *
        FROM mabawa_staging.source_itr_log
        where post_date::date between %(From)s and %(To)s
    ) statuses
    on statuses.itr_no::text = itr.internal_no::text
    left join mabawa_staging.source_itr_details sid 
    on sid.doc_internal_id::text = statuses.itr_no::text and sid.item_no::text = statuses.item_code::text
    where su.user_name in ('DERRICK', 'manager')
    and itr.exchange_type = 'Replacement'
    and doc_status = 'O'
    and createdon::date between %(From)s and %(To)s;
    """
    itrs = pd.read_sql_query(pending_itr, con=engine, params={'From': rn.date(), 'To': rn.date()})
    return itrs

    
#    main_store_reps["Main Store Cut"] =  pd.to_datetime(rn.date().strftime("%Y/%m/%d") + " "+ main_store_reps["Main Store Cut"].astype(str), dayfirst=True).dt.time

def mainstore(open_replacemnts):
    main_store_reps = open_replacemnts[(open_replacemnts["ITR Status"].isin(["Pick List Printed","Pick List Created"])) & (open_replacemnts["Filter"].isin(["0MA"]))]
    main_store_reps["Main Store Cut"] =  pd.to_datetime(rn.date().strftime("%Y/%m/%d") + " "+ main_store_reps["Main Store Cut"].astype(str), dayfirst=True).dt.time
    main_store_reps['Delay'] = np.where((rn + datetime.timedelta(minutes=1)).time()>main_store_reps["Main Store Cut"],"Delay", "Not delay")
    main_store_reps['TenTo'] = np.where(main_store_reps['Status_Time+10min']>main_store_reps["Main Store Cut"],"Delay", "Not delay")
    main_store_reps["Classification"] = np.where(((main_store_reps["Delay"] == "Not delay") & (main_store_reps["TenTo"] == "Delay")),"Within 15 Mins of Cut Off", "Ok")
    main_store_reps["Classification"] = np.where(((main_store_reps["Delay"] == "Not delay") & (main_store_reps["TenTo"] == "Not delay")), "Ok",main_store_reps["Classification"])
    main_store_reps["Classification"] = np.where(((main_store_reps["Delay"] == "Delay") & (main_store_reps["TenTo"] == "Delay")), "Delayed",main_store_reps["Classification"])
    main_store_reps = main_store_reps[["Classification","Branch", "Document Number","ITR Status", "Status_Time","Main Store Cut","Delay","TenTo"]]
    return main_store_reps


def designerstore(open_replacemnts):
    designer_store_reps = open_replacemnts[(open_replacemnts["ITR Status"].isin(["Pick List Printed","Pick List Created"])) & (open_replacemnts["Filter"].isin(["0DS"]))]
    designer_store_reps["Designer Store Cut"] =  pd.to_datetime(rn.date().strftime("%Y/%m/%d") + " "+ designer_store_reps["Designer Store Cut"].astype(str), dayfirst=True).dt.time
    designer_store_reps['Delay'] = np.where((rn + datetime.timedelta(minutes=1)).time()>designer_store_reps["Designer Store Cut"],"Delay", "Not delay")
    designer_store_reps['TenTo'] = np.where(designer_store_reps['Status_Time+10min']>designer_store_reps["Designer Store Cut"],"Delay", "Not delay")
    designer_store_reps["Classification"] = np.where(((designer_store_reps["Delay"] == "Not delay") & (designer_store_reps["TenTo"] == "Delay")),"Within 15 Mins of Cut Off", "Ok")
    designer_store_reps["Classification"] = np.where(((designer_store_reps["Delay"] == "Not delay") & (designer_store_reps["TenTo"] == "Not delay")), "Ok",designer_store_reps["Classification"])
    designer_store_reps["Classification"] = np.where(((designer_store_reps["Delay"] == "Delay") & (designer_store_reps["TenTo"] == "Delay")), "Delayed",designer_store_reps["Classification"])
    designer_store_reps = designer_store_reps[["Classification","Branch", "Document Number","ITR Status", "Status_Time","Designer Store Cut","Delay","TenTo"]]
    return designer_store_reps

def lensstore(open_replacemnts):
    lens_store_reps = open_replacemnts[(open_replacemnts["ITR Status"].isin(["Pick List Printed","Pick List Created"])) & (open_replacemnts["Filter"].isin(["0LE"]))]
    lens_store_reps["Lens Store Cut"] =  pd.to_datetime(rn.date().strftime("%Y/%m/%d") + " "+ lens_store_reps["Lens Store Cut"].astype(str), dayfirst=True).dt.time
    lens_store_reps['Delay'] = np.where((rn + datetime.timedelta(minutes=1)).time()>lens_store_reps["Lens Store Cut"],"Delay", "Not delay")
    lens_store_reps['TenTo'] = np.where(lens_store_reps['Status_Time+10min']>lens_store_reps["Lens Store Cut"],"Delay", "Not delay")
    lens_store_reps["Classification"] = np.where(((lens_store_reps["Delay"] == "Not delay") & (lens_store_reps["TenTo"] == "Delay")),"Within 15 Mins of Cut Off", "Ok")
    lens_store_reps["Classification"] = np.where(((lens_store_reps["Delay"] == "Not delay") & (lens_store_reps["TenTo"] == "Not delay")), "Ok",lens_store_reps["Classification"])
    lens_store_reps["Classification"] = np.where(((lens_store_reps["Delay"] == "Delay") & (lens_store_reps["TenTo"] == "Delay")), "Delayed",lens_store_reps["Classification"])
    lens_store_reps = lens_store_reps[["Classification","Branch", "Document Number","ITR Status", "Status_Time","Lens Store Cut","Delay","TenTo"]]
    return lens_store_reps


def controlroom(open_replacemnts):
    control_reps =  open_replacemnts[open_replacemnts["ITR Status"].isin(["Rep Sent to Control Room"])]
    control_reps["Control Cut"] =  pd.to_datetime(rn.date().strftime("%Y/%m/%d") + " "+ control_reps["Control Cut"].astype(str), dayfirst=True).dt.time
    control_reps['Delay'] = np.where((rn + datetime.timedelta(minutes=1)).time()>control_reps["Control Cut"],"Delay", "Not delay")
    control_reps['TenTo'] = np.where(control_reps['Status_Time+10min']>control_reps["Control Cut"],"Delay", "Not delay")
    control_reps["Classification"] = np.where(((control_reps["Delay"] == "Not delay") & (control_reps["TenTo"] == "Delay")),"Within 15 Mins of Cut Off", "Ok")
    control_reps["Classification"] = np.where(((control_reps["Delay"] == "Not delay") & (control_reps["TenTo"] == "Not delay")), "Ok",control_reps["Classification"])
    control_reps["Classification"] = np.where(((control_reps["Delay"] == "Delay") & (control_reps["TenTo"] == "Delay")), "Delayed",control_reps["Classification"])
    control_reps = control_reps[["Classification","Branch", "Document Number","ITR Status", "Status_Time","Control Cut","Delay","TenTo"]]
    return control_reps


def packaging(open_replacemnts):
    packaging_reps = open_replacemnts[open_replacemnts["ITR Status"].isin(["Rep Sent to Packaging"])]
    packaging_reps["Packaging Cut"] =  pd.to_datetime(rn.date().strftime("%Y/%m/%d") + " "+ packaging_reps["Packaging Cut"].astype(str), dayfirst=True).dt.time
    packaging_reps['Delay'] = np.where((rn + datetime.timedelta(minutes=1)).time()>packaging_reps["Packaging Cut"],"Delay", "Not delay")
    packaging_reps['TenTo'] = np.where(packaging_reps['Status_Time+10min']>packaging_reps["Packaging Cut"],"Delay", "Not delay")
    packaging_reps["Classification"] = np.where(((packaging_reps["Delay"] == "Not delay") & (packaging_reps["TenTo"] == "Delay")),"Within 15 Mins of Cut Off", "Ok")
    packaging_reps["Classification"] = np.where(((packaging_reps["Delay"] == "Not delay") & (packaging_reps["TenTo"] == "Not delay")), "Ok",packaging_reps["Classification"])
    packaging_reps["Classification"] = np.where(((packaging_reps["Delay"] == "Delay") & (packaging_reps["TenTo"] == "Delay")), "Delayed",packaging_reps["Classification"])
    packaging_reps = packaging_reps[["Classification","Branch", "Document Number","ITR Status", "Status_Time","Packaging Cut","Delay","TenTo"]]
    return packaging_reps

def manipulate_delayed_and_pending_ITRs():
    itrs = fetch_delayed_and_pending_ITRs()

    itrs = itrs[itrs["draft_orderno"].notna()]
    itrs = itrs[itrs["draft_orderno"] != '']
    itrs = itrs[itrs["draft_order_entry"].notna()]
    itrs = itrs[itrs["draft_order_entry"] != '']
    itrs = itrs[itrs["draft_order_branch"].notna()]
    itrs = itrs[itrs["draft_order_branch"] != '']
    itrs['Created_Time'] = pd.to_datetime(itrs['creationtime_incl_secs'], format="%H%M%S").dt.time
    open_replacemnts = itrs[itrs["exchange_type"] == "Replacement"]
    open_replacemnts["Type"] = np.where(open_replacemnts['Created_Time'] >= datetime.time(12, 0, 0),"Second","First")
    # open_replacemnts["Type"] = open_replacemnts.apply(lambda row: "Second" if row['Created_Time'] >= datetime.time(12, 0, 0) else "First", axis=1)
    itr_cutoff = fetch_gsheet_data()["itr_cutoff"]
    open_replacemnts = pd.merge(open_replacemnts, itr_cutoff,  on=['Branch', "Type"], how='left')
    # creating Address Category, Region
    open_replacemnts['Region'] = ['HQ' if x == 'YOR OHO' else "CBD" if x == 'CBD Messenger' else 'Nairobi' if x == 'Rider 1' or x == 'Rider 2' or x == 'Rider 3'
                              else 'Upcountry' for x in open_replacemnts['Address']]
    open_replacemnts["Status_Time"] = pd.to_datetime(open_replacemnts["Time"], format="%H:%M:%S").dt.time
    open_replacemnts = open_replacemnts.sort_values(by='Time', ascending=True)
    open_replacemnts = open_replacemnts.drop_duplicates(subset="Document Number", keep="last")
    open_replacemnts['Status_Time+10min'] = rn + datetime.timedelta(minutes=15)
    open_replacemnts['Status_Time+10min'] = open_replacemnts['Status_Time+10min'].dt.time
    open_replacemnts = open_replacemnts.dropna(subset=["Warehouse Name"])
    open_replacemnts["Delay"] = np.NAN
    open_replacemnts["TenTo"] = np.NAN
    open_replacemnts["Classification"] = np.NAN
    
    main_store_reps = mainstore(open_replacemnts)
    designer_store_reps = designerstore(open_replacemnts)
    lens_store_reps = lensstore(open_replacemnts)
    control_reps = controlroom(open_replacemnts)
    packaging_reps = packaging(open_replacemnts) 
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    with pd.ExcelWriter(f"{path}delayed_and_pending_ITRs/open_ITRs.xlsx", engine='xlsxwriter') as writer:
        # Write each dataframe to a different worksheet.
        main_store_reps.to_excel(writer, sheet_name='MainStore Data', index=False)
        designer_store_reps.to_excel(writer, sheet_name='DesignerStore Data', index=False)
        lens_store_reps.to_excel(writer, sheet_name='Lens Data', index=False)
        control_reps.to_excel(writer, sheet_name='ControlRoom Data', index=False)
        packaging_reps.to_excel(writer, sheet_name='Packaging Data', index=False)


def floor_dt(dt, interval):
    replace = (dt.minute // interval)*interval
    return dt.replace(minute = replace, second=0, microsecond=0)

def smtp():
    load_dotenv()
    your_email = os.getenv("simon_email")
    password = os.getenv("simon_password")
    open_ITRs_file = f"{path}delayed_and_pending_ITRs/open_ITRs.xlsx"

    report_date = floor_dt(rn,15)

    cutoff_efficiency_full_sheet_names =  ["MainStore Data","DesignerStore Data","Lens Data","ControlRoom Data","Packaging Data"]
    cutoff_efficiency_full_dfs = []
    for sheet_name in cutoff_efficiency_full_sheet_names:
        df = pd.read_excel(open_ITRs_file, sheet_name=sheet_name)
        cutoff_efficiency_full_dfs.append(df)

    departments = ["Main store","Designer","Lens store","Control","Packaging"]
    for department in departments:
        table1heading = ""
        table1html = "" 
        table2heading = ""
        table2html = "" 
        department_email = ""
        if department == "Packaging":
            department_email = 'packaging@optica.africa'
            packaging_cutoff_full = cutoff_efficiency_full_dfs[4].rename(columns = {'Unnamed: 0':''})
            table1 = packaging_cutoff_full[packaging_cutoff_full["Classification"] == "Delayed"]
            table1 = table1.drop(["Delay","TenTo"], axis=1)
            
            if not table1.empty:  
                table1heading = "cut off delays"
                table1html = table1.to_html(index=False)
            table2 = packaging_cutoff_full[packaging_cutoff_full["Classification"] == "Within 15 Mins of Cut Off"]
            table2 = table2.drop(["Delay","TenTo"], axis=1)
            
            if not table2.empty:  
                table2heading = "15 minutes to cut off"
                table2html = table2.to_html(index=False)
        elif department == "Control":
            department_email = 'urvashi@optica.africa'
            control_cutoff_full = cutoff_efficiency_full_dfs[3].rename(columns = {'Unnamed: 0':''})
            table1 = control_cutoff_full[control_cutoff_full["Classification"] == "Delayed"]
            table1 = table1.drop(["Delay","TenTo"], axis=1)

            if not table1.empty:  
                table1heading = "cut off delays"
                table1html = table1.to_html(index=False)
            table2 = control_cutoff_full[control_cutoff_full["Classification"] == "Within 15 Mins of Cut Off"]
            table2 = table2.drop(["Delay","TenTo"], axis=1)

            if not table2.empty:  
                table2heading = "15 minutes to cut off"
                table2html = table2.to_html(index=False)

        elif department == "Lens store":
            department_email = 'stores@optica.africa'
            lens_cutoff_full = cutoff_efficiency_full_dfs[2].rename(columns = {'Unnamed: 0':''})
            table1 = lens_cutoff_full[lens_cutoff_full["Classification"] == "Delayed"]
            table1 = table1.drop(["Delay","TenTo"], axis=1)
            
            if not table1.empty:  
                table1heading = "cut off delays"
                table1html = table1.to_html(index=False)
            table2 = lens_cutoff_full[lens_cutoff_full["Classification"] == "Within 15 Mins of Cut Off"]
            table2 = table2.drop(["Delay","TenTo"], axis=1)
            
            if not table2.empty:  
                table2heading = "15 minutes to cut off"
                table2html = table2.to_html(index=False)
                
        elif department == "Designer":
            department_email = 'designer@optica.africa'
            designer_cutoff_full = cutoff_efficiency_full_dfs[1].rename(columns = {'Unnamed: 0':''})
            table1 = designer_cutoff_full[designer_cutoff_full["Classification"] == "Delayed"]
            table1 = table1.drop(["Delay","TenTo"], axis=1)
            
            if not table1.empty:  
                table1heading = "cut off delays"
                table1html = table1.to_html(index=False)    
            table2 = designer_cutoff_full[designer_cutoff_full["Classification"] == "Within 15 Mins of Cut Off"]
            table2 = table2.drop(["Delay","TenTo"], axis=1)
            
            if not table2.empty:  
                table2heading = "15 minutes to cut off"
                table2html = table2.to_html(index=False) 
        elif department == "Main store":
            department_email = 'mainstore@optica.africa'
            # department_email = 'wairimu@optica.africa','shyam@optica.africa' 
            main_cutoff_full = cutoff_efficiency_full_dfs[0].rename(columns = {'Unnamed: 0':''})
            table1 = main_cutoff_full[main_cutoff_full["Classification"] == "Delayed"]
            table1 = table1.drop(["Delay","TenTo"], axis=1)
            
            if not table1.empty:  
                table1heading = "cut off delays"
                table1html = table1.to_html(index=False)
            table2 = main_cutoff_full[main_cutoff_full["Classification"] == "Within 15 Mins of Cut Off"]
            table2 = table2.drop(["Delay","TenTo"], axis=1)
            
            if not table2.empty:  
                table2heading = "15 minutes to cut off"
                table2html = table2.to_html(index=False)
            
        # Define the HTML document
        if table1.empty and table2.empty:
            print(department + " empty")
            continue
        html = '''
            <html>
                <head>
                    <style>
                        table {{border-collapse: collapse;font-family:Times New Roman; font-size:12; width:auto;}}
                        th {{background-color:lightsalmon;}}
                        th, td {{text-align: center;font-family:Times New Roman; font-size:12; padding: 8px;}}
                        body, p, h3 {{font-family:Sans Serif}}
                    </style>
                </head>
                <body>
                    <p>Dear {departmemt},</p>
                        <p>Please work on this replacements.</p>
                        <p><u><b>{table1heading}</b></u></p>
                        <table>{table1html}</table>
                        <br>
                        <p><u><b>{table2heading}</b></u></p>
                        <table>{table2html}</table>
                        <br>
                </body>
            </html>
            '''.format(departmemt=department,table1html=table1html,table1heading = table1heading,
                    table2html=table2html,table2heading = table2heading)

    # Create a MIMEMultipart class, and set up the From, To, Subject fields
        # receiver_email = ["tstbranch@gmail.com"]
        receiver_email = [department_email,'john.kinyanjui@optica.africa','john.mwithiga@optica.africa','kelvin@optica.africa','shyam@optica.africa' ,'wairimu@optica.africa']

        email_message = MIMEMultipart()
        email_message['From'] = your_email
        email_message['To'] = ','.join(receiver_email)
        email_message['Subject'] = "Delayed and Pending Replacements for {department}, as of {report_date}".format(department = department, report_date = report_date)
        email_message.attach(MIMEText(html, "html"))
        # Convert it as a string
        email_string = email_message.as_string()

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(your_email, password)
            server.sendmail(
                your_email, 
                receiver_email, 
                email_string
            )
        print(department)

# manipulate_delayed_and_pending_ITRs()
if __name__ == '__main__':
    smtp()
        


