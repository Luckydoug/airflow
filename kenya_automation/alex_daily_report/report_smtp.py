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

#Others
import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from sub_tasks.libraries.styles import styles, properties
from sub_tasks.libraries.utils import get_todate
from dateutil.relativedelta import relativedelta
from sub_tasks.libraries.utils import fetch_gsheet_data


def order_efficiency_smtp(): 
    departmentlist = fetch_gsheet_data()["department_emails"]
    print(departmentlist.head())
    deptemail = departmentlist['Email']
    emaillist = deptemail.tolist()

    #order efficiency
    order_efficiency =pd.ExcelFile(r"/home/opticabi/Documents/optica_reports/order_efficiency/order efficiency results.xlsx")  
    print(order_efficiency.sheet_names)
    departments = ["Main store","Designer","Lens store","Control","Packaging","BRS"]
    order_sheet_names = departments[:5]
    order_efficiency_dfs = []
    for sheet_name in order_sheet_names:
        for start_row in [0, 9, 15]:
            df = pd.read_excel(order_efficiency, sheet_name=sheet_name, header=start_row)
            order_efficiency_dfs.append(df)
        
    order1 = order_efficiency_dfs[0].iloc[:4, :10]


    #damage supply efficiency
    damage_supply_efficiency = r"/home/opticabi/Documents/optica_reports/order_efficiency\newdamagesupplytime.xlsx"
    damage_supply_sheet_names =  ["mainstore","designer","lensstore"]
    damage_supply_dfs = []
    for sheet_name in damage_supply_sheet_names:
        print(sheet_name)
        df = pd.read_excel(damage_supply_efficiency, sheet_name=sheet_name)
        damage_supply_dfs.append(df)
        
    damage_supply1 = damage_supply_dfs[0]

    #control_damge_supply
    control_to_store_damge_supply = pd.read_excel(damage_supply_efficiency, sheet_name="control to store")
    store_to_control_damge_supply = pd.read_excel(damage_supply_efficiency, sheet_name="store to control")
    departments = ["Main store","Designer","Lens store","Control","Packaging","BRS"]

    #lensstore awaiting efficiency
    lensstore_awaiting_efficiency = r"/home/opticabi/Documents/optica_reports/order_efficiency/lensstoreawaiting.xlsx"
    lensstore_awaiting_efficiency_sheet="Awaiting feedback"
    lensstore_awaiting = pd.read_excel(lensstore_awaiting_efficiency, sheet_name=lensstore_awaiting_efficiency_sheet)

    #cutoff efficiency
    cutoff_efficiency_summary = r"/home/opticabi/Documents/optica_reports/order_efficiency/Cutoff_Summary.xlsx"
    print(cutoff_efficiency_summary)
    cutoff_efficiency_full= r"/home/opticabi/Documents/optica_reports/order_efficiency/cutoff_Full_Report.xlsx"
    
    cutoff_efficiency_summary_sheet_names =  ["Main","Designer","Lens","Control","Packaging","BRS"]
    cutoff_efficiency_summary_dfs = []
    for sheet_name in cutoff_efficiency_summary_sheet_names:
        print(sheet_name)
        df = pd.read_excel(cutoff_efficiency_summary, sheet_name=sheet_name)
        cutoff_efficiency_summary_dfs.append(df)
        
    cutoff_efficiency_summary1 = cutoff_efficiency_summary_dfs[0]

    cutoff_efficiency_full_sheet_names =  ["MainStore Data","DesignerStore Data","Lens Data","ControlRoom Data","Packaging Data","BRS Data"]
    cutoff_efficiency_full_dfs = []
    for sheet_name in cutoff_efficiency_full_sheet_names:
        print(sheet_name)
        df = pd.read_excel(cutoff_efficiency_full, sheet_name=sheet_name)
        cutoff_efficiency_full_dfs.append(df)
        
    cutoff_efficiency_full1 = cutoff_efficiency_full_dfs[0]

    #replacements efficiency
    replacements_efficiency_summary = r"/home/opticabi/Documents/optica_reports/order_efficiency/Stock_Replacement_Efficiency_Summary.xlsx"
    replacements_efficiency_full = r"/home/opticabi/Documents/optica_reports/order_efficiency/Stock_Replacement_Efficiency_Full_Report.xlsx"

    replacements_efficiency_summary_sheet_names =  ["BRS Main","BRS Designer","BRS Lens","Main","Designer","Lens","Control","Packaging"]
    replacements_efficiency_summary_dfs = []
    for sheet_name in replacements_efficiency_summary_sheet_names:
        print(sheet_name)
        df = pd.read_excel(replacements_efficiency_summary, sheet_name=sheet_name)
        replacements_efficiency_summary_dfs.append(df)
        
    replacements_efficiency_summary1 = replacements_efficiency_summary_dfs[0]

    today = date.today()
    report_date = today
    # report_date = today - datetime.timedelta(days =2)
    if today.weekday() == 0:  # 0 means Monday
        report_date = (today - relativedelta(days=2)).strftime('%d-%m-%Y')
    else:
        report_date = (today - relativedelta(days=1)).strftime('%d-%m-%Y')

    i=0
    j=0

    #Compiling data for each branch
    for department, email in zip(departments,emaillist):
        if departmentlist.isin([email]).any().any():
            # dept = departmentlist['Department'].iloc[i]
            department = departmentlist['Department Name'].iloc[j]
        else:
            print("Does not exist") 
        i=i+1
        j=j+1

        table1heading = ""
        table1html = "" 
        table2heading = ""
        table2html = "" 
        table3heading = ""
        table3html = "" 
        table4heading = ""
        table4html = "" 
        table5heading = ""
        table5html = "" 
        table6heading = ""
        table6html = "" 
        table7heading = ""
        table7html = "" 
        table8heading = ""
        table8html = "" 
        table9heading = ""
        table9html = ""

        if department == "BRS":
            table1heading = "Cut Off Summary"
            table1 = cutoff_efficiency_summary_dfs[5].fillna("")
            table1html = table1.to_html(index=False,formatters={
            # 'var1': '{:,.2f}'.format,
                    '%_ge Efficiency': '{:,.2%}'.format
                })
            brs_cutoff_full = cutoff_efficiency_full_dfs[5].rename(columns = {'Unnamed: 0':''})
            table2 = brs_cutoff_full[brs_cutoff_full["BRS CUT OFF"] == 0][['Item No.', 'Item/Service Description', 'ITR Status',
                                                                            'ITR Number', 'Internal Number', 'ITR Date', 'Exchange Type',
                                                                            'Sales Order number', 'Sales Order entry', 'Sales Branch',
                                                                            'Draft order entry', 'Order Number', 'Creation Date',
                                                                            'Creatn Time - Incl. Secs', 'Picker Name', 'Branch', 'Type', 'Max',
                                                                            'Warehouse Name', 'Address', 'BRS Cut', 'Lens Store Cut',
                                                                            'Designer Store Cut', 'Main Store Cut', 'Control Cut', 'Packaging Cut',
                                                                            'Region', 'BRS CUT OFF', 'DEPARTMENT', 'DEPARTMENT 2']]
            if not table2.empty:  
                table2heading = "cut off delays"
                table2html = table2.to_html(index=False)
        elif department == "Packaging":
            table1heading = "Order Efficiency"
            table1 = order_efficiency_dfs[12].iloc[:4, :].rename(columns = {'Unnamed: 0':''})
            table1html = table1.to_html(index=False)
            table2heading = "Delays"
            table2 = order_efficiency_dfs[14].iloc[:, :3].fillna("")
            table2html = table2.to_html(index=False)
            table3heading = """
                        <p color:blue>Delayed orders with various cut off times</p>
                        <p>Total delayed orders after 15 minutes, 12 minutes, 8 minutes, 6 minutes</p>
                        """
            table3 = order_efficiency_dfs[13].iloc[:1, :5]
            table3html = table3.to_html(index=False)
            table4heading = "Cut Off Summary"
            table4 = cutoff_efficiency_summary_dfs[4].fillna("")
            table4html = table4.to_html(index=False,formatters={
            #   'var1': '{:,.2f}'.format,
                    '%_ge Efficiency': '{:,.2%}'.format
                })
            packaging_cutoff_full = cutoff_efficiency_full_dfs[4].rename(columns = {'Unnamed: 0':''})
            table5 = packaging_cutoff_full[packaging_cutoff_full["Packaging Cutoff"] == 0][["Region","Type","Internal Number",
                                                                                            "ITR Number","Created User",
                                                                                            "Status","Address","Warehouse Name",
                                                                                            "Branch","Date_Time","Date","Time",
                                                                                            "Packaging Cut","Max"]]
            if not table5.empty:  
                table5heading = "cut off delays"
                table5html = table5.to_html(index=False)
            table6heading = "Replacements"
            table6 = replacements_efficiency_summary_dfs[7]
            table6html = table6.to_html(index=False,float_format="{:.2f}".format) 
        elif department == "Control":
            table1heading = "Order Efficiency"
            table1 = order_efficiency_dfs[9].iloc[:4, :].rename(columns = {'Unnamed: 0':''})
            table1html = table1.to_html(index=False)
            table2heading = "Delays"
            table2 = order_efficiency_dfs[11].iloc[:, :3].fillna("")
            table2html = table2.to_html(index=False)
            table3heading = """
                        <p style="color:blue">Delayed orders with various cut off times</p>
                        <p>Total delayed orders after 15 minutes, 12 minutes, 10 minutes, 7 minutes</p>
                        """
            table3 = order_efficiency_dfs[10].iloc[:1, :5]
            table3html = table3.to_html(index=False)
            table4heading = "Cut Off Summary"
            table4 = cutoff_efficiency_summary_dfs[3].fillna("")
            table4html = table4.to_html(index=False,formatters={
            #  'var1': '{:,.2f}'.format,
                    '%_ge Efficiency': '{:,.2%}'.format
                })
            control_cutoff_full = cutoff_efficiency_full_dfs[3].rename(columns = {'Unnamed: 0':''})
            table5 = control_cutoff_full[control_cutoff_full["Control CutOFF"] == 0][["Region","Type","Internal Number","ITR Number",
                                                                                    "Created User","Status","Address","Warehouse Name",
                                                                                    "Branch","Date_Time","Date","Time","Control Cut",
                                                                                    "Max"]]
            
            if not table5.empty:  
                table5heading = "cut off delays"
                table5html = table5.to_html(index=False)
                
            table6heading = "Replacements"
            table6 = replacements_efficiency_summary_dfs[6]
            table6html = table6.to_html(index=False,float_format="{:.2f}".format)
            
            table7 = control_to_store_damge_supply
            if not table7.empty:  
                table7heading = """
                        <p style="color:blue">Damage Supply Time</p>
                        <p>Rejected order sent to Control to reissued Lens for order</p>
                        """
                table7html = table7.to_html(index=False)
                
            table8 = store_to_control_damge_supply
            if not table8.empty:
                table8heading = "Reissued order sent to Control to sent to prequality"
                table8html = table8.to_html(index=False)
        elif department == "Lens store":
            table1heading = "Order Efficiency"
            table1 = order_efficiency_dfs[6].iloc[:4, :].rename(columns = {'Unnamed: 0':''})
            table1html = table1.to_html(index=False)
            table2heading = "Delays"
            table2 = order_efficiency_dfs[8].iloc[:, :3].fillna("")
            table2html = table2.to_html(index=False)
            table3heading = """
                        <p style="color:blue">Delayed orders with various cut off times</p>
                        <p>Total delayed orders after 15 minutes, 12 minutes, 8 minutes, 6 minutes</p>
                        """
            table3 = order_efficiency_dfs[7].iloc[:1, :5].rename(columns = {'Unnamed: 0':''})
            table3html = table3.to_html(index=False)
            table4heading = "Cut Off Summary"
            table4 = cutoff_efficiency_summary_dfs[2].fillna("")
            table4html = table4.to_html(index=False,formatters={
            #    'var1': '{:,.2f}'.format,
                    '%_ge Efficiency': '{:,.2%}'.format
                })
            control_cutoff_full = cutoff_efficiency_full_dfs[2].rename(columns = {'Unnamed: 0':''})
            table5 = control_cutoff_full[control_cutoff_full["Lens Store CutOff"] == 0][["Region","Type","Internal Number","ITR Number",
                                                                                        "Created User","Status","Address","Warehouse Name",
                                                                                        "Branch","Date_Time","Date","Time",
                                                                                        "Lens Store Cut","Max"]]
            
            if not table5.empty:  
                table5heading = "cut off delays"
                table5html = table5.to_html(index=False)
            table6heading = """<p>Replacement</p>
                            <p style="font_weight:italic">BRSCreated_To_PLP Time</p>"""
            table6 = replacements_efficiency_summary_dfs[2]
            table6html = table6.to_html(index=False,float_format="{:.2f}".format) 
            table7heading = """<p style="font_weight:italic">PLP_To_STC Time</p>"""
            table7 = replacements_efficiency_summary_dfs[5]
            table7html = table7.to_html(index=False,float_format="{:.2f}".format)  
            table8heading = "Time taken from order printed to awaiting feedback"
            table8 = lensstore_awaiting
            table8html = table8.to_html(index=False) 
            table9 = damage_supply_dfs[2]
            if not table9.empty:
                table9heading = """<p style="color:blue">Damage Supply Time</p>
                                <p style="font_weight:italic">Rejected frame sent to LensStore to reissued Lens for order</p>"""
                table9html = table9.to_html(index=False) 
        elif department == "Designer":
            table1heading = "Order Efficiency"
            table1 = order_efficiency_dfs[3].iloc[:4, :].rename(columns = {'Unnamed: 0':''})
            table1html = table1.to_html(index=False)
            table2heading = "Delays"
            table2 = order_efficiency_dfs[5].iloc[:, :3].fillna("")
            table2html = table2.to_html(index=False)
            table3heading = """
                        <p style="color:blue">Delayed orders with various cut off times</p>
                        <p>Total delayed orders after 15 minutes, 12 minutes, 8 minutes, 6 minutes</p>
                        """
            table3 = order_efficiency_dfs[4].iloc[:1, :5]
            table3html = table3.to_html(index=False)
            table4heading = "Cut Off Summary"
            table4 = cutoff_efficiency_summary_dfs[1].fillna("")
            table4html = table4.to_html(index=False,formatters={
            #  'var1': '{:,.2f}'.format,
                    '%_ge Efficiency': '{:,.2%}'.format
                })
            control_cutoff_full = cutoff_efficiency_full_dfs[1].rename(columns = {'Unnamed: 0':''})
            table5 = control_cutoff_full[control_cutoff_full["Designer CUTOFF"] == 0][["Region","Type","Internal Number","ITR Number",
                                                                                    "Created User","Status","Address","Warehouse Name",
                                                                                    "Branch","Date_Time","Date","Time",
                                                                                    "Designer Store Cut","Max"]]
            
            if not table5.empty:  
                table5heading = "cut off delays"
                table5html = table5.to_html(index=False)
            table6heading = """<p>Replacement</p>
                            <p style="font_weight:italic">BRSCreated_To_PLP Time</p>"""
            table6 = replacements_efficiency_summary_dfs[1]
            table6html = table6.to_html(index=False,float_format="{:.2f}".format) 
            table7heading = """<p style="font_weight:italic">PLP_To_STC Time</p>"""
            table7 = replacements_efficiency_summary_dfs[4]
            table7html = table7.to_html(index=False,float_format="{:.2f}".format)
            table8 = damage_supply_dfs[1]
            if not table8.empty: 
                table8heading = """<p style="color:blue">Damage Supply Time</p>
                            <p style="font_weight:italic">Rejected frame sent to frame to reissued frame for order</p>"""
                table8html = table8.to_html(index=False)     
        elif department == "Main store":
            table1heading = "Order Efficiency"
            table1 = order_efficiency_dfs[0].iloc[:4, :].rename(columns = {'Unnamed: 0':''})
            table1html = table1.to_html(index=False)
            table2heading = "Delays"
            table2 = order_efficiency_dfs[2].iloc[:, :3].fillna("")
            table2html = table2.to_html(index=False)
            table3heading = """
                        <p style="color:blue">Delayed orders with various cut off times</p>
                        <p>Total delayed orders after 15 minutes, 12 minutes, 8 minutes, 6 minutes</p>
                        """
            table3 = order_efficiency_dfs[1].iloc[:1, :5]
            table3html = table3.to_html(index=False)
            table4heading = "Cut Off Summary"
            table4 = cutoff_efficiency_summary_dfs[0].fillna("")
            table4html = table4.to_html(index=False,formatters={
            #   'var1': '{:,.2f}'.format,
                    '%_ge Efficiency': '{:,.2%}'.format
                })
            control_cutoff_full = cutoff_efficiency_full_dfs[0].rename(columns = {'Unnamed: 0':''})
            table5 = control_cutoff_full[control_cutoff_full["Main CUTOFF"] == 0][["Region","Type","Internal Number","ITR Number",
                                                                                "Created User","Status","Address","Warehouse Name",
                                                                                "Branch","Date_Time","Date","Time","Main Store Cut",
                                                                                "Max"]]
            
            if not table5.empty:  
                table5heading = "cut off delays"
                table5html = table5.to_html(index=False)
            table6heading = """<p>Replacement</p>
                            <p style="font_weight:italic">BRSCreated_To_PLP Time</p>"""
            table6 = replacements_efficiency_summary_dfs[0]
            table6html = table6.to_html(index=False,float_format="{:.2f}".format) 
            table7heading = """<p style="font_weight:italic">PLP_To_STC Time</p>"""
            table7 = replacements_efficiency_summary_dfs[3]
            table7html = table7.to_html(index=False,float_format="{:.2f}".format)  
            table8 = damage_supply_dfs[0]
            if not table8.empty:  
                table8heading = """<p style="color:blue">Damage Supply Time</p>
                            <p style="font_weight:italic">Rejected frame sent to frame to reissued frame for order</p>"""
                table8html = table8.to_html(index=False)

        # Define the HTML document
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
                        <p>View your performance report for the stated date.</p>
                        <p><u><b>{table1heading}</b></u></p>
                        <table>{table1html}</table>
                        <br>
                        <p><u><b>{table2heading}</b></u></p>
                        <table>{table2html}</table>
                        <br>
                        <p><u><b>{table3heading}</b></u></p>
                        <table>{table3html}</table>
                        <br>
                        <p><u><b>{table4heading}</b></u></p>
                        <table>{table4html}</table>
                        <br>
                        <p><u><b>{table5heading}</b></u></p>
                        <table>{table5html}</table>
                        <br>
                        <p><u><b>{table6heading}</b></u></p>
                        <table>{table6html}</table>
                        <br>
                        <p><u><b>{table7heading}</b></u></p>
                        <table>{table7html}</table>
                        <br>
                        <!-- <p><u><b>{table8heading}</b></u></p>
                        <table>{table8html}</table>
                        <br>
                        <p><u><b>{table9heading}</b></u></p>
                        <table>{table9html}</table> -->
                        <br>
                </body>
            </html>
            '''.format(departmemt=department,table1html=table1html,table1heading = table1heading,
                table2html=table2html,table2heading = table2heading,table3html=table3html,table3heading = table3heading,
                table4html=table4html,table4heading = table4heading,table5html=table5html,table5heading = table5heading,
                table6html=table6html,table6heading = table6heading,table7html=table7html,table7heading = table7heading,
                table8html=table8html,table8heading = table8heading,table9html=table9html,table9heading = table9heading)

            # Define a function to attach files as MIMEApplication to the email
            ##############################################################
        def attach_file_to_email(email_message, filename):
            # Open the attachment file for reading in binary mode, and make it a MIMEApplication class
            with open(filename, "rb") as f:
                file_attachment = MIMEApplication(f.read())
            # Add header/name to the attachments    
            file_attachment.add_header(
                "Content-Disposition",
                f"attachment; filename= {filename}",
            )
            # Attach the file to the message
            email_message.attach(file_attachment)
        ##############################################################
            
        # Set up the email addresses and password. Please replace below with your email address and password
        sender_email = 'wairimu@optica.africa'
        yourpassword = 'maureen!!3636'
        email_from = sender_email
        password = yourpassword

        # receiver_email = ['tstbranch@gmail.com']
        receiver_email = [email,'john.kinyanjui@optica.africa','john.mwithiga@optica.africa','kelvin@optica.africa','nicholas.muthui@optica.africa','simon.peter@optica.africa','shyam@optica.africa']

        # Create a MIMEMultipart class, and set up the From, To, Subject fields
        email_message = MIMEMultipart()
        email_message['From'] = sender_email
        email_message['To'] = ','.join(receiver_email)

        email_message['Subject'] = "Efficiency Report for {department},{report_date}".format(department = department, report_date = report_date)

        # Attach the html doc defined earlier, as a MIMEText html content type to the MIME message
        email_message.attach(MIMEText(html, "html"))

        smtp_server = smtplib.SMTP("smtp.gmail.com", 587)
        smtp_server.starttls()
        smtp_server.login(sender_email, "maureen!!3636")
        text = email_message.as_string()
        smtp_server.sendmail(sender_email, receiver_email, text)
        smtp_server.quit()

if __name__ == '__main__': 
    order_efficiency_smtp()  

