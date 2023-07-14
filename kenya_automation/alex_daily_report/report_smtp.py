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


def order_efficiency_smtp(): 
    #order efficiency
    order_efficiency =pd.ExcelFile(r"/home/opticabi/Documents/optica_reports/order_efficiency/order efficiency results.xlsx")  
    print(order_efficiency.sheet_names)
    departments = ["Main store","Designer","Lens store","Control","Packaging","BRS","Receiving"]
    order_sheet_names = departments[:5]
    order_efficiency_dfs = []
    for sheet_name in order_sheet_names:
        for start_row in [0, 9, 15]:
            df = pd.read_excel(order_efficiency, sheet_name=sheet_name, header=start_row)
            order_efficiency_dfs.append(df)
        
    order1 = order_efficiency_dfs[0].iloc[:4, :10]

    #cutoff efficiency
    cutoff_efficiency_summary = r"/home/opticabi/Documents/optica_reports/order_efficiency/Cutoff_Summary.xlsx"
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

    #Compiling data for each branch
    for department in departments:
        if department == "BRS":
            table1heading = "Cut Off Summary"
            table1 = cutoff_efficiency_summary_dfs[5].fillna("")
            table1html = table1.to_html(index=False,formatters={
    #                 'var1': '{:,.2f}'.format,
                    '%_ge Efficiency': '{:,.2%}'.format
                })
            brs_cutoff_full = cutoff_efficiency_full_dfs[5].rename(columns = {'Unnamed: 0':''})
            table2 = brs_cutoff_full[brs_cutoff_full["BRS CUT OFF"] == 0][['Item No.', 'Item/Service Description', 'ITR Status', 'ITR Number',
                                                                            'Internal Number', 'ITR Date', 'Exchange Type', 'From Warehouse Code',
                                                                            'Row Status', 'Document Status', 'Sales Order number',
                                                                            'Sales Order entry', 'Order Number', 'Draft order entry',
                                                                            'Sales Order branch', 'Creation Date', 'Creatn Time - Incl. Secs',
                                                                            'Picker Name', 'Name', 'Branch', 'Type', 'Max', 'Warehouse Name',
                                                                            'Address', 'BRS Cut', 'Lens Store Cut', 'Designer Store Cut',
                                                                            'Main Store Cut', 'Control Cut', 'Packaging Cut', 'Region']]
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
    #                 'var1': '{:,.2f}'.format,
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
    #                 'var1': '{:,.2f}'.format,
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
            
            # table7 = control_to_store_damge_supply
            # if not table7.empty:  
            #     table7heading = """
            #             <p style="color:blue">Damage Supply Time</p>
            #             <p>Rejected order sent to Control to reissued Lens for order</p>
            #             """
            #     table7html = table7.to_html(index=False)
                
            # table8 = store_to_control_damge_supply
            # if not table8.empty:
            #     table8heading = "Reissued order sent to Control to sent to prequality"
            #     table8html = table8.to_html(index=False)
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
    #                 'var1': '{:,.2f}'.format,
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
            # table8 = lensstore_awaiting
            # table8html = table8.to_html(index=False) 
            # table9 = damage_supply_dfs[2]
            # if not table9.empty:
            #     table9heading = """<p style="color:blue">Damage Supply Time</p>
            #                     <p style="font_weight:italic">Rejected frame sent to LensStore to reissued Lens for order</p>"""
            #     table9html = table9.to_html(index=False) 
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
    #                 'var1': '{:,.2f}'.format,
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
            # table8 = damage_supply_dfs[1]
            # if not table8.empty: 
            #     table8heading = """<p style="color:blue">Damage Supply Time</p>
            #                 <p style="font_weight:italic">Rejected frame sent to frame to reissued frame for order</p>"""
            #     table8html = table8.to_html(index=False)     
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
    #                 'var1': '{:,.2f}'.format,
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
            # table8 = damage_supply_dfs[0]
            # if not table8.empty:  
            #     table8heading = """<p style="color:blue">Damage Supply Time</p>
            #                 <p style="font_weight:italic">Rejected frame sent to frame to reissued frame for order</p>"""
            #     table8html = table8.to_html(index=False)

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
                       table8heading = table8heading)

    to_date = get_todate()
    # to_date = '2023-06-01'
    # till_date = '2023-06-30'
    sender_email = 'wairimu@optica.africa'
    receiver_email = 'wairimu@optica.africa'
    # receiver_email = ['wairimu@optica.africa','christopher@optica.africa']
    email_message = MIMEMultipart()
    email_message["From"] = sender_email
    email_message["To"] = r','.join(receiver_email)
    email_message["Subject"] = f"Insurance Desk and Approval's Rejections for {to_date}"
    email_message.attach(MIMEText(html, "html"))

    # # Open the Excel file and attach it to the email
    # with open('/home/opticabi/Documents/optica_reports/insurance_rejections/Insurance Rejections.xlsx', 'rb') as attachment:
    #     excel_file = MIMEApplication(attachment.read(), _subtype='xlsx')
    #     excel_file.add_header('Content-Disposition', 'attachment', filename='Kenya Insurance Rejections.xlsx')
    #     email_message.attach(excel_file)


    smtp_server = smtplib.SMTP("smtp.gmail.com", 587)
    smtp_server.starttls()
    smtp_server.login(sender_email, "maureen!!3636")
    text = email_message.as_string()
    smtp_server.sendmail(sender_email, receiver_email, text)
    smtp_server.quit()
if __name__ == '__main__': 
    order_efficiency_smtp()  

