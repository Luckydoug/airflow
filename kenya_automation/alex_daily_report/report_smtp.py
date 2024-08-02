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
    departments = ["Main store","Designer","Lens store","Control","Packaging","BRS","BRS_UG","BRS_RW"]
    order_sheet_names = departments[:5]
    order_efficiency_dfs = []
    for sheet_name in order_sheet_names:
        for start_row in [0, 9, 15]:
            df = pd.read_excel(order_efficiency, sheet_name=sheet_name, header=start_row)
            order_efficiency_dfs.append(df)
        
    order1 = order_efficiency_dfs[0].iloc[:4, :10]

    maine = order_efficiency_dfs[0].iloc[:4, :].rename(columns = {'Unnamed: 0':''})
    mainefficiency = pd.to_numeric((maine['Total'].iloc[-1]).replace("%",""))
    deignere = order_efficiency_dfs[3].iloc[:4, :].rename(columns = {'Unnamed: 0':''})
    designerefficiency = pd.to_numeric((deignere['Total'].iloc[-1]).replace("%",""))
    lense = order_efficiency_dfs[6].iloc[:4, :].rename(columns = {'Unnamed: 0':''})
    lensstoreefficiency = pd.to_numeric((lense['Total'].iloc[-1]).replace("%",""))
    packe = order_efficiency_dfs[12].iloc[:4, :].rename(columns = {'Unnamed: 0':''})
    packagingefficiency = pd.to_numeric((packe['Total'].iloc[-1]).replace("%",""))
    controle = order_efficiency_dfs[9].iloc[:4, :].rename(columns = {'Unnamed: 0':''})
    controlefficiency = pd.to_numeric((controle['Total'].iloc[-1]).replace("%",""))

    #saleorder_to_orderprinted
    path_maindesignerbreakdown = r"/home/opticabi/Documents/optica_reports/order_efficiency/maindesignerbreakdown.xlsx"
    so_to_op_main = pd.read_excel(path_maindesignerbreakdown,sheet_name='so_to_op_main')
    op_to_lenstore_main = pd.read_excel(path_maindesignerbreakdown,sheet_name='op_to_lenstore_main')
    so_to_op_designer = pd.read_excel(path_maindesignerbreakdown,sheet_name='so_to_op_designer')
    op_to_lenstore_designer = pd.read_excel(path_maindesignerbreakdown,sheet_name='op_to_lenstore_designer')

    #damage supply efficiency
    damage_supply_efficiency = r"/home/opticabi/Documents/optica_reports/order_efficiency\newdamagesupplytime.xlsx"
    damage_supply_sheet_names =  ["mainstore","designer","lensstore","mainstoresec","lensstoresec"]
    damage_supply_dfs = []
    for sheet_name in damage_supply_sheet_names:
        print(sheet_name)
        df = pd.read_excel(damage_supply_efficiency, sheet_name=sheet_name)
        damage_supply_dfs.append(df)
    

    #control_damge_supply
    control_to_store_damge_supply = pd.read_excel(damage_supply_efficiency, sheet_name="control to store")
    store_to_control_damge_supply = pd.read_excel(damage_supply_efficiency, sheet_name="store to control")
    departments = ["Main store","Designer","Lens store","Control","Packaging","BRS","BRS_UG","BRS_RW"]

    #lensstore awaiting efficiency
    lensstore_awaiting_efficiency = r"/home/opticabi/Documents/optica_reports/order_efficiency/lensstoreawaiting.xlsx"
    lensstore_awaiting_efficiency_sheet="Awaiting feedback"
    lensstore_awaiting = pd.read_excel(lensstore_awaiting_efficiency, sheet_name=lensstore_awaiting_efficiency_sheet)

    #receiving to lenstore
    receiving_lenstore = pd.read_excel(r"/home/opticabi/Documents/optica_reports/order_efficiency/lensstoreawaiting.xlsx",sheet_name='From Receiving')

    #cutoff efficiency
    cutoff_efficiency_summary = r"/home/opticabi/Documents/optica_reports/order_efficiency/Cutoff_Summary.xlsx"
    cutoff_efficiency_full= r"/home/opticabi/Documents/optica_reports/order_efficiency/cutoff_Full_Report.xlsx"
    
    cutoff_efficiency_summary_sheet_names =  ["Main","Designer","Lens","Control","Packaging","BRS","BRS_UG","BRS_RW"]
    cutoff_efficiency_summary_dfs = []
    for sheet_name in cutoff_efficiency_summary_sheet_names:
        print(sheet_name)
        df = pd.read_excel(cutoff_efficiency_summary, sheet_name=sheet_name)
        cutoff_efficiency_summary_dfs.append(df)
        
    cutoff_efficiency_summary1 = cutoff_efficiency_summary_dfs[0]

    cutoff_efficiency_full_sheet_names =  ["MainStore Data","DesignerStore Data","Lens Data","ControlRoom Data","Packaging Data","BRS Data","BRS Data UG","BRS Data RW"]
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
    if today.weekday() == 0:  # 0 means Monday
        report_date = (today - relativedelta(days=2)).strftime('%d-%m-%Y')
    else:
        report_date = (today - relativedelta(days=1)).strftime('%d-%m-%Y')

    i=0
    j=0

    #Compiling data for each department
    for department, email in zip(departments,emaillist):
        print(department)
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
        table10heading = ""
        table10html = ""
        table11heading = ""
        table11html = ""

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

            table3heading = "Cut Off Summary - Uganda"
            if not cutoff_efficiency_summary_dfs[6].empty and len(cutoff_efficiency_summary_dfs[6]) > 0:
                table3 = cutoff_efficiency_summary_dfs[6].fillna("")
                table3html = table3.to_html(index=False,formatters={'%_ge Efficiency': '{:,.2%}'.format})
            else:
                table3html = """<p style = "color: purple;"> No Replacements were done for Rwanda</p>"""  

            if not cutoff_efficiency_full_dfs[6].empty and len(cutoff_efficiency_full_dfs[6]) > 0:
                brs_cutoff_full_ug = cutoff_efficiency_full_dfs[6].rename(columns = {'Unnamed: 0':''})
                table4 = brs_cutoff_full_ug[brs_cutoff_full_ug["BRS CUT OFF"] == 0][['Item No.', 'Item/Service Description', 'ITR Status',
                                                                                'ITR Number', 'Internal Number', 'ITR Date', 'Exchange Type',
                                                                                'Sales Order number', 'Sales Order entry', 'Sales Branch',
                                                                                'Draft order entry', 'Order Number', 'Creation Date',
                                                                                'Creatn Time - Incl. Secs', 'Picker Name', 'Branch', 'Type', 'Max',
                                                                                'Warehouse Name', 'Address', 'BRS Cut', 'Lens Store Cut',
                                                                                'Designer Store Cut', 'Main Store Cut', 'Control Cut', 'Packaging Cut',
                                                                                'Region', 'BRS CUT OFF', 'DEPARTMENT', 'DEPARTMENT 2']]
                if not table4.empty:  
                    table4heading = "cut off delays"
                    table4html = table4.to_html(index=False)

            table5heading = "Cut Off Summary - Rwanda"
            if not cutoff_efficiency_summary_dfs[7].empty and len(cutoff_efficiency_summary_dfs[7]) > 0:
                table5 = cutoff_efficiency_summary_dfs[7].fillna("")
                table5html = table5.to_html(index=False,formatters={'%_ge Efficiency': '{:,.2%}'.format})
            else:
                table5html = """<p style = "color: purple;"> No Replacements were done for Rwanda</p>"""   

            if not cutoff_efficiency_full_dfs[7].empty and len(cutoff_efficiency_full_dfs[7]) > 0:
                brs_cutoff_full_rw = cutoff_efficiency_full_dfs[7].rename(columns={'Unnamed: 0': ''})
                table6 = brs_cutoff_full_rw[brs_cutoff_full_rw["BRS CUT OFF"] == 0][[
                    'Item No.', 'Item/Service Description', 'ITR Status', 'ITR Number', 'Internal Number', 'ITR Date', 
                    'Exchange Type', 'Sales Order number', 'Sales Order entry', 'Sales Branch', 'Draft order entry', 
                    'Order Number', 'Creation Date', 'Creatn Time - Incl. Secs', 'Picker Name', 'Branch', 'Type', 
                    'Max', 'Warehouse Name', 'Address', 'BRS Cut', 'Lens Store Cut', 'Designer Store Cut', 
                    'Main Store Cut', 'Control Cut', 'Packaging Cut', 'Region', 'BRS CUT OFF', 'DEPARTMENT', 
                    'DEPARTMENT 2'
                ]]
                table6heading = "cut off delays"
                table6html = table6.to_html(index=False)         
                   
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
                        <p>Rejected order sent to Control to Rejected Lenses sent to Lens Store</p>
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
            table10 = damage_supply_dfs[3]
            if not table10.empty:  
                table10heading = """<p style="color:blue">Second Damage Supply Time</p>
                            <p style="font_weight:italic">Rejected frame sent to frame to reissued frame for order</p>"""
                table10html = table10.to_html(index=False) 
            table11heading = "Sent to LensStore to Received by LensStore"
            table11html = receiving_lenstore.to_html(index=False)
        elif department == "Designer":
            table1heading = "Order Efficiency"
            table1 = order_efficiency_dfs[3].iloc[:4, :].rename(columns = {'Unnamed: 0':''})
            table1html = table1.to_html(index=False)
            table2heading = "Sales Order to Order Printed"
            table2html = so_to_op_designer.to_html(index=False)
            table3heading = "Order Printed to Sent to Lens Store"
            table3html = op_to_lenstore_designer.to_html(index=False)
            table4heading = "Delays"
            table4 = order_efficiency_dfs[5].iloc[:, :3].fillna("")
            table4html = table4.to_html(index=False)
            table5heading = """
                        <p style="color:blue">Delayed orders with various cut off times</p>
                        <p>Total delayed orders after 15 minutes, 12 minutes, 8 minutes, 6 minutes</p>
                        """
            table5 = order_efficiency_dfs[4].iloc[:1, :5]
            table5html = table5.to_html(index=False) 
            table6heading = "Cut Off Summary"
            table6 = cutoff_efficiency_summary_dfs[1].fillna("")
            table6html = table6.to_html(index=False,formatters={
                    '%_ge Efficiency': '{:,.2%}'.format
                })
            control_cutoff_full = cutoff_efficiency_full_dfs[1].rename(columns = {'Unnamed: 0':''})
            table7 = control_cutoff_full[control_cutoff_full["Designer CUTOFF"] == 0][["Region","Type","Internal Number","ITR Number",
                                                                                    "Created User","Status","Address","Warehouse Name",
                                                                                    "Branch","Date_Time","Date","Time",
                                                                                    "Designer Store Cut","Max"]]
            
            if not table7.empty:  
                table7heading = "cut off delays"
                table7html = table7.to_html(index=False)
            table8heading = """<p>Replacement</p>
                            <p style="font_weight:italic">BRSCreated_To_PLP Time</p>"""
            table8 = replacements_efficiency_summary_dfs[1]
            table8html = table8.to_html(index=False,float_format="{:.2f}".format) 
            table9heading = """<p style="font_weight:italic">PLP_To_STC Time</p>"""
            table9 = replacements_efficiency_summary_dfs[4]
            table9html = table9.to_html(index=False,float_format="{:.2f}".format)
            table10 = damage_supply_dfs[1]
            if not table10.empty: 
                table10heading = """<p style="color:blue">Damage Supply Time</p>
                            <p style="font_weight:italic">Rejected frame sent to frame to reissued frame for order</p>"""
                table10html = table10.to_html(index=False)     
        elif department == "Main store":
            table1heading = "Order Efficiency"
            table1 = order_efficiency_dfs[0].iloc[:4, :].rename(columns = {'Unnamed: 0':''})
            table1html = table1.to_html(index=False)
            table2heading = "Sales Order to Order Printed"
            table2html = so_to_op_main.to_html(index=False)
            table3heading = "Order Printed to Sent to Lens Store"
            table3html = op_to_lenstore_main.to_html(index=False)
            table4heading = "Delays"
            table4 = order_efficiency_dfs[2].iloc[:, :3].fillna("")
            table4html = table4.to_html(index=False)
            table5heading = """
                        <p style="color:blue">Delayed orders with various cut off times</p>
                        <p>Total delayed orders after 15 minutes, 12 minutes, 8 minutes, 6 minutes</p>
                        """
            table5 = order_efficiency_dfs[1].iloc[:1, :5]
            table5html = table5.to_html(index=False)
            table6heading = "Cut Off Summary"
            table6 = cutoff_efficiency_summary_dfs[0].fillna("")
            table6html = table6.to_html(index=False,formatters={
                    '%_ge Efficiency': '{:,.2%}'.format
                })
            control_cutoff_full = cutoff_efficiency_full_dfs[0].rename(columns = {'Unnamed: 0':''})
            table7 = control_cutoff_full[control_cutoff_full["Main CUTOFF"] == 0][["Region","Type","Internal Number","ITR Number",
                                                                                "Created User","Status","Address","Warehouse Name",
                                                                                "Branch","Date_Time","Date","Time","Main Store Cut",
                                                                                "Max"]]
            
            if not table7.empty:  
                table7heading = "cut off delays"
                table7html = table7.to_html(index=False)
            table8heading = """<p>Replacement</p>
                            <p style="font_weight:italic">BRSCreated_To_PLP Time</p>"""
            table8 = replacements_efficiency_summary_dfs[0]
            table8html = table8.to_html(index=False,float_format="{:.2f}".format) 
            table9heading = """<p style="font_weight:italic">PLP_To_STC Time</p>"""
            table9 = replacements_efficiency_summary_dfs[3]
            table9html = table9.to_html(index=False,float_format="{:.2f}".format)  
            table10 = damage_supply_dfs[0]
            if not table10.empty:  
                table10heading = """<p style="color:blue">First Damage Supply Time</p>
                            <p style="font_weight:italic">Rejected frame sent to frame to reissued frame for order</p>"""
                table10html = table10.to_html(index=False)
            table11 = damage_supply_dfs[3]
            if not table11.empty:  
                table11heading = """<p style="color:blue">Second Damage Supply Time</p>
                            <p style="font_weight:italic">Rejected frame sent to frame to reissued frame for order</p>"""
                table11html = table11.to_html(index=False)    
                

        html = '''
            <html>
                <head>
                    <style>
                        table {{border-collapse: collapse;font-family:Times New Roman; font-size:12; width:auto;}}
                        th {{background-color:lightblue;}}
                        th, td {{text-align: center;font-family:Times New Roman; font-size:12; padding: 8px;}}
                        body, p, h3 {{font-family:Sans Serif}}
                    </style>
                </head>
                <body>
                    <p>Dear {departmemt},</p>
                        <p>View your performance report for the stated date.</p>
                        <p>Kindly comment on your performance. </p>
                        <p><u><b>{table1heading}</b></u></p>
                        <table>{table1html}</table>
                        <p><u><b>{table2heading}</b></u></p>
                        <table>{table2html}</table>
                        <p><u><b>{table3heading}</b></u></p>
                        <table>{table3html}</table>
                        <p><u><b>{table11heading}</b></u></p>
                        <table>{table11html}</table>
                        <p><u><b>{table4heading}</b></u></p>
                        <table>{table4html}</table>
                        <p><u><b>{table5heading}</b></u></p>
                        <table>{table5html}</table>
                        <p><u><b>{table6heading}</b></u></p>
                        <table>{table6html}</table>
                        <p><u><b>{table7heading}</b></u></p>
                        <table>{table7html}</table>
                        <p><u><b>{table8heading}</b></u></p>
                        <table>{table8html}</table>
                        <p><u><b>{table9heading}</b></u></p>
                        <table>{table9html}</table>
                        <p><u><b>{table10heading}</b></u></p>
                        <table>{table10html}</table>
                </body>
            </html>
            '''.format(departmemt=department,table1html=table1html,table1heading = table1heading,
                table2html=table2html,table2heading = table2heading,table3html=table3html,table3heading = table3heading,
                table4html=table4html,table4heading = table4heading,table5html=table5html,table5heading = table5heading,
                table6html=table6html,table6heading = table6heading,table7html=table7html,table7heading = table7heading,
                table8html=table8html,table8heading = table8heading,table9html=table9html,table9heading = table9heading,
                table10html = table10html,table10heading = table10heading, table11heading=table11heading,table11html=table11html)


        def attach_file_to_email(email_message, filename):
            with open(filename, "rb") as f:
                file_attachment = MIMEApplication(f.read())   
            file_attachment.add_header(
                "Content-Disposition",
                f"attachment; filename= {filename}",
            )
            email_message.attach(file_attachment)
            
        sender_email = os.getenv("mulei_email")
        yourpassword = os.getenv("mulei_password")
        email_from = sender_email
        password = yourpassword

        # receiver_email = ['tstbranch@gmail.com']

        if department == "BRS":           
            receiver_email = [email,'john.kinyanjui@optica.africa','john.mwithiga@optica.africa','kelvin@optica.africa','shyam@optica.africa','stock@optica.africa','mulei.mutuku@optica.africa','nicholas.muthui@optica.africa ']
           
        else:    
            if pd.to_numeric((table1['Total'].iloc[-1]).replace("%","")) < 95:
                receiver_email = [email,'john.kinyanjui@optica.africa','john.mwithiga@optica.africa','kelvin@optica.africa','shyam@optica.africa','mulei.mutuku@optica.africa','nicholas.muthui@optica.africa ']
            else:
                receiver_email = [email,'john.kinyanjui@optica.africa','john.mwithiga@optica.africa','kelvin@optica.africa','shyam@optica.africa','mulei.mutuku@optica.africa','nicholas.muthui@optica.africa ']


        email_message = MIMEMultipart()
        email_message['From'] = sender_email
        email_message['To'] = ','.join(receiver_email)

        email_message['Subject'] = "Efficiency Report for {department},{report_date}".format(department = department, report_date = report_date)

        email_message.attach(MIMEText(html, "html"))

        smtp_server = smtplib.SMTP("smtp.gmail.com", 587)
        smtp_server.starttls()
        smtp_server.login(sender_email, os.getenv("mulei_password"))
        text = email_message.as_string()
        smtp_server.sendmail(sender_email, receiver_email, text)
        smtp_server.quit()

if __name__ == '__main__': 
    order_efficiency_smtp()  

# def clean_folder(dir_name=r"/home/opticabi/Documents/optica_reports/"):
#     files = os.listdir(dir_name)
#     for file in files:
#         if file.endswith(".xlsx"):
#             os.remove(os.path.join(dir_name, file))

# clean_folder()