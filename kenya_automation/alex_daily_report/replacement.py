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

##Others
import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from sub_tasks.libraries.styles import styles, properties

# PG Execute(Query)
from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.api_login.api_login import(login)
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")


"""We shall recreate the query :3rd floor  all user Replacement & Replacement ITR over date range to distinguish between branch stock and stock borrow"""

##Define the days that is yesterday
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
formatted_date = yesterday.strftime('%Y-%m-%d')


def replacements():
    """Replacement ITR over date range to distinguish between branch stock and stock borrow"""
    Repdata = """select * 
                    from 
                    (select sid.replaced_itemcode as "Item No.",sid.itr__status as "ITR Status",si.doc_no as "ITR Number",si.internal_no as "Internal Number",si.post_date as "ITR Date",
                    si.exchange_type as "Exchange Type",si.to_warehouse_code as "To Warehouse Code",sid.sales_orderno as "Sales Order number",sid.sales_order_entry as "Sales Order entry",
                    sid.sales_order_branch as "Sales Branch",sid.draft_order_entry as "Draft order entry",sid.draft_orderno as "Order Number",si.createdon as "Creation Date",
                    si.creationtime_incl_secs as "Creatn Time - Incl. Secs",sid.picker_name as "Picker Name"
                    from mabawa_staging.source_itr_details sid
                    inner join mabawa_staging.source_itr si on si.internal_no = sid.doc_internal_id 
                    where si.exchange_type = 'Replacement') as t
                    where "ITR Date"::date between '{yesterday}' and '{yesterday}'
                """.format(yesterday=yesterday)
    
    BRS = pd.read_sql_query(Repdata,con=conn) 
    print(BRS)

    """3rd floor  all user Replacement"""
    Replacements = """select created_user as "Created User",post_date as "Date",post_time,
                        to_char(post_time, 'FM999:09:99'::text)::time without time zone AS "Time", status as "Status", item_code as "Item Code", itr_no as "ITR No" 
                        from mabawa_staging.source_itr_log sil 
                        where post_date::date between '{yesterday}' and '{yesterday}'
                    """.format(yesterday=yesterday)
    
    Replacements = pd.read_sql_query(Replacements,con=conn)
    print(Replacements)

    """ REPLACEMENT EFFICIENCY""" 
    Replacements['Date']=pd.to_datetime(Replacements['Date'],dayfirst=True ).dt.date
    Replacements["Date_Time"]=pd.to_datetime(Replacements.Date.astype(str) + ' ' + Replacements.Time.astype(str), format="%Y%m%d %H:%M:%S")
    Replacements['Day_End'] = Replacements["Date_Time"].dt.day_name()
    Replacements=Replacements[['Created User', 'Status', 'Item Code', 'ITR No','Day_End','Date_Time']]


    main1=Replacements[Replacements["Created User"]=="main1"]
    main2=Replacements[Replacements["Created User"]=="main2"]

    lens1=Replacements[Replacements["Created User"]=="lens1"]
    lens2=Replacements[Replacements["Created User"]=="lens2"]

    designer1=Replacements[Replacements["Created User"]=="designer1"]
    designer2=Replacements[Replacements["Created User"]=="designer2"]

    control1=Replacements[Replacements["Created User"]=="control1"]
    control2=Replacements[Replacements["Created User"]=="control2"]
    control3=Replacements[Replacements["Created User"]=="control3"]

    packaging1=Replacements[Replacements["Created User"]=="packaging1"]
    packaging2=Replacements[Replacements["Created User"]=="packaging2"]


    BRS["Creation Date"]=pd.to_datetime(BRS["Creation Date"],dayfirst=True ).dt.date
    BRS["Creatn Time - Incl. Secs"]=pd.to_datetime(BRS["Creatn Time - Incl. Secs"],format= '%H%M%S' ).dt.time

    """
    Part 1: Replacements Efficieny amid Different Departments 
    1.Getting data with Status "Picklist Created">>>SAP "Replacement ITR over date range to distinguish between branch stock and stock borrow
    """
    BRS=BRS.rename(columns={"Creatn Time - Incl. Secs":"Time","Creation Date":"CreatedDate","Internal Number":"ITR No"})
    BRSCreated=BRS
    BRSCreated["BRSCreatedDate"]=pd.to_datetime(BRSCreated.CreatedDate.astype(str) + ' ' + BRSCreated.Time.astype(str), format="%Y%m%d %H:%M:%S")
    BRSCreated['Day of the Week'] = BRSCreated["BRSCreatedDate"].dt.day_name()
    
    """Total Number of Items per ITR"""
    Total_Number=pd.pivot_table(BRSCreated,values="ITR No",index="ITR Number",aggfunc=np.count_nonzero)
    BRSCreated=pd.merge(BRSCreated,Total_Number,on="ITR Number",how="left")
    BRSCreatedData=BRSCreated.rename(columns={"ITR No_y":"No. of Items","ITR No_x":"ITR No"}).drop_duplicates(subset=['ITR No'],keep="first", inplace=False) 
    BRSCreatedData['ITR No'] = BRSCreatedData['ITR No'].astype(int)

    """2.Getting data with Status "Picklist Printed" """
    PicklistPrinted=pd.concat([main2,main1,designer1,designer2,lens1,lens2])
    PicklistPrinted=PicklistPrinted[PicklistPrinted.Status=="Pick List Printed"]
    PicklistPrinted=PicklistPrinted.drop_duplicates(subset=['ITR No'],keep="first", inplace=False) 
    PicklistPrinted['ITR No'] = PicklistPrinted['ITR No'].astype(int)

    """Merging Picklist Printed to PickList Created"""
    PickListCreatedTOPrinted=pd.merge(BRSCreatedData,PicklistPrinted,on="ITR No",how="outer")
    PickListCreatedTOPrintedData=PickListCreatedTOPrinted[["ITR Number","ITR No","No. of Items","Created User","Day_End","BRSCreatedDate","Date_Time"]]
    PickListCreatedTOPrintedData=PickListCreatedTOPrintedData.rename(columns={"Date_Time":"PLP_DateTime","Day_End":"PLP_Day"})

    """ 3.Getting data with Status "Rep Sent to Control Room" """
    SenttoControl=pd.concat([main2,main1,designer1,designer2,lens1,lens2])
    SenttoControl=SenttoControl[SenttoControl.Status=="Rep Sent to Control Room"]
    SenttoControl=SenttoControl.drop_duplicates(subset=['ITR No'],keep="first", inplace=False) 
    SenttoControl['ITR No'] = SenttoControl['ITR No'].astype(int)

    """"Merging Picklist Printed to Sent to Control """
    MasterData_Stores=pd.merge(PickListCreatedTOPrintedData,SenttoControl,on="ITR No",how="outer")
    MasterData_Stores=MasterData_Stores.rename(columns={"Created User_y":"StoresCreatedUser",'Day_End':'STC_Day','Date_Time':'STC_DateTime'})
    MasterData_Stores=MasterData_Stores[['ITR Number', 'ITR No', 'No. of Items', 'PLP_Day', 'BRSCreatedDate', 'PLP_DateTime',
       "StoresCreatedUser", 'Status', 'Item Code', 'STC_Day', 'STC_DateTime']]
    
    """ 4.Getting data with Status "Rep Sent to Packaging" """
    SenttoPackaging=pd.concat([control1,control2,control3])
    SenttoPackaging=SenttoPackaging[SenttoPackaging.Status=="Rep Sent to Packaging"]
    SenttoPackaging=SenttoPackaging.drop_duplicates(subset=['ITR No'],keep="first", inplace=False)
    SenttoPackaging['ITR No'] = SenttoPackaging['ITR No'].astype(int)

    MasterData=pd.merge(MasterData_Stores,SenttoPackaging,on="ITR No",how="outer")
    MasterData=MasterData.rename(columns={"Status_x":"STC_Status","Status_y":"STP_Status",'Day_End':"STP_Day",'Date_Time':"STP_DateTime","Created User":"STP_User"})
    MasterData=MasterData[['ITR Number', 'ITR No', 'No. of Items','PLP_Day', 'BRSCreatedDate', 'PLP_DateTime', 'StoresCreatedUser','STC_Status', 'STC_Day', 'STC_DateTime', "STP_User",'STP_Status', 'STP_Day', 'STP_DateTime']]


    """ 5.Getting data with Status "Rep Sent to Branch" """
    SenttoBranch=pd.concat([packaging1,packaging2])
    SenttoBranch=SenttoBranch[SenttoBranch.Status=="Rep Sent to Branch"]
    SenttoBranch=SenttoBranch.drop_duplicates(subset=['ITR No'],keep="first", inplace=False)
    SenttoBranch['ITR No'] = SenttoBranch['ITR No'].astype(int)

    Master_Data=pd.merge(MasterData,SenttoBranch,on="ITR No",how="outer")
    Master_Data=Master_Data.rename(columns={'Status':"STB_Status",'Day_End':"STB_Day",'Date_Time':"STB_DateTime","Created User":"STB_User"})
    MasterData=Master_Data[['ITR Number', 'ITR No', 'No. of Items','PLP_Day', 'StoresCreatedUser','STC_Status', 'STC_Day', 'STP_User', 'STP_Status','STP_Day',  'STB_User', 'STB_Status', 'Item Code','STB_Day', 'BRSCreatedDate','PLP_DateTime','STC_DateTime','STP_DateTime','STB_DateTime']]



    """ Calculating Duration Taken Between the Statuses """
    ####Days of the week
    workday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(18),
        working_days=[0, 1, 2, 3, 4])

    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    businesshrs = businesstimedelta.Rules([workday, holidays])

    def BusHrs(start, end):
        if end>=start:
            return businesshrs.difference(start,end).hours+float(businesshrs.difference(start,end).seconds)/float(3600)
        else:
            return ""
            


    BRSCreatedToPLPTimeWKhrs=MasterData.apply(lambda row: BusHrs(row['BRSCreatedDate'], row['PLP_DateTime']), axis=1)
    PLPTimeToSTCTimeWKhrs=MasterData.apply(lambda row: BusHrs(row['PLP_DateTime'], row['STC_DateTime']), axis=1)
    STCToSTPTimeWKhrs=MasterData.apply(lambda row: BusHrs(row['STC_DateTime'], row["STP_DateTime"]), axis=1)
    STPToSTBTimeWKhrs=MasterData.apply(lambda row: BusHrs(row['STP_DateTime'], row["STB_DateTime"]), axis=1)

    ####Weekends
    # Define a working weekend day(Saturday)

    Saturday = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(16),
        working_days=[5])

    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    businesshrs = businesstimedelta.Rules([Saturday, holidays])

    def SatHrs(start, end):
        if end>=start:
            return businesshrs.difference(start,end).hours+float(businesshrs.difference(start,end).seconds)/float(3600)
        else:
            return ""


    BRSCreatedToPLPTimeSathrs=MasterData.apply(lambda row: SatHrs(row['BRSCreatedDate'], row['PLP_DateTime']), axis=1)
    PLPTimeToSTCTimeSathrs=MasterData.apply(lambda row: SatHrs(row['PLP_DateTime'], row['STC_DateTime']), axis=1)
    STCToSTPTimeSathrs=MasterData.apply(lambda row: SatHrs(row['STC_DateTime'], row["STP_DateTime"]), axis=1)
    STPToSTBTimeSathrs=MasterData.apply(lambda row: SatHrs(row['STP_DateTime'], row["STB_DateTime"]), axis=1)

    MasterData["BRSCreated_To_PLP Time"]=(BRSCreatedToPLPTimeWKhrs+BRSCreatedToPLPTimeSathrs)*60
    MasterData["PLP_To_STC Time"]=(PLPTimeToSTCTimeSathrs+PLPTimeToSTCTimeWKhrs)*60
    MasterData["STC_To_STP Time"]=(STCToSTPTimeWKhrs+STCToSTPTimeSathrs)*60
    MasterData["STP_To_STB Time"]=(STPToSTBTimeWKhrs+STPToSTBTimeSathrs)*60
    MasterData =MasterData.replace("", np.nan)
    MasterData.dropna(axis=0, thresh=None, subset=["BRSCreated_To_PLP Time","PLP_To_STC Time","STC_To_STP Time","STP_To_STB Time"], inplace=False)


    MasterData["StoresCreatedUser"]=MasterData["StoresCreatedUser"].str[:-1]
    MasterData['STP_User']=MasterData['STP_User'].str[:-1]
    MasterData['STB_User']=MasterData['STB_User'].str[:-1]
        


    #### Average Time Taken From BRS created to Picklist Printed in the Stores   
    AverageTime1= pd.pivot_table(MasterData,
                        values = ['No. of Items','BRSCreated_To_PLP Time',"ITR No"],
                        index = {'StoresCreatedUser'},
                        columns = [],aggfunc={'No. of Items':np.sum,'BRSCreated_To_PLP Time':np.mean,"ITR No":np.count_nonzero},
                        margins=True).fillna('')
    print(AverageTime1)
    print('Average Time Taken From BRS created to Picklist Printed')

    ### Average Time Taken From Picklist Printed  to Sent to Control Room 
    AverageTime2= pd.pivot_table(MasterData,
                        values = ['No. of Items','PLP_To_STC Time',"ITR No"],
                        index = {'STC_Day','StoresCreatedUser'},
                        columns = [],aggfunc={'No. of Items':np.sum,'PLP_To_STC Time':np.mean,"ITR No":np.count_nonzero},
                        margins=True).fillna('')
    
    print(AverageTime2)
    print('Average Time Taken From Picklist Printed  to Sent to Control Room ')

    ### Average Time Taken From  Sent to Control Room to Sent to Packaging
    AverageTime3= pd.pivot_table(MasterData,
                        values = ['No. of Items','STC_To_STP Time',"ITR No"],
                        index = {'STC_Day','STP_User'},
                        columns = [],aggfunc={'No. of Items':np.sum,'STC_To_STP Time':np.mean,"ITR No":np.count_nonzero},
                        margins=True).fillna('')
    print(AverageTime3)
    print('Average Time Taken From  Sent to Control Room to Sent to Packaging ')

    ### Average Time Taken From  Sent to Packaging¶ to Sent to Branch
    AverageTime4= pd.pivot_table(MasterData,
                        values = ['No. of Items','STP_To_STB Time',"ITR No"],
                        index = {'STB_Day','STB_User'},
                        columns = [],aggfunc={'No. of Items':np.sum,'STP_To_STB Time':np.mean,"ITR No":np.count_nonzero},
                        margins=True).fillna('')
    
    print(AverageTime4)
    print('Average Time Taken From  Sent to Packaging¶ to Sent to Branch ')



    """ Data Summary: Main Store, Designer Store, Lens Store, Control Room and Packaging """
    MasterData["Hour"]=MasterData["PLP_DateTime"].dt.hour
    BRSMain=MasterData[MasterData.StoresCreatedUser=="main"]
    BRSMain_pivot=pd.pivot_table(BRSMain,index="Hour",values=["ITR No","No. of Items","BRSCreated_To_PLP Time"],aggfunc={"ITR No":np.count_nonzero,
                                                                                                                    "No. of Items":np.sum,
                                                                                                                    "BRSCreated_To_PLP Time":np.mean},margins=True)
    BRSMain_pivot=BRSMain_pivot.rename(columns={"BRSCreated_To_PLP Time":"AVG BRSCreated_To_PLP Time","ITR No":"Counts of ITRs"})
    BRSMain_pivot=BRSMain_pivot.transpose()  


    BRSDesigner=MasterData[MasterData.StoresCreatedUser=="designer"]
    BRSDesigner_pivot=pd.pivot_table(BRSDesigner,index="Hour",values=["ITR No","No. of Items","BRSCreated_To_PLP Time"],aggfunc={"ITR No":np.count_nonzero,
                                                                                                                    "No. of Items":np.sum,
                                                                                                                    "BRSCreated_To_PLP Time":np.mean},
                            margins=True)
    BRSDesigner_pivot=BRSDesigner_pivot.rename(columns={"BRSCreated_To_PLP Time":"AVG BRSCreated_To_PLP Time","ITR No":"Counts of ITRs"})
    BRSDesigner_pivot=BRSDesigner_pivot.transpose()
    BRSDesigner_pivot


    BRSLens=MasterData[MasterData.StoresCreatedUser=="lens"]
    BRSLens_pivot=pd.pivot_table(BRSLens,index="Hour",values=["ITR No","No. of Items","BRSCreated_To_PLP Time"],aggfunc={"ITR No":np.count_nonzero,
                                                                                                                    "No. of Items":np.sum,
                                                                                                                    "BRSCreated_To_PLP Time":np.mean},
                            margins=True)
    BRSLens_pivot=BRSLens_pivot.rename(columns={"BRSCreated_To_PLP Time":"AVG BRSCreated_To_PLP Time","ITR No":"Counts of ITRs"})
    BRSLens_pivot=BRSLens_pivot.transpose()
    BRSLens_pivot
    ############################################################################
    MasterData["Hour"]=MasterData['STC_DateTime'].dt.hour
    Main=MasterData[MasterData.StoresCreatedUser=="main"]
    main_pivot=pd.pivot_table(Main,index="Hour",values=["ITR No","No. of Items",'PLP_To_STC Time'],aggfunc={"ITR No":np.count_nonzero,
                                                                                                                    "No. of Items":np.sum,
                                                                                                                    'PLP_To_STC Time':np.mean},
                            margins=True)
    main_pivot=main_pivot.rename(columns={'PLP_To_STC Time':'AVG PLP_To_STC Time',"ITR No":"Counts of ITRs"})
    main_pivot=main_pivot.transpose()
    main_pivot
    ###########
    Designer=MasterData[MasterData.StoresCreatedUser=="designer"]
    Designer_pivot=pd.pivot_table(Designer,index="Hour",values=["ITR No","No. of Items",'PLP_To_STC Time'],aggfunc={"ITR No":np.count_nonzero,
                                                                                                                    "No. of Items":np.sum,
                                                                                                                    'PLP_To_STC Time':np.mean},
                            margins=True)
    Designer_pivot=Designer_pivot.rename(columns={'PLP_To_STC Time':'AVG PLP_To_STC Time',"ITR No":"Counts of ITRs"})
    Designer_pivot=Designer_pivot.transpose()
    Designer_pivot
    ###########
    Lens=MasterData[MasterData.StoresCreatedUser=="lens"]
    Lens_pivot=pd.pivot_table(Lens,index="Hour",values=["ITR No","No. of Items",'PLP_To_STC Time'],aggfunc={"ITR No":np.count_nonzero,
                                                                                                                    "No. of Items":np.sum,
                                                                                                                    'PLP_To_STC Time':np.mean},
                            margins=True)
    Lens_pivot=Lens_pivot.rename(columns={'PLP_To_STC Time':'AVG PLP_To_STC Time',"ITR No":"Counts of ITRs"})
    Lens_pivot=Lens_pivot.transpose()
    Lens_pivot

    ############################################################################
    MasterData["Hour"]=MasterData["STP_DateTime"].dt.hour
    Control_pivot=pd.pivot_table(MasterData,index="Hour",values=["ITR No","No. of Items","STC_To_STP Time"],aggfunc={"ITR No":np.count_nonzero,
                                                                                                                    "No. of Items":np.sum,
                                                                                                                    "STC_To_STP Time":np.mean},
                            margins=True)
    Control_pivot=Control_pivot.rename(columns={"STC_To_STP Time":"AVG STC_To_STP Time","ITR No":"Counts of ITRs"})
    Control_pivot=Control_pivot.transpose()
    Control_pivot

    ############################################################################
    MasterData["Hour"]=MasterData["STB_DateTime"].dt.hour
    Packaging_pivot=pd.pivot_table(MasterData,index="Hour",values=["ITR No","No. of Items","STP_To_STB Time"],aggfunc={"ITR No":np.count_nonzero,
                                                                                                                    "No. of Items":np.sum,
                                                                                                                    "STP_To_STB Time":np.mean},
                            margins=True)
    Packaging_pivot=Packaging_pivot.rename(columns={"STP_To_STB Time":"AVG STP_To_STB Time","ITR No":"Counts of ITRs"})
    Packaging_pivot=Packaging_pivot.transpose()
    

    MainItems = BRSMain_pivot.iloc[2,-1]
    MainCount = BRSMain_pivot.iloc[1,-1]
    Mainavg = BRSMain_pivot.iloc[0,-1] + main_pivot.iloc[2,-1]

    DesignerItems = BRSDesigner_pivot.iloc[2,-1]
    DesignerCount = BRSDesigner_pivot.iloc[1,-1]
    Designeravg = BRSDesigner_pivot.iloc[0,-1] + Designer_pivot.iloc[2,-1]

    LensItems = BRSLens_pivot.iloc[2,-1]
    LensCount = BRSLens_pivot.iloc[1,-1]
    Lensavg = BRSLens_pivot.iloc[0,-1] + Lens_pivot.iloc[2,-1]

    ControlItems = Control_pivot.iloc[1,-1]
    ControlCount = Control_pivot.iloc[0,-1]
    Controlavg = Control_pivot.iloc[2,-1]

    PackagingItems = Packaging_pivot.iloc[1,-1]
    PackagingCount = Packaging_pivot.iloc[0,-1]
    Packagingavg = Packaging_pivot.iloc[2,-1]

    replacements_itr_summary = pd.DataFrame({
    "Department":["main","designer","lens","control","packaging"],
    "ITR count":[MainCount,DesignerCount,LensCount,ControlCount,PackagingCount],
    "Items":[MainItems,DesignerItems,LensItems,ControlItems,PackagingItems],
    "Avg Time":[Mainavg,Designeravg,Lensavg,Controlavg,Packagingavg],
    })
    replacements_itr_summary = replacements_itr_summary.round(2)

    ####Writing to Excel
    import xlsxwriter
    print(xlsxwriter.__version__)

    #Create a Pandas Excel writer using XlsxWriter as the engine.
    with pd.ExcelWriter(r"/home/opticabi/Documents/optica_reports/order_efficiency/Stock_Replacement_Efficiency_Summary.xlsx", engine='xlsxwriter') as writer:    
        # Write each dataframe to a different worksheet.
        BRSMain_pivot.to_excel(writer, sheet_name='BRS Main',index=True,index_label="Hour")
        BRSDesigner_pivot.to_excel(writer, sheet_name='BRS Designer',index=True,index_label="Hour")
        BRSLens_pivot.to_excel(writer, sheet_name='BRS Lens',index=True,index_label="Hour")
        main_pivot.to_excel(writer, sheet_name='Main',index=True,index_label="Hour")
        Designer_pivot.to_excel(writer, sheet_name='Designer',index=True,index_label="Hour")
        Lens_pivot.to_excel(writer, sheet_name='Lens',index=True,index_label="Hour")
        Control_pivot.to_excel(writer, sheet_name='Control',index=True,index_label="Hour")
        Packaging_pivot.to_excel(writer, sheet_name='Packaging',index=True,index_label="Hour")
        replacements_itr_summary.to_excel(writer, sheet_name='ITR')
        

    def save_xls(list_dfs, xls_path):
        with ExcelWriter(xls_path) as writer:
            for n, df in enumerate(list_dfs):
                df.to_excel(writer,'sheet%s' % n)
            writer.save()
    #Create a Pandas Excel writer using XlsxWriter as the engine.
    with pd.ExcelWriter(r"/home/opticabi/Documents/optica_reports/order_efficiency/Stock_Replacement_Efficiency_Full_Report.xlsx", engine='xlsxwriter') as writer:    
        # Write each dataframe to a different worksheet.
        MasterData.to_excel(writer, sheet_name='ReplacementsEfficiency',index=False)
        

    def save_xls(list_dfs, xls_path):
        with ExcelWriter(xls_path) as writer:
            for n, df in enumerate(list_dfs):
                df.to_excel(writer,'sheet%s' % n)
            writer.save()
replacements()