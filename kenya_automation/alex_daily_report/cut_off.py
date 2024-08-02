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
from sub_tasks.libraries.utils import createe_engine,create_unganda_engine,create_rwanda_engine

conn = createe_engine()
uganda_engine = create_unganda_engine()
rwanda_engine = create_rwanda_engine()

##Others
import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from sub_tasks.libraries.utils import get_todate,send_report,assert_date_modified, create_initial_file, return_sent_emails, fetch_gsheet_data, record_sent_branch, fourth_week_start, fourth_week_end
from kenya_automation.alex_daily_report.data.fetch_data import repdata, to_warehouse


today = datetime.date.today()
# today = datetime.date(2024,1,25)
yesterday = today - datetime.timedelta(days=1)
print(yesterday)
formatted_date = yesterday.strftime('%Y-%m-%d')



def cutoff():
    ITRWithIssues = fetch_gsheet_data()["itrs_with_issues"]
    ITRWithIssues.rename(columns={'  ITR Number':'ITR Number'}, inplace = True)
    ITRWithIssues=ITRWithIssues[["ITR Number","DEPARTMENT","DEPARTMENT 2"]]

    orders_cutoff = fetch_gsheet_data()["itr_cutoff"]
    Branches2 = orders_cutoff

    """Replacement ITR over date range to distinguish between branch stock and stock borrow"""
    """KENYA"""    
    Repdata = repdata(database="mabawa",engine=conn )
    Repdata['Creation Date']=pd.to_datetime(Repdata['Creation Date'],dayfirst=True  ).dt.date
    Repdata['Creatn Time - Incl. Secs']=pd.to_datetime(Repdata["Creatn Time - Incl. Secs"],format="%H%M%S",errors = 'coerce').dt.time
    
    """UGANDA"""   
    Repdata_ug = repdata(database="mawingu",engine=uganda_engine)
    Repdata_ug['Creation Date']=pd.to_datetime(Repdata_ug['Creation Date'],dayfirst=True  ).dt.date
    Repdata_ug['Creatn Time - Incl. Secs']=pd.to_datetime(Repdata_ug["Creatn Time - Incl. Secs"],format="%H%M%S",errors = 'coerce').dt.time
    
    """RWANDA""" 
    Repdata_rw = repdata(database="voler",engine=rwanda_engine)
    Repdata_rw['Creation Date']=pd.to_datetime(Repdata_rw['Creation Date'],dayfirst=True  ).dt.date
    Repdata_rw['Creatn Time - Incl. Secs']=pd.to_datetime(Repdata_rw["Creatn Time - Incl. Secs"],format="%H%M%S",errors = 'coerce').dt.time


    """3rd floor  all user Replacement"""
    Replacements = """select created_user as "Created User",post_date as "Date",post_time,
                        to_char(post_time, 'FM999:09:99'::text)::time without time zone AS "Time", status as "Status", item_code as "Item Code", itr_no as "ITR No" 
                        from mabawa_staging.source_itr_log sil 
                        where post_date::date between '{yesterday}' and '{yesterday}'
                    """.format(yesterday=yesterday)
    
    Replacements = pd.read_sql_query(Replacements,con=conn)
    Replacements['Date']=pd.to_datetime(Replacements['Date'],dayfirst=True  ).dt.date
    Replacements["Date_Time"]=pd.to_datetime(Replacements.Date.astype(str) + ' ' + Replacements.Time.astype(str), format="%Y%m%d %H:%M")
    Replacements['Day_End'] = Replacements["Date_Time"].dt.day_name()
    Replacements=Replacements[["Date","Time",'Created User', 'Status', 'Item Code', 'ITR No','Day_End','Date_Time']]
    Replacements['Date'] = Replacements.Date.astype('datetime64[ns]')
    Replacements=Replacements.rename(columns={"ITR No":"Internal Number"})
    print(Replacements)

    """ITR Created by BRS with Date and Time"""    
    towhse = to_warehouse(database="mabawa",engine=conn)
    towhse['Creation Date']=pd.to_datetime(towhse['Creation Date'],dayfirst=True ).dt.date

    towhse_ug = to_warehouse(database="mawingu",engine=uganda_engine)
    towhse_ug['Creation Date']=pd.to_datetime(towhse_ug['Creation Date'],dayfirst=True ).dt.date

    towhse_rw = to_warehouse(database="voler",engine=rwanda_engine)
    towhse_rw['Creation Date']=pd.to_datetime(towhse_rw['Creation Date'],dayfirst=True ).dt.date

    
    """ DEPARTMENTS CUT OFF:
    Filtering Through the master dataset """
    mainstoreusers = ('main1','main2','main3')
    main1=Replacements[Replacements["Created User"].isin(mainstoreusers)]
    main2=Replacements[Replacements["Created User"]=="main2"]

    lensstoreusers = ('lens1','lens2','lens3')
    lens1=Replacements[Replacements["Created User"].isin(lensstoreusers)]
    lens2=Replacements[Replacements["Created User"]=="lens2"]

    designer1=Replacements[Replacements["Created User"]=="designer1"]
    designer2=Replacements[Replacements["Created User"]=="designer2"]

    controlusers = ('control1','control2','control3','control4')
    control1=Replacements[Replacements["Created User"].isin(controlusers)]
    control2=Replacements[Replacements["Created User"]=="control2"]
    control3=Replacements[Replacements["Created User"]=="control3"]

    packaging1=Replacements[Replacements["Created User"]=="packaging1"]
    packaging2=Replacements[Replacements["Created User"]=="packaging2"]

    """1. BRS Cut Off """
    ##Filtering Out OTC Items,Since OTC items are done in the evenning
    Repdata1 = Repdata.copy(deep=True)
    Repdata1['Item/Service Description'] = Repdata1['Item/Service Description'].astype(str)
    Repdata1 = Repdata1[~Repdata1['Item/Service Description'].str.contains("CASE", "BAG")]
    Repdata2 = Repdata1[~Repdata1['Item/Service Description'].str.contains("CLOTH")]
    Repdata2=Repdata2.dropna(subset=['Order Number'],inplace=False)
    NormalRep = Repdata2.copy()

    ##Merging towhse data into rep data i.e to get To Warehouse
    towhse=towhse[["Internal Number","To Warehouse Code"]]
    NormalRep =pd.merge(NormalRep,towhse,  on = 'Internal Number', how = 'left')
    NormalRep = NormalRep.rename(columns = {"To Warehouse Code": "Branch"})
    NormalRepRegion=NormalRep.copy()

    ###Separating First and Second cut offs then finding the max value per [Branch,Day & Cut off]
    # NormalRepRegion["Type"]=np.where(NormalRepRegion['Creatn Time - Incl. Secs']>=datetime.time(12, 0, 0),"Second","First")
    NormalRepRegion = pd.merge(NormalRepRegion,Branches2[["Branch","Address"]],on='Branch', how='left')    
    def cut_off(x):
        try:
            if 'YOR' in x['Address']:
                type_ = np.where(x['Creatn Time - Incl. Secs']>datetime.time(9, 20, 0),"Second","First")
                
                if (x['Branch']=="OHO")& (x['Creatn Time - Incl. Secs']>datetime.time(11, 15, 0) ) :
                    type_ = "Third"
            
            elif "CBD" in  x['Address'] :
                type_ = np.where(x['Creatn Time - Incl. Secs']>datetime.time(10, 30, 0),"Second","First")

            elif 'Rider' in x['Address']:
                type_ = np.where(x['Creatn Time - Incl. Secs']>datetime.time(10, 30, 0),"Second","First")  
            
            else:
                type_ = "First"
            
        except :
            type_ = np.where(x['Creatn Time - Incl. Secs']>datetime.time(10, 30, 0), "Second", "First")
        return str(type_)  

    NormalRepRegion["Type"] = NormalRepRegion.apply( lambda y: cut_off(y) , axis=1) 


    Brs_Pivot=pd.pivot_table(NormalRepRegion,index=["Branch",'Creation Date',"Type"],values='Creatn Time - Incl. Secs',aggfunc=np.max)
    Brs_Pivot = Brs_Pivot.reset_index()
    Brs_Pivot=Brs_Pivot.rename(columns={'Creatn Time - Incl. Secs':"Max"})

    ###Calculating the cut off
    NormalRepRegion=pd.merge(NormalRepRegion,Brs_Pivot,on=["Branch","Creation Date","Type"],how="left")
    NormalRepRegion = pd.merge(NormalRepRegion,Branches2.drop(columns=["Address"]),  on = ['Branch',"Type"], how = 'left')
    NormalRepRegion['Region'] = ['YOR/OHO' if x=='YOR OHO' 
                                else "CBD" if x=='CBD Messenger' 
                                else 'Thika' if x == 'Rider 5 Thika Town'
                                else 'Nairobi' if x=='Rider 2 Karen' 
                                or x=='Rider 3 Eastlands' or x=='Rider 4 Thika Rd' or x=='Rider 1 Westlands'                                
                                else 'Uganda/Rwanda' if x== 'Rider' 
                                else 'G4S' if x=='Rider G4S' 
                                else 'Upcountry'  for x in NormalRepRegion['Address']]

    NormalRepRegion['Creatn Time - Incl. Secs'] = NormalRepRegion['Creatn Time - Incl. Secs'].astype(str)
    NormalRepRegion['BRS CUT OFF'] = np.where(NormalRepRegion['Creatn Time - Incl. Secs'] > NormalRepRegion["BRS Cut"],0, 1)
    NormalRepRegion['Region']=np.where(((NormalRepRegion['Warehouse Name']=="Junction") |
                                         (NormalRepRegion['Warehouse Name']=='The HUB')  |
                          (NormalRepRegion['Warehouse Name']=='Optica House') |
                          (NormalRepRegion['Warehouse Name']=='Capital Centre')|
                          (NormalRepRegion['Warehouse Name']=='Garden City')|
                          (NormalRepRegion['Warehouse Name']=='T-Mall')|
                          (NormalRepRegion['Warehouse Name']=='TRM')|
                          (NormalRepRegion['Warehouse Name']=='Village Market')|
                          (NormalRepRegion['Warehouse Name']=='Westgate')|
                          (NormalRepRegion['Warehouse Name']=='York House')|
                          (NormalRepRegion['Warehouse Name']=='Corner House')|                         
                          (NormalRepRegion['Warehouse Name']=='Kaunda')|
                          (NormalRepRegion['Warehouse Name']=='Westlands')|
                          (NormalRepRegion['Warehouse Name']=='Mega Mall')|
                          (NormalRepRegion['Warehouse Name']=='Two Rivers Mall')|
                           (NormalRepRegion['Warehouse Name']=='Ug Arena')|
                            (NormalRepRegion['Warehouse Name']=='Ug Acacia')|
                             (NormalRepRegion['Warehouse Name']=='Ug Oasis')|
                             (NormalRepRegion['Warehouse Name']=='Ug Kampala Boulevard')) &
                         (NormalRepRegion['Type']=='Second'),NormalRepRegion['Warehouse Name'],
                         NormalRepRegion['Region']) 
    
    NormalRep=NormalRepRegion.copy()
    sameNormalRep = NormalRepRegion.copy()
    NormalRepRegion = pd.merge(sameNormalRep,ITRWithIssues,on="ITR Number", how="left")
    print(NormalRepRegion.columns)

    branchesfilter=['JUN','HUB','YOR','OHO',"CAP","GAR","TMA","TRM","VMA","WGT","MEG","COR","KAU","WES","TWO"]
    NormalRep["Type"]=np.where((NormalRep["Type"]=="Second") & ~(NormalRep["Branch"].isin(branchesfilter)),"First",NormalRep["Type"])

     

    filterBRS=['BRS','ALL']
    issuesBRS = NormalRepRegion[(NormalRepRegion['DEPARTMENT'].isin(filterBRS) | (NormalRepRegion['DEPARTMENT 2'].isin(filterBRS)))]
    issuesBRS = issuesBRS["ITR Number"].to_list()

    NormalRepRegion = NormalRepRegion[~NormalRepRegion["ITR Number"].isin(issuesBRS)]
    print(NormalRepRegion.columns)
    print(NormalRepRegion)
    print('This has been printed NormalRepRegion')

    ###Grouping per Department and region
    count_summary=pd.pivot_table(NormalRepRegion,index=['Type', "Region"],values="BRS CUT OFF",aggfunc='count',margins=True)
    count_summary.reset_index()

    sum_summary=pd.pivot_table(NormalRepRegion,index=['Type', "Region"],values="BRS CUT OFF",aggfunc=np.sum,margins=True)
    sum_summary.reset_index()

    count_ITR=pd.pivot_table(NormalRepRegion,index=['Type', "Region"],values="ITR Number",aggfunc=pd.Series.nunique,margins=True)
    count_ITR.reset_index()

    ###Merging the data
    Summary_BRS=pd.merge(sum_summary,count_summary,on=["Type","Region"])
    Summary_BRS["%_ge Efficiency"]=Summary_BRS["BRS CUT OFF_x"]/Summary_BRS["BRS CUT OFF_y"]
    Summary_BRS=pd.merge(Summary_BRS,count_ITR,on=["Type","Region"],how="left")
    Summary_BRS
    Summary_BRS.drop(["BRS CUT OFF_x","BRS CUT OFF_y"], axis='columns', inplace=True)
    format_dict = { '%_ge Efficiency': '{:.2%}'}
    Summary_BRS.style.format(format_dict)
    Summary_BRS.reset_index()

    if not Repdata_ug.empty:
        """1.1 BRS Cut Off - Uganda"""
        ##Filtering Out OTC Items,Since OTC items are done in the evenning
        Repdata1_ug = Repdata_ug.copy(deep=True)
        Repdata1_ug['Item/Service Description'] = Repdata1_ug['Item/Service Description'].astype(str)
        Repdata1_ug = Repdata1_ug[~Repdata1_ug['Item/Service Description'].str.contains("CASE", "BAG")]
        Repdata2_ug = Repdata1_ug[~Repdata1_ug['Item/Service Description'].str.contains("CLOTH")]
        Repdata2_ug=Repdata2_ug.dropna(subset=['Order Number'],inplace=False)
        NormalRep_ug = Repdata2_ug

        ##Merging towhse data into rep data i.e to get To Warehouse
        towhse_ug=towhse_ug[["Internal Number","To Warehouse Code"]]
        NormalRep_ug =pd.merge(NormalRep_ug,towhse_ug,  on = 'Internal Number', how = 'left').rename(columns = {"To Warehouse Code": "Branch"})
        NormalRepRegion_ug=NormalRep_ug

        ###Separating First and Second cut offs then finding the max value per [Branch,Day & Cut off]
        NormalRepRegion_ug["Type"]=np.where(NormalRepRegion_ug['Creatn Time - Incl. Secs']>=datetime.time(12, 0, 0),"Second","First")
        print(NormalRepRegion_ug)
        print('NormalRepRegion_ug Printed')
        Brs_Pivot_ug=pd.pivot_table(NormalRepRegion_ug,index=["Branch",'Creation Date',"Type"],values='Creatn Time - Incl. Secs',aggfunc=np.max)
        Brs_Pivot_ug = Brs_Pivot_ug.reset_index()
        Brs_Pivot_ug=Brs_Pivot_ug.rename(columns={'Creatn Time - Incl. Secs':"Max"})

        ###Calculating the cut off
        NormalRepRegion_ug=pd.merge(NormalRepRegion_ug,Brs_Pivot_ug,on=["Branch","Creation Date","Type"],how="left")
        NormalRepRegion_ug = pd.merge(NormalRepRegion_ug,Branches2,  on = ['Branch',"Type"], how = 'left')
        NormalRepRegion_ug['Region'] = ['Uganda' if x== 'Rider_UG' else ' ' for x in NormalRepRegion_ug['Address']]
        
        print(NormalRepRegion_ug[['Creatn Time - Incl. Secs','BRS Cut']])
        NormalRepRegion_ug['Creatn Time - Incl. Secs'] = NormalRepRegion_ug['Creatn Time - Incl. Secs'].astype(str)
        NormalRepRegion_ug['BRS CUT OFF'] = np.where(NormalRepRegion_ug['Creatn Time - Incl. Secs'] > NormalRepRegion_ug["BRS Cut"],0, 1)   
        
        NormalRep_ug=NormalRepRegion_ug
        sameNormalRep_ug = NormalRepRegion_ug.copy()
        NormalRepRegion_ug = pd.merge(sameNormalRep_ug,ITRWithIssues,on="ITR Number", how="left")

        # branchesfilter=['JUN','HUB','YOR','OHO',"CAP","GAR","TMA","TRM","VMA","WGT","MEG","COR","KAU","WES","TWO","ARE","ACA","KAM","OAS"]
        NormalRep_ug["Type"]=np.where((NormalRep_ug["Type"]=="Second"),"First",NormalRep_ug["Type"])
        

        filterBRS=['BRS','ALL']
        issuesBRS_ug = NormalRepRegion_ug[(NormalRepRegion_ug['DEPARTMENT'].isin(filterBRS) | (NormalRepRegion_ug['DEPARTMENT 2'].isin(filterBRS)))]
        issuesBRS_ug = issuesBRS_ug["ITR Number"].to_list()

        NormalRepRegion_ug = NormalRepRegion_ug[~NormalRepRegion_ug["ITR Number"].isin(issuesBRS)]

        ###Grouping per Department and region
        count_summary_ug=pd.pivot_table(NormalRepRegion_ug,index=['Type', "Region"],values="BRS CUT OFF",aggfunc='count',margins=True).reset_index()

        sum_summary_ug=pd.pivot_table(NormalRepRegion_ug,index=['Type', "Region"],values="BRS CUT OFF",aggfunc=np.sum,margins=True).reset_index()

        count_ITR_ug=pd.pivot_table(NormalRepRegion_ug,index=['Type', "Region"],values="ITR Number",aggfunc=pd.Series.nunique,margins=True).reset_index()

        ###Merging the data
        Summary_BRS_ug=pd.merge(sum_summary_ug,count_summary_ug,on=["Type","Region"])
        Summary_BRS_ug["%_ge Efficiency"]=Summary_BRS_ug["BRS CUT OFF_x"]/Summary_BRS_ug["BRS CUT OFF_y"]
        Summary_BRS_ug=pd.merge(Summary_BRS_ug,count_ITR_ug,on=["Type","Region"],how="left")
    
        Summary_BRS_ug.drop(["BRS CUT OFF_x","BRS CUT OFF_y"], axis='columns', inplace=True)
        print('uganda')
        print(Summary_BRS_ug)
        format_dict = { '%_ge Efficiency': '{:.2%}'}
        Summary_BRS_ug.style.format(format_dict)
        Summary_BRS_ug.reset_index()

    if not Repdata_rw.empty:
        """1.1 BRS Cut Off - Rwanda"""
        ##Filtering Out OTC Items,Since OTC items are done in the evenning
        Repdata1_rw = Repdata_rw.copy(deep=True)
        Repdata1_rw['Item/Service Description'] = Repdata1_rw['Item/Service Description'].astype(str)
        Repdata1_rw = Repdata1_rw[~Repdata1_rw['Item/Service Description'].str.contains("CASE", "BAG")]
        Repdata2_rw = Repdata1_rw[~Repdata1_rw['Item/Service Description'].str.contains("CLOTH")]
        Repdata2_rw=Repdata2_rw.dropna(subset=['Order Number'],inplace=False)
        NormalRep_rw = Repdata2_rw

        ##Merging towhse data into rep data i.e to get To Warehouse
        towhse_rw=towhse_rw[["Internal Number","To Warehouse Code"]]
        NormalRep_rw =pd.merge(NormalRep_rw,towhse_rw,  on = 'Internal Number', how = 'left').rename(columns = {"To Warehouse Code": "Branch"})
        NormalRepRegion_rw =NormalRep_rw

        ###Separating First and Second cut offs then finding the max value per [Branch,Day & Cut off]
        NormalRepRegion_rw["Type"]=np.where(NormalRepRegion_rw['Creatn Time - Incl. Secs']>=datetime.time(12, 0, 0),"Second","First")
        Brs_Pivot_rw=pd.pivot_table(NormalRepRegion_rw,index=["Branch",'Creation Date',"Type"],values='Creatn Time - Incl. Secs',aggfunc=np.max).reset_index()
        Brs_Pivot_rw=Brs_Pivot_rw.rename(columns={'Creatn Time - Incl. Secs':"Max"})

        ###Calculating the cut off
        NormalRepRegion_rw=pd.merge(NormalRepRegion_rw,Brs_Pivot_rw,on=["Branch","Creation Date","Type"],how="left")
        NormalRepRegion_rw = pd.merge(NormalRepRegion_rw,Branches2,  on = ['Branch',"Type"], how = 'left')
        NormalRepRegion_rw['Region'] = ['Rwanda' if x== 'Rider_RW' else ' ' for x in NormalRepRegion_rw['Address']]
        
        print(NormalRepRegion_rw[['Creatn Time - Incl. Secs','BRS Cut']])
        NormalRepRegion_rw['Creatn Time - Incl. Secs'] = NormalRepRegion_rw['Creatn Time - Incl. Secs'].astype(str)
        NormalRepRegion_rw['BRS CUT OFF'] = np.where(NormalRepRegion_rw['Creatn Time - Incl. Secs'] > NormalRepRegion_rw["BRS Cut"],0, 1)   
        
        NormalRep_rw=NormalRepRegion_rw
        sameNormalRep_rw = NormalRepRegion_rw.copy()
        NormalRepRegion_rw = pd.merge(sameNormalRep_rw,ITRWithIssues,on="ITR Number", how="left")

        # branchesfilter=['JUN','HUB','YOR','OHO',"CAP","GAR","TMA","TRM","VMA","WGT","MEG","COR","KAU","WES","TWO","ARE","ACA","KAM","OAS"]
        NormalRep_rw["Type"]=np.where((NormalRep_rw["Type"]=="Second"),"First",NormalRep_rw["Type"])
        

        filterBRS=['BRS','ALL']
        issuesBRS_rw = NormalRepRegion_rw[(NormalRepRegion_rw['DEPARTMENT'].isin(filterBRS) | (NormalRepRegion_rw['DEPARTMENT 2'].isin(filterBRS)))]
        issuesBRS_rw = issuesBRS_rw["ITR Number"].to_list()

        NormalRepRegion_rw = NormalRepRegion_rw[~NormalRepRegion_rw["ITR Number"].isin(issuesBRS)]

        ###Grouping per Department and region
        count_summary_rw=pd.pivot_table(NormalRepRegion_rw,index=['Type', "Region"],values="BRS CUT OFF",aggfunc='count',margins=True).reset_index()

        sum_summary_rw=pd.pivot_table(NormalRepRegion_rw,index=['Type', "Region"],values="BRS CUT OFF",aggfunc=np.sum,margins=True).reset_index()
    
        count_ITR_rw=pd.pivot_table(NormalRepRegion_rw,index=['Type', "Region"],values="ITR Number",aggfunc=pd.Series.nunique,margins=True).reset_index()

        ###Merging the data
        Summary_BRS_rw=pd.merge(sum_summary_rw,count_summary_rw,on=["Type","Region"])
        Summary_BRS_rw["%_ge Efficiency"]=Summary_BRS_rw["BRS CUT OFF_x"]/Summary_BRS_rw["BRS CUT OFF_y"]
        Summary_BRS_rw=pd.merge(Summary_BRS_rw,count_ITR_rw,on=["Type","Region"],how="left")
    
        Summary_BRS_rw.drop(["BRS CUT OFF_x","BRS CUT OFF_y"], axis='columns', inplace=True)
        print('rwanda')
        print(Summary_BRS_rw)
        format_dict = { '%_ge Efficiency': '{:.2%}'}
        Summary_BRS_rw.style.format(format_dict)
        Summary_BRS_rw.reset_index()
    else:
        Summary_BRS_rw = pd.DataFrame({'Type': [],'Region': [],'%_ge Efficiency': [],'ITR Number': []})
        NormalRepRegion_rw = pd.DataFrame({'BRS CUT OFF':[]})

    """ 2. Main Store Cut Off """
    MainStore_data=pd.concat([main1,main2])
    MainStore_data=MainStore_data[MainStore_data.Status=="Rep Sent to Control Room"]
    MainStore_data['Internal Number'] = MainStore_data['Internal Number'].astype(int)
    NormalRepRegion['Internal Number'] = NormalRepRegion['Internal Number'].astype(int)
    print(NormalRepRegion)
    print(NormalRepRegion.columns)
    print('NormalRepRegion')
    MainStore_data=pd.merge(MainStore_data,NormalRepRegion,on="Internal Number",how="inner")
    MainStore_data=MainStore_data.drop_duplicates(subset=['Internal Number'],keep="first", inplace=False)
    MainStore_data=MainStore_data[['Region',"Type","Internal Number","ITR Number","Created User","Status","Address","Warehouse Name","Branch","Date_Time","Date" ,"Time","Main Store Cut","DEPARTMENT","DEPARTMENT 2"]]
    print(MainStore_data)

    Main_Pivot=pd.pivot_table(MainStore_data,index=["Warehouse Name",'Date',"Type"],values='Time',aggfunc=np.max)
    Main_Pivot = Main_Pivot.reset_index()
    Main_Pivot=Main_Pivot.rename(columns={'Time':"Max"})
    MainStore_data=pd.merge(MainStore_data,Main_Pivot,on=["Warehouse Name","Date","Type"],how="left")
    MainStore_data['Time'] = MainStore_data['Time'].astype(str)
    MainStore_data['Main CUTOFF'] = np.where(MainStore_data['Time']>MainStore_data["Main Store Cut"],0, 1)

    MainStore_data["Type"]=np.where((MainStore_data["Type"]=="Second") & ~(MainStore_data["Branch"].isin(branchesfilter)),"First",MainStore_data["Type"])

    filtermain=['MAIN STORE','ALL']
    issuesMain = MainStore_data[(MainStore_data['DEPARTMENT'].isin(filtermain)) | (MainStore_data['DEPARTMENT 2'].isin(filtermain))]
    issuesMain = issuesMain["ITR Number"].to_list()
    MainStore_data = MainStore_data[~MainStore_data["ITR Number"].isin(issuesMain)]

    ###Grouping per Department and region
    count_summary=pd.pivot_table(MainStore_data,index=['Type', "Region"],values="Main CUTOFF",aggfunc='count',margins=True)
    count_summary.reset_index()

    sum_summary=pd.pivot_table(MainStore_data,index=['Type', "Region"],values="Main CUTOFF",aggfunc=np.sum,margins=True)
    sum_summary.reset_index()

    count_ITR=pd.pivot_table(MainStore_data,index=['Type', "Region"],values="ITR Number",aggfunc=np.count_nonzero,margins=True)
    count_ITR.reset_index()

    ###Merging the data
    Summary_MainStore=pd.merge(sum_summary,count_summary,on=["Type","Region"])
    Summary_MainStore["%_ge Efficiency"]=Summary_MainStore["Main CUTOFF_x"]/Summary_MainStore["Main CUTOFF_y"]
    Summary_MainStore=pd.merge(Summary_MainStore,count_ITR,on=["Type","Region"],how="left")
    Summary_MainStore.drop(["Main CUTOFF_x","Main CUTOFF_y"], axis='columns', inplace=True)
    format_dict = { '%_ge Efficiency': '{:.2%}'}
    Summary_MainStore.style.format(format_dict)


    """ 3. Designer Store Cut Off """
    DesinerStore_data=pd.concat([designer1,designer2])
    DesinerStore_data=DesinerStore_data[DesinerStore_data.Status=="Rep Sent to Control Room"]
    DesinerStore_data=DesinerStore_data.rename(columns={"ITR No":"Internal Number"})
    DesinerStore_data['Internal Number'] = DesinerStore_data['Internal Number'].astype(int)
    NormalRepRegion['Internal Number'] = NormalRepRegion['Internal Number'].astype(int)
    DesinerStore_data=pd.merge(DesinerStore_data,NormalRepRegion,on="Internal Number",how="inner")
    DesinerStore_data=DesinerStore_data.drop_duplicates(subset=['Internal Number'],keep="first", inplace=False)

    DesinerStore_data=DesinerStore_data[['Region',"Type","Internal Number","ITR Number","Created User","Status","Address","Warehouse Name","Branch","Date_Time","Date" ,"Time",'Designer Store Cut',"DEPARTMENT","DEPARTMENT 2"]]

    Designer_Pivot=pd.pivot_table(DesinerStore_data,index=["Warehouse Name",'Date',"Type"],values='Time',aggfunc=np.max)
    Designer_Pivot= Designer_Pivot.reset_index()
    Designer_Pivot=Designer_Pivot.rename(columns={'Time':"Max"})
    DesinerStore_data=pd.merge(DesinerStore_data,Designer_Pivot,on=["Warehouse Name","Date","Type"],how="left")
    DesinerStore_data['Time'] = DesinerStore_data['Time'].astype(str)
    DesinerStore_data['Designer CUTOFF'] = np.where(DesinerStore_data['Time']>DesinerStore_data['Designer Store Cut'],0, 1)
    DesinerStore_data["Type"]=np.where((DesinerStore_data["Type"]=="Second") & ~(DesinerStore_data["Branch"].isin(branchesfilter)),"First",DesinerStore_data["Type"])

    filterdes=['DESIGNER STORE','ALL']
    issuesDes = DesinerStore_data[((DesinerStore_data['DEPARTMENT'].isin(filterdes)) | (DesinerStore_data['DEPARTMENT 2'].isin(filterdes)))]
    issuesDes = issuesDes["ITR Number"].to_list()
    DesinerStore_data = DesinerStore_data[~DesinerStore_data["ITR Number"].isin(issuesDes)]


    ###Grouping per Department and region
    count_summary=pd.pivot_table(DesinerStore_data,index=['Type', "Region"],values='Designer CUTOFF',aggfunc='count',margins=True)
    count_summary.reset_index()

    sum_summary=pd.pivot_table(DesinerStore_data,index=['Type', "Region"],values='Designer CUTOFF',aggfunc=np.sum,margins=True)
    sum_summary.reset_index()

    count_ITR=pd.pivot_table(DesinerStore_data,index=['Type', "Region"],values="ITR Number",aggfunc=np.count_nonzero,margins=True)
    count_ITR.reset_index()

    ###Merging the data
    Summary_designerStore=pd.merge(sum_summary,count_summary,on=["Type","Region"])
    Summary_designerStore["%_ge Efficiency"]=Summary_designerStore["Designer CUTOFF_x"]/Summary_designerStore["Designer CUTOFF_y"]
    Summary_designerStore=pd.merge(Summary_designerStore,count_ITR,on=["Type","Region"],how="left")
    Summary_designerStore.drop(["Designer CUTOFF_x","Designer CUTOFF_y"], axis='columns', inplace=True)
    format_dict = { '%_ge Efficiency': '{:.2%}'}
    Summary_designerStore.style.format(format_dict)


    """ 4. Lens Store Cut Off """
    LensStore_data=pd.concat([lens1,lens2])
    print(LensStore_data.columns)
    LensStore_data=LensStore_data[LensStore_data.Status=="Rep Sent to Control Room"]
    LensStore_data=LensStore_data.rename(columns={"ITR No":"Internal Number"})
    LensStore_data['Internal Number'] = LensStore_data['Internal Number'].astype(int)
    NormalRepRegion['Internal Number'] = NormalRepRegion['Internal Number'].astype(int)
    LensStore_data=pd.merge(LensStore_data,NormalRepRegion,on="Internal Number",how="inner")
    LensStore_data=LensStore_data.drop_duplicates(subset=['Internal Number'],keep="first", inplace=False)

    LensStore_data=LensStore_data[['Region',"Type","Internal Number","ITR Number","Created User","Status","Address","Warehouse Name","Branch","Date_Time","Date" ,"Time",'Lens Store Cut',"DEPARTMENT","DEPARTMENT 2"]]

    LensStore_Pivot=pd.pivot_table(LensStore_data,index=["Warehouse Name",'Date',"Type"],values='Time',aggfunc=np.max)
    LensStore_Pivot= LensStore_Pivot.reset_index()
    LensStore_Pivot=LensStore_Pivot.rename(columns={'Time':"Max"})
    LensStore_data=pd.merge(LensStore_data,LensStore_Pivot,on=["Warehouse Name","Date","Type"],how="left")
    LensStore_data['Time'] = LensStore_data['Time'].astype(str)
    LensStore_data['Lens Store CutOff'] = np.where(LensStore_data['Time']>LensStore_data['Lens Store Cut'],0, 1)

    LensStore_data["Type"]=np.where((LensStore_data["Type"]=="Second") & ~(LensStore_data["Branch"].isin(branchesfilter)),"First",LensStore_data["Type"])

    filterlens = ['LENS STORE','ALL']
    issuesLens = LensStore_data[((LensStore_data['DEPARTMENT'].isin(filterlens)) | (LensStore_data['DEPARTMENT 2'].isin(filterlens)))]
    issuesLens = issuesLens["ITR Number"].to_list()
    LensStore_data = LensStore_data[~LensStore_data["ITR Number"].isin(issuesLens)]
    print(LensStore_data)

    
    ###Grouping per Department and region
    count_summary=pd.pivot_table(LensStore_data,index=['Type', "Region"],values='Lens Store CutOff',aggfunc='count',margins=True)
    count_summary.reset_index()

    sum_summary=pd.pivot_table(LensStore_data,index=['Type', "Region"],values='Lens Store CutOff',aggfunc=np.sum,margins=True)
    sum_summary.reset_index()

    count_ITR=pd.pivot_table(LensStore_data,index=['Type', "Region"],values="ITR Number",aggfunc=np.count_nonzero,margins=True)
    count_ITR.reset_index()

    ###Merging the data
    Summary_LensStore=pd.merge(sum_summary,count_summary,on=["Type","Region"])
    Summary_LensStore["%_ge Efficiency"]=Summary_LensStore["Lens Store CutOff_x"]/Summary_LensStore["Lens Store CutOff_y"]
    Summary_LensStore=pd.merge(Summary_LensStore,count_ITR,on=["Type","Region"],how="left")
    Summary_LensStore.drop(["Lens Store CutOff_x","Lens Store CutOff_y"], axis='columns', inplace=True)
    format_dict = { '%_ge Efficiency': '{:.2%}'}
    Summary_LensStore.style.format(format_dict)


    """ 5.Control Room Cut Off """
    ControlRoom_data=pd.concat([control1,control2,control3])
    ControlRoom_data=ControlRoom_data[ControlRoom_data.Status=="Rep Sent to Packaging"]
    ControlRoom_data=ControlRoom_data.rename(columns={"ITR No":"Internal Number"})
    ControlRoom_data['Internal Number'] = ControlRoom_data['Internal Number'].astype(int)
    NormalRepRegion['Internal Number'] = NormalRepRegion['Internal Number'].astype(int)
    ControlRoom_data=pd.merge(ControlRoom_data,NormalRepRegion,on="Internal Number",how="inner")
    ControlRoom_data=ControlRoom_data.drop_duplicates(subset=['Internal Number'],keep="first", inplace=False)
    ControlRoom_data=ControlRoom_data[['Region',"Type","Internal Number","ITR Number","Created User","Status","Address","Warehouse Name","Branch","Date_Time","Date" ,"Time",'Control Cut',"DEPARTMENT","DEPARTMENT 2"]]

    Control_Pivot=pd.pivot_table(ControlRoom_data,index=["Warehouse Name",'Date',"Type"],values='Time',aggfunc=np.max)
    Control_Pivot= Control_Pivot.reset_index()
    Control_Pivot=Control_Pivot.rename(columns={'Time':"Max"})
    ControlRoom_data=pd.merge(ControlRoom_data,Control_Pivot,on=["Warehouse Name","Date","Type"],how="left")
    ControlRoom_data['Time'] = ControlRoom_data['Time'].astype(str)
    ControlRoom_data['Control Cut'] = ControlRoom_data['Control Cut'].astype(str)
    ControlRoom_data['Control CutOFF'] = np.where(ControlRoom_data['Time']>ControlRoom_data['Control Cut'],0, 1)
    print(ControlRoom_data[ControlRoom_data['Internal Number'] == 257904])

    ControlRoom_data["Type"]=np.where((ControlRoom_data["Type"]=="Second") & ~(ControlRoom_data["Branch"].isin(branchesfilter)),"First",ControlRoom_data["Type"])
    filtercontrol = ['CONTROL ROOM','ALL']
    issuesControl = ControlRoom_data[((ControlRoom_data['DEPARTMENT'].isin(filtercontrol)) | (ControlRoom_data['DEPARTMENT 2'].isin(filtercontrol)))]
    issuesControl = issuesControl["ITR Number"].to_list()
    ControlRoom_data = ControlRoom_data[~ControlRoom_data["ITR Number"].isin(issuesControl)]

    
    ###Grouping per Department and region
    count_summary=pd.pivot_table(ControlRoom_data,index=['Type', "Region"],values='Control CutOFF',aggfunc='count',margins=True)
    count_summary.reset_index()

    sum_summary=pd.pivot_table(ControlRoom_data,index=['Type', "Region"],values='Control CutOFF',aggfunc=np.sum,margins=True)
    sum_summary.reset_index()

    count_ITR=pd.pivot_table(ControlRoom_data,index=['Type', "Region"],values="ITR Number",aggfunc=np.count_nonzero,margins=True)
    count_ITR.reset_index()

    ###Merging the data
    Summary_control=pd.merge(sum_summary,count_summary,on=["Type","Region"])
    Summary_control["%_ge Efficiency"]=Summary_control["Control CutOFF_x"]/Summary_control["Control CutOFF_y"]
    Summary_control=pd.merge(Summary_control,count_ITR,on=["Type","Region"],how="left")
    Summary_control.drop(["Control CutOFF_x","Control CutOFF_y"], axis='columns', inplace=True)
    format_dict = { '%_ge Efficiency': '{:.2%}'}
    Summary_control.style.format(format_dict)

    """ 6.Packaging Cut Off """
    Packaging_data=pd.concat([packaging2,packaging1])
    Packaging_data=Packaging_data[Packaging_data.Status=="Rep Sent to Branch"]
    Packaging_data=Packaging_data.rename(columns={"ITR No":"Internal Number"})
    Packaging_data['Internal Number'] = Packaging_data['Internal Number'].astype(int)
    NormalRepRegion['Internal Number'] = NormalRepRegion['Internal Number'].astype(int)
    Packaging_data=pd.merge(Packaging_data,NormalRepRegion,on="Internal Number",how="inner")
    Packaging_data=Packaging_data.drop_duplicates(subset=['Internal Number'],keep="first", inplace=False)
    Packaging_data=Packaging_data[['Region',"Type","Internal Number","ITR Number","Created User","Status","Address","Warehouse Name","Branch","Date_Time","Date" ,"Time",'Packaging Cut',"DEPARTMENT","DEPARTMENT 2"]]

    Packaging_Pivot=pd.pivot_table(Packaging_data,index=["Warehouse Name",'Date',"Type"],values='Time',aggfunc=np.max)
    Packaging_Pivot= Packaging_Pivot.reset_index()
    Packaging_Pivot=Packaging_Pivot.rename(columns={'Time':"Max"})
    Packaging_data=pd.merge(Packaging_data,Packaging_Pivot,on=["Warehouse Name","Date","Type"],how="left")
    Packaging_data['Time'] = Packaging_data['Time'].astype(str)
    Packaging_data['Packaging Cutoff'] = np.where(Packaging_data['Time']>Packaging_data['Packaging Cut'],0, 1)

    Packaging_data["Type"]=np.where((Packaging_data["Type"]=="Second") & ~(Packaging_data["Branch"].isin(branchesfilter)),"First",Packaging_data["Type"])
    filterpack = ['PACKAGING','ALL']
    issuesPack = Packaging_data[((Packaging_data['DEPARTMENT'].isin(filterpack)) | (Packaging_data['DEPARTMENT 2'].isin(filterpack)))]
    issuesPack = issuesPack["ITR Number"].to_list()

    Packaging_data = Packaging_data[~Packaging_data["ITR Number"].isin(issuesPack)]

    ###Grouping per Department and region
    count_summary=pd.pivot_table(Packaging_data,index=['Type', "Region"],values='Packaging Cutoff',aggfunc='count',margins=True)
    count_summary.reset_index()

    sum_summary=pd.pivot_table(Packaging_data,index=['Type', "Region"],values='Packaging Cutoff',aggfunc=np.sum,margins=True)
    sum_summary.reset_index()

    count_ITR=pd.pivot_table(Packaging_data,index=['Type', "Region"],values="ITR Number",aggfunc=np.count_nonzero,margins=True)
    count_ITR.reset_index()

    ###Merging the data
    Summary_packaging=pd.merge(sum_summary,count_summary,on=["Type","Region"])
    Summary_packaging["%_ge Efficiency"]=Summary_packaging["Packaging Cutoff_x"]/Summary_packaging["Packaging Cutoff_y"]
    Summary_packaging=pd.merge(Summary_packaging,count_ITR,on=["Type","Region"],how="left")
    Summary_packaging.drop(["Packaging Cutoff_x","Packaging Cutoff_y"], axis='columns', inplace=True)
    format_dict = { '%_ge Efficiency': '{:.2%}'}
    Summary_packaging.style.format(format_dict)

    BRS = pd.DataFrame([['1.', "Brs Summary Report for the Period "]])
    Mainstore = pd.DataFrame([['2.', "Mainstore Summary Report for the Period "]])
    Designerstore = pd.DataFrame([['3.', "Designerstore Summary Report for the Period "]])
    Lenstore = pd.DataFrame([['4.', "Lenstore Summary Report for the Period "]])
    Control = pd.DataFrame([['5.', "ControlRoom Summary Report for the Period "]])
    Packaging = pd.DataFrame([['6.', "Packaging Summary Report for the Period "]])
    print(NormalRepRegion)

    import xlsxwriter
    print(xlsxwriter.__version__)
    #Create a Pandas Excel writer using XlsxWriter as the engine.
    with pd.ExcelWriter(r"/home/opticabi/Documents/optica_reports/order_efficiency/Cutoff_Summary.xlsx", engine='xlsxwriter') as writer:      
        Summary_BRS.to_excel(writer, sheet_name='BRS',startrow=0 , startcol=0)
        Summary_BRS_ug.to_excel(writer, sheet_name='BRS_UG',startrow=0 , startcol=0,index = False)
        Summary_BRS_rw.to_excel(writer, sheet_name='BRS_RW',startrow=0 , startcol=0,index = False)
        Summary_MainStore.to_excel(writer, sheet_name='Main',startrow=0 , startcol=0)
        Summary_designerStore.to_excel(writer, sheet_name='Designer',startrow=0, startcol=0)
        Summary_LensStore.to_excel(writer, sheet_name='Lens',startrow=0, startcol=0)
        Summary_control.to_excel(writer, sheet_name='Control',startrow=0, startcol=0)
        Summary_packaging.to_excel(writer, sheet_name='Packaging',startrow=0, startcol=0)  

            
    writer.save()

    def save_xls(list_dfs, xls_path):
        with ExcelWriter(xls_path) as writer:
            for n, df in enumerate(list_dfs):
                df.to_excel(writer,'sheet%s' % n)
            writer.save()


    #Create a Pandas Excel writer using XlsxWriter as the engine.
    with pd.ExcelWriter(r"/home/opticabi/Documents/optica_reports/order_efficiency/cutoff_Full_Report.xlsx", engine='xlsxwriter') as writer: 
      
        BRS.to_excel(writer, sheet_name='Summary Cut Off',startrow=0 , startcol=0,header=False,index=False)
        Summary_BRS.to_excel(writer, sheet_name='Summary Cut Off',startrow=2 , startcol=0,header="Brs Summary")
        
        Mainstore.to_excel(writer, sheet_name='Summary Cut Off',startrow=0 , startcol=5,header=False,index=False)
        Summary_MainStore.to_excel(writer, sheet_name='Summary Cut Off',startrow=2 , startcol=5)
        
        Designerstore.to_excel(writer, sheet_name='Summary Cut Off',startrow=0 , startcol=10,header=False,index=False)
        Summary_designerStore.to_excel(writer, sheet_name='Summary Cut Off',startrow=2, startcol=10)
        
        Lenstore.to_excel(writer, sheet_name='Summary Cut Off',startrow=0 , startcol=15,header=False,index=False)
        Summary_LensStore.to_excel(writer, sheet_name='Summary Cut Off',startrow=2, startcol=15)
        
        Control.to_excel(writer, sheet_name='Summary Cut Off',startrow=0 , startcol=20,header=False,index=False)
        Summary_control.to_excel(writer, sheet_name='Summary Cut Off',startrow=2, startcol=20)
        
        Packaging.to_excel(writer, sheet_name='Summary Cut Off',startrow=0 , startcol=25,header=False,index=False)
        Summary_packaging.to_excel(writer, sheet_name='Summary Cut Off',startrow=2, startcol=25)
        NormalRepRegion.to_excel(writer, sheet_name='BRS Data')
        NormalRepRegion_ug.to_excel(writer, sheet_name='BRS Data UG')
        NormalRepRegion_rw.to_excel(writer, sheet_name='BRS Data RW')
        MainStore_data.to_excel(writer, sheet_name='MainStore Data')
        DesinerStore_data.to_excel(writer, sheet_name='DesignerStore Data')
        ControlRoom_data.to_excel(writer, sheet_name='ControlRoom Data')
        Packaging_data.to_excel(writer, sheet_name='Packaging Data')
        LensStore_data.to_excel(writer, sheet_name='Lens Data')  
        
    writer.save()
    def save_xls(list_dfs, xls_path):
        with ExcelWriter(xls_path) as writer:
            for n, df in enumerate(list_dfs):
                df.to_excel(writer,'sheet%s' % n)
            writer.save()            


# cutoff()       