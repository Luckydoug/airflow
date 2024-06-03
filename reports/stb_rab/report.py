import pandas as pd
import numpy as np
from airflow.models import variable
import os
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from sub_tasks.libraries.utils import create_unganda_engine
from kenya_automation.alex_daily_report.data.fetch_data import stb_rab_data
from sub_tasks.libraries.time_diff import calculate_time_difference,calculate_time_difference1
from reports.draft_to_upload.data.fetch_data import fetch_branch_data,fetch_working_hours
from sub_tasks.libraries.utils import fourth_week_start, fourth_week_end
from reports.draft_to_upload.utils.utils import return_report_daterange
from reports.draft_to_upload.utils.utils import get_report_frequency
from sub_tasks.libraries.utils import createe_engine
from sub_tasks.libraries.utils import path
from pangres import upsert
import datetime


selection = get_report_frequency()

if selection == 'Weekly':
    start_date = fourth_week_start
    yesterday = fourth_week_end
elif selection == 'Daily':
    start_date = return_report_daterange(selection)
    yesterday = start_date    
print(start_date)
print(yesterday)


def daily_stb_delays(database,engine,working_hours,country,path):
    hqorders  = f"""select * 
                from mabawa_mviews.delayedorders
                where collection_dt::date between '2024-05-04' and '2024-05-04'  
                and ods_outlet = 'MSA'
                """
    stb_rab = pd.read_sql_query(hqorders,con = createe_engine() )
    print(stb_rab)
    drop = ('0MA','Uganda','Rwanda')
    stb_rab = stb_rab[~stb_rab['ods_outlet'].isin(drop)]
    stb_rab = stb_rab.rename(columns = {"ods_outlet":"Outlet"})


    """ Calculating Time Difference from Difference Statuses """
    """Branch"""
    stb_rab['starttime_to_so'] = stb_rab.apply(lambda row:calculate_time_difference(row , "start_cal_coll_time",'so_dt',country= country,working_hours=working_hours), axis=1)
    stb_rab['starttime_to_ppf'] = stb_rab.apply(lambda row:calculate_time_difference(row, "so_dt",'printedpf_dt',country= country,working_hours=working_hours), axis=1)
    stb_rab['ppf_pfsenttohq'] = stb_rab.apply(lambda row:calculate_time_difference(row, "printedpf_dt",'pfsenttohq_dt',country= country,working_hours=working_hours), axis=1)    
    stb_rab['pfsenttohq_pfreceivedathq'] = stb_rab.apply(lambda row:calculate_time_difference(row, "pfsenttohq_dt",'frameatlenstore_dt',country= country,working_hours=working_hours,outlet = 'ods_orderprocess_branch'), axis=1)

    """ Main Store"""
    stb_rab['so_opmain'] = stb_rab.apply(lambda row:calculate_time_difference(row, "so_dt",'opmain_dt',country= country,working_hours=working_hours,outlet='department'), axis=1)
    stb_rab['opmain_lenstore'] = stb_rab.apply(lambda row:calculate_time_difference(row, "opmain_dt",'frameatlenstore_dt',country= country,working_hours=working_hours,outlet='department'), axis=1)
    
    """ Designer Store"""
    stb_rab['so_opdesigner'] = stb_rab.apply(lambda row:calculate_time_difference(row, "so_dt",'opdesigner_dt',country= country,working_hours=working_hours,outlet='department'), axis=1)
    stb_rab['so_lenstore'] = stb_rab.apply(lambda row:calculate_time_difference(row, "opdesigner_dt",'frameatlenstore_dt',country= country,working_hours=working_hours,outlet = 'ods_orderprocess_branch'), axis=1)

    """ Lens Store"""
    stb_rab['pfreceivedathq_pfreceivedatlenstore'] = stb_rab.apply(lambda row:calculate_time_difference(row, "frameatlenstore_dt",'pfreceivedatlenstore_dt',country= country,working_hours=working_hours,outlet = 'ods_orderprocess_branch'), axis=1)
    stb_rab['pfreceivedatlenstore_oplens'] = stb_rab.apply(lambda row:calculate_time_difference(row, "pfreceivedatlenstore_dt",'oplens_dt',country= country,working_hours=working_hours,outlet = 'ods_orderprocess_branch'), axis=1)
    stb_rab['oplens_spc'] = stb_rab.apply(lambda row:calculate_time_difference(row, "oplens_dt",'stc_dt',country= country,working_hours=working_hours,outlet = 'ods_orderprocess_branch'), axis=1)
    
    """ Overseas Desk"""  
    if stb_rab['ods_ordercriteriastatus'].str.contains('Overseas', case=False, regex=True).any():
        stb_rab.loc[:, 'overseas_gpo'] = stb_rab.apply(lambda row: calculate_time_difference(row, "frameatlenstore_dt", 'gpo_dt', country=country, working_hours=working_hours, outlet='ods_orderprocess_branch'), axis=1)
        stb_rab.loc[:, 'gpo_grpo'] = stb_rab.apply(lambda row: calculate_time_difference(row, "gpo_dt", 'grpo_dt', country=country, working_hours=working_hours, outlet='ods_orderprocess_branch'), axis=1)
        stb_rab.loc[:, 'overseas_stc'] = stb_rab.apply(lambda row: calculate_time_difference(row, "grpo_dt", 'stc_dt', country=country, working_hours=working_hours, outlet='ods_orderprocess_branch'), axis=1)
    else:
        stb_rab.loc[:, 'overseas_gpo'] = 0
        stb_rab.loc[:, 'gpo_grpo'] = 0
        stb_rab.loc[:, 'overseas_stc'] = 0


    """Other Departments"""
    if stb_rab['ods_ordercriteriastatus'].str.contains('Overseas', case=False, regex=True).any():
        stb_rab.loc[:, 'lenstore_stc'] = 0
    else:
        stb_rab['lenstore_stc'] = stb_rab.apply(lambda row: calculate_time_difference(row, "frameatlenstore_dt", 'stc_dt', country=country, working_hours=working_hours, outlet='department'), axis=1)
    
    stb_rab['stc_preqc'] = stb_rab.apply(lambda row:calculate_time_difference(row, "stc_dt",'senttopreqc_dt',country= country,working_hours=working_hours,outlet='department'), axis=1)
    stb_rab['preqc_assignedtech'] = stb_rab.apply(lambda row:calculate_time_difference(row, "senttopreqc_dt",'assignedtotech_dt',country= country,working_hours=working_hours,outlet='department'), axis=1)
    stb_rab['assignedtech_stp'] = stb_rab.apply(lambda row:calculate_time_difference(row, "assignedtotech_dt",'stp_dt',country= country,working_hours=working_hours,outlet='department'), axis=1)
    stb_rab['stp_stb'] = stb_rab.apply(lambda row:calculate_time_difference(row, "stp_dt",'stb_dt',country= country,working_hours=working_hours,outlet='department'), axis=1)
    stb_rab['stb_rab'] = stb_rab.apply(lambda row:calculate_time_difference(row, "stb_dt",'rab_dt',country= country,working_hours=working_hours), axis=1)
    stb_rab['Delayed by Mins'] = stb_rab.apply(lambda row:calculate_time_difference(row, "collection_dt",'rab_dt',country= country,working_hours=working_hours), axis=1)
    stb_rab['Delayed by Mins_Rider Time Considered'] = stb_rab.apply(lambda row:calculate_time_difference(row, "adjusted_stb",'rab_dt',country= country,working_hours=working_hours), axis=1)


    """Rejected Orders"""
    stb_rab['rejcontrol_resentpreqc'] = stb_rab.apply(lambda row:calculate_time_difference(row, "rejectedpreqc_dt",'resenttopreqc_dt',country= country,working_hours=working_hours), axis=1)

    """ Calculating the Auto Times """
    stb_rab['order_hours'] = pd.to_numeric(stb_rab['order_hours'])
    stb_rab['starttime_collection_dt'] = stb_rab.apply(lambda row:calculate_time_difference1(row, "start_cal_coll_time",'collection_dt',country= country,working_hours=working_hours), axis=1)
    stb_rab['starttime_collection_hours'] = (stb_rab['starttime_collection_dt'] /60).round(0)


    """ Creating new columns if certain conditions are met """
    stb_rab['Achieved <= Auto time'] = stb_rab.apply(lambda row: 1 if row['starttime_collection_hours'] < row['order_hours'] else 0,axis = 1)
    stb_rab['Achieved > Auto time_1 Hour'] = stb_rab.apply(lambda row: 1 if abs(row['order_hours'] - row['starttime_collection_hours']) > 1 else 0,axis= 1)
    stb_rab['More or Less'] = stb_rab.apply(lambda row: "More" if row['starttime_collection_hours'] > row['order_hours'] else ("Less" if row['starttime_collection_hours'] < row['order_hours'] else "Equal"), axis=1)
    
    print(stb_rab)
    print(stb_rab.columns)

# def upsert_stb_efficiency():
    # from sub_tasks.libraries.utils import createe_engine
    # from sub_tasks.data.connect import engine
    from sub_tasks.libraries.utils import path
#     data  = daily_stb_delays(database = 'mabawa',
#                     engine = createe_engine(),
#                     working_hours = fetch_working_hours(engine= createe_engine()),
#                     country = 'Kenya',
#                     path = path)
#     print(data)
#     print(data.columns)
    stb_rab = stb_rab.drop_duplicates(subset='doc_entry',keep = 'first')
    stb_rab = stb_rab.set_index(['doc_entry'])
    print(stb_rab.columns)
    upsert(engine=engine,
    df=stb_rab,
    schema='mabawa_dw',
    table_name='stb_efficiency',
    if_row_exists='update',
    create_table=False)   

daily_stb_delays(database = 'mabawa',
                engine = createe_engine(),
                working_hours = fetch_working_hours(engine= createe_engine()),
                country = 'Kenya',
                path = path)
    


    # stb_rab = stb_rab.rename(columns = {'doc_no':'Order Number','user_name':'Order Creator','order_hours':'Hours Recommended on WebApp','ods_status':'Status',
    #                                     'ods_ordercriteriastatus':'Order Criteria','starttime_collection_hours':'Promised Delivery Hours',
    #                                 'collection_dt':'Promised Collection Date & Time','start_cal_coll_time':'Start Date_Time',
    #                                 'stb_dt':'Sent to Branch (STB) Date & Time','rab_dt':'Received at Branch (RAB) Date & Time'})


    # """ Comparison to the Autotime Given on SAP """
    # stb_rab_autotime = stb_rab[['Order Number', 'Outlet', 'Order Creator','Status', 
    #                             'Order Criteria','Mode of Pay','Start Date_Time', 'Promised Collection Date & Time',
    #                             'Hours Recommended on WebApp','Promised Delivery Hours','Achieved > Auto time (1 Hour)','STB On Time', 'RAB On Time',
    #                             'More or Less']]
    # stb_rab_autotime_incorrect = stb_rab_autotime[stb_rab_autotime['Achieved > Auto time (1 Hour)'] == 1]

    # stb_rab_autotime_incorrect = stb_rab_autotime_incorrect[['Order Number', 'Outlet', 'Order Creator','Status', 
    #                             'Order Criteria','Mode of Pay','Start Date_Time', 'Promised Collection Date & Time',
    #                             'Hours Recommended on WebApp','Promised Delivery Hours','STB On Time', 'RAB On Time',
    #                             'More or Less']]
    
    

    # """ Condition to consider for Kenya."""
    # if country == 'Kenya':
    #     stb_rab['Delay (Mins)'] = stb_rab['Delayed by Mins (Rider Time Considered)'] - stb_rab['riders_time']
    #     stb_rab['Delay (Mins)'] = stb_rab['Delay (Mins)'].round(0)
    #     stb_rab = stb_rab.sort_values(by = 'Delay (Mins)',ascending = False)
    #     delays_df = stb_rab[(stb_rab['Delay (Mins)'] > 0) & (stb_rab['Delay (Mins)'] > 0)& (stb_rab['Sent to Branch (STB) Date & Time'].notna())]
    #     delays = delays_df[['Order Number', 'Outlet', 'Order Creator',
    #             'Order Criteria','Mode of Pay', 'Start Date_Time',
    #             'Sent to Branch (STB) Date & Time','Received at Branch (RAB) Date & Time',
    #             'Promised Collection Date & Time', 'Hours Recommended on WebApp','Promised Delivery Hours',              
    #             'STB On Time', 'RAB On Time',
    #             'Delay (Mins)','draft_upload_time', 'upload_so_time',
    #             'so_senttopreqc_time', 'senttopreqc_assignedtotech_dt',
    #             'assignedtotech_stp_dt', 'stp_stb_dt', 'stb_rab_dt']]         
    #     raghav_format = delays_df[['Outlet','Order Number','Sent to Branch (STB) Date & Time','Received at Branch (RAB) Date & Time',
    #                 'Promised Collection Date & Time','Delay (Mins)']] 

        
    #     with pd.ExcelWriter(f"{path}stb_rab_report/STB_RAB Summary.xlsx", engine='xlsxwriter') as writer:
    #         raghav_format.to_excel(writer, sheet_name="Delays", index=False)   
    #         stb_rab_autotime_incorrect.to_excel(writer, sheet_name="Long TAT Promised", index=False)
    #         delays.to_excel(writer, sheet_name="Where the Delay Occurred", index=False)

    # else:
    #     """ Delays """
    #     delays_df = stb_rab[(stb_rab['Delayed by Mins'] > 0) | (stb_rab['Sent to Branch (STB) Date & Time'].isna())]
    #     delays_df['Delayed by Mins.'] = delays_df.apply(lambda row: "Not Yet Processed/Received at Branch" if (pd.isna(row['Sent to Branch (STB) Date & Time'])) else row['Delayed by Mins'],axis = 1)
    #     delays_df = delays_df.sort_values(by = 'Delayed by Mins',ascending = False)

    #     delays = delays_df[['Order Number', 'Outlet', 'Order Creator',
    #                 'Order Criteria','Mode of Pay', 'Start Date_Time',
    #                     'Sent to Branch (STB) Date & Time','Received at Branch (RAB) Date & Time',
    #                 'Promised Collection Date & Time', 'Hours Recommended on WebApp','Promised Delivery Hours',              
    #                     'STB On Time', 'RAB On Time',
    #                     'Delayed by Mins.','draft_upload_time', 'upload_so_time',
    #                 'so_senttopreqc_time', 'senttopreqc_assignedtotech_dt',
    #                 'assignedtotech_stp_dt', 'stp_stb_dt', 'stb_rab_dt']]    
        

    #     """ Raghav's Format"""
    #     raghav_format = delays_df[['Outlet','Order Number','Sent to Branch (STB) Date & Time','Received at Branch (RAB) Date & Time',
    #                         'Promised Collection Date & Time','Delayed by Mins.']] 

    #     """ Summary """
    #     stb_rab_summary_branch = stb_rab.pivot_table(index='Outlet', values=['Order Number',"STB On Time","RAB On Time"]
    #                 , aggfunc={
    #                             'Order Number':'count',
    #                             "STB On Time": lambda x: (x == "No").sum(),
    #                             "RAB On Time": lambda x: (x == "No").sum(),
    #                         }, 
    #                         fill_value='', margins=True,
    #                         margins_name='Total').reset_index()


    #     stb_rab_summary_branch["STB Delay %"] = round((stb_rab_summary_branch[( 'STB On Time')] / stb_rab_summary_branch[("Order Number")]) * 100,2)
    #     stb_rab_summary_branch["RAB Delay %"] = round((stb_rab_summary_branch[( 'RAB On Time')] / stb_rab_summary_branch[("Order Number")]) * 100,2)

    #     stbdelay = stb_rab[stb_rab["STB On Time"] == "No"]
    #     if not stbdelay.empty:    
    #         criteria_stb =  pd.pivot_table(stbdelay, index='Order Criteria', values='Order Number', columns="Outlet", aggfunc='count', margins=True, margins_name='Grand Total')
    #         criteria_stb = criteria_stb.fillna(0)
    #         criteria_stb = criteria_stb.sort_values(by = 'Grand Total',ascending = False)
    #     else:
    #         criteria_stb = pd.DataFrame({'No STB Delays': ['No Delay Took place']})

    #     rabdelay = stb_rab[stb_rab["RAB On Time"] == "No"]
    #     if not stbdelay.empty:
    #         criteria_rab =  pd.pivot_table(rabdelay, index='Order Criteria', values='Order Number', columns="Outlet", aggfunc='count', margins=True, margins_name='Grand Total')
    #         criteria_rab = criteria_rab.fillna(0)
    #         criteria_rab = criteria_rab.sort_values(by = 'Grand Total',ascending = False)
    #         criteria_rab
    #     else:
    #         criteria_rab = pd.DataFrame({'No RAB Delays': ['No Delay Took place']}) 

    #     with pd.ExcelWriter(f"{path}stb_rab_report/STB_RAB Summary.xlsx", engine='xlsxwriter') as writer:
    #         stb_rab_summary_branch.to_excel(writer, sheet_name="STB Summary", index=False)
    #         criteria_stb.to_excel(writer, sheet_name="STB Summary by Criteria", index=True)
    #         raghav_format.to_excel(writer, sheet_name="Delays", index=False)
    #         delays.to_excel(writer, sheet_name="Where the Delay Occurred", index=False)
    #         stb_rab_autotime_incorrect.to_excel(writer, sheet_name="Long TAT Promised", index=False)
    #         stb_rab_autotime.to_excel(writer, sheet_name='Master Data',index=False)

 


