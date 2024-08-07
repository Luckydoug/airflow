import pandas as pd
from airflow.models import variable
from sub_tasks.libraries.utils import create_unganda_engine
from kenya_automation.alex_daily_report.data.fetch_data import stb_rab_data
from sub_tasks.libraries.time_diff import calculate_time_difference,calculate_time_difference1
from sub_tasks.libraries.styles import ug_styles
from kenya_automation.alex_daily_report.data.smtp import send_to_branches
from sub_tasks.libraries.utils import uganda_path
from reports.draft_to_upload.data.fetch_data import fetch_branch_data,fetch_working_hours

rwanda_engine = create_unganda_engine()

"""  STB RAB Report tracks if we are able to meet the customers TAT withing the promised time. 
Within different branches, we can see the % of orders that do not meet their TAT and also where the issue takes place """

def working_hours() -> pd.DataFrame:
    working_hours = fetch_working_hours(
        engine= rwanda_engine
    )
    return working_hours

def daily_stb_delays():
    stb_rab = stb_rab_data(database="mawingu",engine=rwanda_engine)
    stb_rab = stb_rab.drop_duplicates(subset = 'doc_no',keep = 'first')
    stb_rab = stb_rab[stb_rab['ods_outlet'] != '0MA']
    dropcriteria = ('OTC','Spare Part From Branch')
    stb_rab = stb_rab[~stb_rab['ods_ordercriteriastatus'].isin(dropcriteria)]
    stb_rab = stb_rab.rename(columns = {"ods_outlet":"Outlet"})



    """ Calculating Time Difference from Difference Statuses """
    stb_rab['draft_upload_time'] = stb_rab.apply(lambda row:calculate_time_difference(row , "draft_dt",'upload_dt',country= 'Uganda',working_hours=working_hours()), axis=1)
    stb_rab['upload_so_time'] = stb_rab.apply(lambda row:calculate_time_difference(row, "upload_dt",'so_dt',country= 'Uganda',working_hours=working_hours()), axis=1)
    stb_rab['so_senttopreqc_time'] = stb_rab.apply(lambda row:calculate_time_difference(row, "so_dt",'senttopreqc_dt',country= 'Uganda',working_hours=working_hours()), axis=1)
    stb_rab['senttopreqc_assignedtotech_dt'] = stb_rab.apply(lambda row:calculate_time_difference(row, "senttopreqc_dt",'assignedtotech_dt',country= 'Uganda',working_hours=working_hours()), axis=1)
    stb_rab['assignedtotech_stp_dt'] = stb_rab.apply(lambda row:calculate_time_difference(row, "assignedtotech_dt",'stp_dt',country= 'Uganda',working_hours=working_hours()), axis=1)
    stb_rab['stp_stb_dt'] = stb_rab.apply(lambda row:calculate_time_difference(row, "stp_dt",'stb_dt',country= 'Uganda',working_hours=working_hours()), axis=1)
    stb_rab['stb_rab_dt'] = stb_rab.apply(lambda row:calculate_time_difference(row, "stb_dt",'rab_dt',country= 'Uganda',working_hours=working_hours()), axis=1)
    stb_rab['Delayed by Mins'] = stb_rab.apply(lambda row:calculate_time_difference(row, "collection_dt",'rab_dt',country= 'Uganda',working_hours=working_hours()), axis=1)


    """ Calculating the Auto Times """

    stb_rab['order_hours'] = pd.to_numeric(stb_rab['order_hours'])
    print(working_hours())
    stb_rab['starttime_collection_dt'] = stb_rab.apply(lambda row:calculate_time_difference1(row, "start_cal_coll_time",'collection_dt',country= 'Uganda',working_hours=working_hours()), axis=1)
    stb_rab['starttime_collection_hours'] = (stb_rab['starttime_collection_dt'] /60).round(0)


    """ Creating new columns if certain conditions are met """
    stb_rab['Achieved <= Auto time'] = stb_rab.apply(lambda row: 1 if row['starttime_collection_hours'] < row['order_hours'] else 0,axis = 1)
    stb_rab['Achieved > Auto time (1 Hour)'] = stb_rab.apply(lambda row: 1 if abs(row['order_hours'] - row['starttime_collection_hours']) > 1 else 0,axis= 1)
    stb_rab['More or Less'] = stb_rab.apply(lambda row: "More" if row['starttime_collection_hours'] > row['order_hours'] else ("Less" if row['starttime_collection_hours'] < row['order_hours'] else "Equal"), axis=1)

    stb_rab = stb_rab.rename(columns = {'doc_no':'Order Number','user_name':'Order Creator','order_hours':'Hours Recommended on WebApp','ods_status':'Status',
                                        'ods_ordercriteriastatus':'Order Criteria','starttime_collection_hours':'Promised Delivery Hours',
                                    'collection_dt':'Promised Collection Date & Time','start_cal_coll_time':'Start Date_Time',
                                    'stb_dt':'Sent to Branch (STB) Date & Time','rab_dt':'Received at Branch (RAB) Date & Time'})


    """ Comparison to the Autotime Given on SAP """
    stb_rab_autotime = stb_rab[['Order Number', 'Outlet', 'Order Creator','Status', 
                                'Order Criteria','Mode of Pay','Start Date_Time', 'Promised Collection Date & Time',
                                'Hours Recommended on WebApp','Promised Delivery Hours','Achieved > Auto time (1 Hour)','STB On Time', 'RAB On Time',
                                'More or Less']]
    stb_rab_autotime_incorrect = stb_rab_autotime[stb_rab_autotime['Achieved > Auto time (1 Hour)'] == 1]

    stb_rab_autotime_incorrect = stb_rab_autotime_incorrect[['Order Number', 'Outlet', 'Order Creator','Status', 
                                'Order Criteria','Mode of Pay','Start Date_Time', 'Promised Collection Date & Time',
                                'Hours Recommended on WebApp','Promised Delivery Hours','STB On Time', 'RAB On Time',
                                'More or Less']]
    
    

    """ Delays """
    delays_df = stb_rab[stb_rab['Delayed by Mins'] != 0.0]
    delays = delays_df[['Order Number', 'Outlet', 'Order Creator',
                   'Order Criteria','Mode of Pay', 'Start Date_Time',
                    'Sent to Branch (STB) Date & Time','Received at Branch (RAB) Date & Time',
                   'Promised Collection Date & Time', 'Hours Recommended on WebApp','Promised Delivery Hours',              
                    'STB On Time', 'RAB On Time',
                    'Delayed by Mins','draft_upload_time', 'upload_so_time',
                   'so_senttopreqc_time', 'senttopreqc_assignedtotech_dt',
                   'assignedtotech_stp_dt', 'stp_stb_dt', 'stb_rab_dt']]
    

    """ Raghav's Format"""
    raghav_format = delays_df[['Outlet','Order Number','Sent to Branch (STB) Date & Time','Received at Branch (RAB) Date & Time',
                        'Promised Collection Date & Time','Delayed by Mins']]
    raghav_format = raghav_format[raghav_format['Sent to Branch (STB) Date & Time'].notna()]


    """ Summary """
    stb_rab_summary_branch = stb_rab.pivot_table(index='Outlet', values=['Order Number',"STB On Time","RAB On Time"]
                 , aggfunc={
                            'Order Number':'count',
                             "STB On Time": lambda x: (x == "No").sum(),
                              "RAB On Time": lambda x: (x == "No").sum(),
                          }, 
                          fill_value='', margins=True,
                          margins_name='Total').reset_index()


    stb_rab_summary_branch["STB Delay %"] = round((stb_rab_summary_branch[( 'STB On Time')] / stb_rab_summary_branch[("Order Number")]) * 100,2)
    stb_rab_summary_branch["RAB Delay %"] = round((stb_rab_summary_branch[( 'RAB On Time')] / stb_rab_summary_branch[("Order Number")]) * 100,2)

    stbdelay = stb_rab[stb_rab["STB On Time"] == "No"]
    if not stbdelay.empty:    
        criteria_stb =  pd.pivot_table(stbdelay, index='ods_ordercriteriastatus', values='Order Number', columns="Outlet", aggfunc='count', margins=True, margins_name='Grand Total')
        criteria_stb = criteria_stb.fillna(0)
        criteria_stb = criteria_stb.sort_values(by = 'Grand Total',ascending = False)
    else:
        criteria_stb = pd.DataFrame({'No STB Delays': ['No Delay Took place']})

    rabdelay = stb_rab[stb_rab["RAB On Time"] == "No"]
    if not stbdelay.empty:
        criteria_rab =  pd.pivot_table(rabdelay, index='ods_ordercriteriastatus', values='Order Number', columns="Outlet", aggfunc='count', margins=True, margins_name='Grand Total')
        criteria_rab = criteria_rab.fillna(0)
        criteria_rab = criteria_rab.sort_values(by = 'Grand Total',ascending = False)
        criteria_rab
    else:
        criteria_rab = pd.DataFrame({'No RAB Delays': ['No Delay Took place']}) 

    with pd.ExcelWriter(r"/home/opticabi/Documents/uganda_reports/stb_rab_report\STB_RAB Summary UG.xlsx", engine='xlsxwriter') as writer:
        stb_rab_summary_branch.to_excel(writer, sheet_name="STB Summary", index=False)
        criteria_stb.to_excel(writer, sheet_name="STB Summary by Criteria", index=True)
        raghav_format.to_excel(writer, sheet_name="Delays", index=False)
        delays.to_excel(writer, sheet_name="Where the Delay Occurred", index=False)
        stb_rab_autotime_incorrect.to_excel(writer, sheet_name="Long TAT Promised", index=False)
        stb_rab_autotime.to_excel(writer, sheet_name='Master Data',index=False) 

    """  Convert tables to HTML so I can apply on my html"""   
    delays_style = raghav_format.style.hide_index().set_table_styles(ug_styles)
    delays_html = delays_style.to_html(doctype_html=True)  


def branch_data():
    branch_data= fetch_branch_data(engine = rwanda_engine, database = "reports_tables" )
    return branch_data


def send_to_uganda_branches():
    send_to_branches(
        path=uganda_path,
        branch_data = branch_data()
    )





