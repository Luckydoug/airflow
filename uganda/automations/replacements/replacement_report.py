import pandas as pd
import numpy as np
from airflow.models import variable
import os
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from sub_tasks.libraries.utils import create_unganda_engine
from kenya_automation.alex_daily_report.data.fetch_data import replacement
from sub_tasks.libraries.time_diff import calculate_time_difference,calculate_time_difference1
from sub_tasks.libraries.styles import ug_styles, properties
from uganda.automations.replacements.smtp import send_to_branches
from sub_tasks.libraries.utils import uganda_path
from reports.draft_to_upload.data.fetch_data import fetch_branch_data,fetch_working_hours

connection = create_unganda_engine()

""" This checks if all replacements are done on the same day """
def working_hours() -> pd.DataFrame:
    working_hours = fetch_working_hours(
        engine= connection
    )
    return working_hours

def daily_replacements():
    itr_replacement = replacement(database="mawingu",engine=connection)
    itr_replacement = itr_replacement.drop_duplicates(subset = ['Item No.','ITR Number','ITR Date'],keep = 'first')
    itr_replacement['ITR Items'] = itr_replacement.groupby('ITR Number')['ITR Number'].transform("count")
    daily_replacement = itr_replacement.drop_duplicates(subset = 'ITR Number',keep = 'first')
    daily_replacement = daily_replacement[['ITR Number','Order Number' ,'ITR Items','Exchange Type', 'Sales Branch', 'Creation Date_Time',
       'Sent to Branch Date_Time','Received at Branch Date_Time','difference','day_difference','duration' ]]
    
    daily_replacement = daily_replacement.rename(columns = {'Sales Branch':'Outlet'})
    daily_replacement['creation_time_rab'] = daily_replacement.apply(lambda row:calculate_time_difference(row, "Creation Date_Time",'Received at Branch Date_Time',country= 'Uganda',working_hours=working_hours()), axis=1)
    daily_replacement['creation_time_rab_Hrs'] = (daily_replacement['creation_time_rab']/60)/24

    daily_replacement['creation_time_rab_Hrs1'] = daily_replacement['creation_time_rab_Hrs'].round(0)
    Not_received_at_branch = daily_replacement[daily_replacement['duration'] == 'Not Received at Branch']

    summary = daily_replacement.pivot_table(index= 'Outlet',columns = 'duration',aggfunc = {'ITR Number':np.count_nonzero}).reset_index().fillna(0).rename(columns = {'ITR Number':'Replacements'})
    
    with pd.ExcelWriter(r"/home/opticabi/Documents/uganda_reports/replacements/Replacement Data.xlsx",engine = 'xlsxwriter') as writer:
        summary.to_excel(writer,sheet_name = 'Summary',index = 'False')
        Not_received_at_branch.to_excel(writer,sheet_name = 'Not Received',index = 'False')
        daily_replacement.to_excel(writer,sheet_name = 'Data',index = 'False')

    """  Convert tables to HTML so I can apply on my html"""   
    summary_style = summary.style.hide_index().set_table_styles(ug_styles)
    summary_html = summary_style.to_html(doctype_html=True)

    Not_received_at_branch_style = Not_received_at_branch.style.hide_index().set_table_styles(ug_styles)
    Not_received_at_branch = Not_received_at_branch_style.to_html(doctype_html=True)


def branch_data():
    branch_data= fetch_branch_data(engine = connection, database = "reports_tables" )
    return branch_data


def send__replacements_to_uganda_branches():
    send_to_branches(
        path=uganda_path,
        branch_data = branch_data()
    )




    
