import pandas as pd
from airflow.models import variable
import os
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from sub_tasks.libraries.utils import create_unganda_engine
from kenya_automation.alex_daily_report.data.fetch_data import stb_rab_data
from sub_tasks.libraries.time_diff import calculate_time_difference,calculate_time_difference1
from sub_tasks.libraries.styles import ug_styles, properties
from kenya_automation.stb_rab.smtp import send_to_branches
from sub_tasks.libraries.utils import uganda_path
from reports.draft_to_upload.data.fetch_data import fetch_branch_data,fetch_working_hours
from reports.stb_rab.report import daily_stb_delays

engine = create_unganda_engine()

"""  STB RAB Report tracks if we are able to meet the customers TAT withing the promised time. 
Within different branches, we can see the % of orders that do not meet their TAT and also where the issue takes place """

def working_hours() -> pd.DataFrame:
    working_hours = fetch_working_hours(
        engine= engine
    )
    return working_hours

def uganda_daily_stb_delays():
    daily_stb_delays(database = 'mawingu',engine = engine,working_hours = working_hours(),country = 'Uganda',path = uganda_path)


def branch_data():
    branch_data= fetch_branch_data(engine = engine, database = "reports_tables" )
    return branch_data


def send_to_uganda_branches():
    send_to_branches(
        path=uganda_path,
        country = 'Uganda',
        branch_data = branch_data()
    )

# uganda_daily_stb_delays()    




