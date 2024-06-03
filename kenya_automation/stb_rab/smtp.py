import pandas as pd
from airflow.models import variable
import os
import datetime
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import smtplib, ssl
from datetime import timedelta
from sub_tasks.libraries.styles import ug_styles, properties
from sub_tasks.libraries.utils import (save_file,return_sent_emails)
from reports.draft_to_upload.utils.utils import return_report_daterange
from reports.draft_to_upload.utils.utils import get_report_frequency
from dags.env import (wairimu_password,wairimu_email)
from sub_tasks.libraries.utils import get_todate,send_report,assert_date_modified, create_initial_file, return_sent_emails, record_sent_branch, fetch_gsheet_data, fourth_week_start, fourth_week_end


selection = get_report_frequency()

if selection == 'Weekly':
    start_date = fourth_week_start
    yesterday = fourth_week_end
elif selection == 'Daily':
    start_date = return_report_daterange(selection)
    yesterday = start_date

print(start_date)
print(yesterday)


def send_to_branches(
    path: str,
    country,
    branch_data: pd.DataFrame
) -> None:

    overall = f"{path}stb_rab_report/STB_RAB Summary.xlsx"
    delayed_orders = pd.ExcelFile(overall) 
    delays = delayed_orders.parse('Delays', index_col=False)
    stb_rab_autotime_incorrect = delayed_orders.parse('Long TAT Promised',index_col=False)
    to_attach = delayed_orders.parse('Where the Delay Occurred',index_col=False)

    branch_list = branch_data["Outlet"].tolist()
    # 
    branch_data = branch_data.set_index("Outlet")

    for branch in branch_list:
        if branch in stb_rab_autotime_incorrect['Outlet'].to_list():
            branch_name = branch_data.loc[branch, "Branch"]
            branch_email = branch_data.loc[branch, "Email"]
            rm_email = branch_data.loc[branch, "RM Email"]
            srm_email = branch_data.loc[branch, "SRM Email"]

            overall_delays = delays[delays["Outlet"] == branch] 
            test = overall_delays.copy()         
            overall_delays = overall_delays.style.hide_index().set_properties(**properties).set_table_styles(ug_styles)
            if not len(test):
                overall_delays_html = "<b> No delays were recorded </b>"
            else:
                overall_delays_html = overall_delays.to_html(doctype_html=True )

            incorrect_tat = stb_rab_autotime_incorrect[stb_rab_autotime_incorrect["Outlet"] == branch]               
            incorrect_tat = incorrect_tat.style.hide_index().set_properties(**properties).set_table_styles(ug_styles)
            incorrect_tat_html = incorrect_tat.to_html(doctype_html=True )


            """ Creating the HTML"""   
            html = """
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta http-equiv="X-UA-Compatible" content="IE=edge">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Document</title>
                <style>
                    table {{border-collapse: collapse;font-family:Times New Roman; font-size:9}}
                    th {{text-align: left;font-family:Times New Roman; font-size:9; padding: 4px;}}
                    body, p, h3, div, span, var {{font-family:Times New Roman; font-size:13}}
                    td {{text-align: left;font-family:Times New Roman; font-size:9; padding: 8px;}}
                    h4 {{font-size: 12px; font-family: Verdana, sans-serif;}}
                </style>
            </head>
            <body>
                <p><b>Hi {branch_name},</b></p> </br>
                <p><i>I Hope this email finds you well.</i></p> </br>
                <p>Kindly see orders that were not received in branch or have not been processed on time and share your comments</p>
                <table style = "width: 70%;">{overall_delays_html}</table> 
                <p>For details refer to the attached file</p>
                <br>
                <b><i>Best Regards</i></b><br>
                <b><i>Wanjiru Kinyara</i></b>
            </body>
            </html>
            """.format(branch_name = branch_name,overall_delays_html = overall_delays_html,incorrect_tat_html = incorrect_tat_html)

            email = os.getenv("wanjiru_email")
            password = os.getenv("wanjiru_password")
            
            if country == 'Kenya':
                # receiver_email = ['wairimu@optica.africa']
                receiver_email = [rm_email,branch_email,srm_email,'wairimu@optica.africa']
            else:
                # receiver_email = ['wairimu@optica.africa']
                receiver_email = [rm_email,branch_email,'raghav@optica.africa','wairimu@optica.africa']                       


            email_message = MIMEMultipart("alternative")
            email_message["From"] = email
            email_message["To"] = r','.join(receiver_email)
            email_message["Subject"] = f"{branch_name} Daily Sent to Branch & Received at Branch Performance from {start_date} to {yesterday}"
            email_message.attach(MIMEText(html, "html"))

            export_data = delayed_orders.parse('Where the Delay Occurred')
            branch_export = export_data[export_data["Outlet"] == branch]
            save_file(
                email_message=email_message,
                reports={"Data": branch_export},
                branch_name=branch_name,
                file_name="STB and RAB Report.xlsx",
                path=f"{path}stb_rab_report")               


            context = ssl.create_default_context()
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                server.login(email, password)
                server.sendmail(email,receiver_email,email_message.as_string())
                server.quit() 

         


if __name__ == '__main__':
    send_to_branches()