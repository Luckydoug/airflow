import pandas as pd
from airflow.models import variable
import os
import datetime
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import smtplib, ssl
from sub_tasks.libraries.styles import ug_styles, properties
from sub_tasks.libraries.utils import (save_file,return_sent_emails)
from sub_tasks.libraries.utils import uganda_path

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
formatted_date = yesterday.strftime('%Y-%m-%d')

def send_to_branches(
    path: str,
    branch_data: pd.DataFrame

) -> None:
    
    overall = f"{path}replacements/Replacement Data.xlsx"
    replacements = pd.ExcelFile(overall) 
    summary = replacements.parse('Summary', index_col=False)
    not_rab = replacements.parse('Not Received',index_col=False)

    branch_list = branch_data["Outlet"].tolist()
    branch_data = branch_data.set_index("Outlet")

              
    summary_rep = summary.style.hide_index().set_properties(**properties).set_table_styles(ug_styles)
    summary_rep_html = summary_rep.to_html(doctype_html=True )

    test = not_rab.copy()         
    overall_delays = not_rab.style.hide_index().set_properties(**properties).set_table_styles(ug_styles)
    if not len(test):
        overall_delays_html = "<b> All Replacements were acted on and received at the branch. </b>"
    else:
        overall_delays_html = overall_delays.to_html(doctype_html=True )

    
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
        <p><b>Hi Larry,</b></p> </br>
        <p><i>I hope this email finds you well.</i></p> </br>
        <pThis report shows all replacements created on the stated date and if they were received on the same day or after.</p>
        <br>
        <b>1. Replacements Received at Branch and Days Taken </b>
        <table style = "width: 70%;">{summary_rep_html}</table> 
        <br>
        <b>2. Replacements Not Received within the same Day.</b>
        <table style = "empty-cells: hide !important;">{overall_delays_html}</table>
        <br>
        <p>For details refer to the attached file</p>
        <br>
        <b><i>Best Regards</i></b><br>
        <b><i>Wairimu Mathenge</i></b>
    </body>
    </html>
    """.format(
                summary_rep_html = summary_rep_html,
                overall_delays_html = overall_delays_html)

    email = os.getenv("wairimu_email")
    password = os.getenv("wairimu_password")
    

    # receiver_email = [rm_email,branch_email,'raghav@optica.africa','wanjiru.kinyara@optica.africa','yuri@optica.africa']
    # receiver_email = ['ugandastores@ug.optica.africa ','raghav@optica.africa','larry.larsen@optica.africa','wairimu@optica.africa']
    receiver_email = ['wairimu@optica.africa']

    email_message = MIMEMultipart("alternative")
    email_message["From"] = email
    email_message["To"] = r','.join(receiver_email)
    email_message["Subject"] = f"Uganda Replacements Created and Received at Branch Report for {yesterday}"
    email_message.attach(MIMEText(html, "html"))

    export_data = replacements.parse('Not Received')
    save_file(
            email_message=email_message,
            reports={"Data": export_data},
            branch_name='Uganda',
            file_name="Replacements not Received.xlsx",
            path=f"{path}replacements")             


    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(email, password)
        server.sendmail(email,receiver_email,email_message.as_string())
        server.quit() 
       


if __name__ == '__main__':
    send_to_branches()