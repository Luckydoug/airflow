import smtplib
from airflow.models import variable
import ssl
import os
import random
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from reports.draft_to_upload.html.html import (pending_insurance)
from sub_tasks.libraries.utils import (
    save_file,
    get_yesterday_date,
    assert_date_modified,
    record_sent_branch,
    return_sent_emails,
    create_initial_file,
    get_four_weeks_date_range,
    get_comparison_months
)

from sub_tasks.libraries.styles import (properties, ug_styles)
from sub_tasks.libraries.utils import return_evn_credentials
load_dotenv()
sender_email = os.getenv("douglas_email")
password = os.getenv("douglas_password")
date_ranges = get_four_weeks_date_range()
start_date = date_ranges[3][0].strftime('%Y-%b-%d')
end_date = date_ranges[3][1].strftime('%Y-%b-%d')
todate = get_yesterday_date(truth=True)
first_month, second_month = get_comparison_months()


def send_pending_insurance(path, branch_data, log_file):
    create_initial_file(log_file)
    branches_data = f"{path}draft_upload/pending_insurance.xlsx"
    export_data = pd.ExcelFile(branches_data)
    export_data = export_data.parse("Data", index_col=False)

    stephen_data = branch_data[branch_data["Retail Analyst"] == "Stephen"]
    stephen_branches = stephen_data["Outlet"].to_list()

    felix_data = branch_data[branch_data["Retail Analyst"] == "Felix"]
    felix_branches = felix_data["Outlet"].to_list()

    getrude_data = branch_data[branch_data["Retail Analyst"] == "Getrude"]
    getrude_branches = getrude_data["Outlet"].to_list()

    if not assert_date_modified([branches_data]):
        return
    
    else:
        data = branch_data.set_index("Outlet")
        branch_list = branch_data['Outlet'].tolist()
        random_list = export_data["Branch"].to_list()
        random_branch = random.choice(random_list)

        for branch in branch_list:
            return
            if branch in export_data["Branch"].to_list():
                branch_name = data.loc[branch, "Branch"]
                branch_email = data.loc[branch, "Email"]
                rm_email = data.loc[branch, "RM Email"]
                
                branch_report = export_data[export_data["Branch"] == branch]
                branch_style = branch_report.style.hide_index().set_properties(**properties).set_table_styles(ug_styles)

                branch_html = branch_style.to_html(doctype_html = True)             
            

                html = pending_insurance.format(
                    branch_name = branch_name,
                    branch_report_html = branch_html
                )

                if branch == random_branch:
                    receiver_email = [
                        "wazeem@optica.africa",
                        rm_email, 
                        "wairimu@optica.africa", 
                        "douglas.kathurima@optica.africa",
                        branch_email
                    ]
                
                elif branch == "YOR":
                    receiver_email = [
                        rm_email,
                        "yh.manager@optica.africa",
                        "insurance@optica.africa",
                        branch_email
                    ]
                
                elif branch == "OHO":
                    receiver_email = [
                        rm_email,
                        "duncan.muchai@optica.africa",
                        "susan@optica.africa",
                        "insuranceoh@optica.africa",
                        branch_email
                    ]

                
                else:
                    receiver_email = [rm_email, branch_email]

                if branch in getrude_branches:
                    sender_email, password = return_evn_credentials("getrude")
                elif branch in stephen_branches:
                    sender_email, password = return_evn_credentials("stephen")
                elif branch in felix_branches:
                    sender_email, password = return_evn_credentials("felix")
                else:
                    return
            
                
                subject = f"{branch_name} Non-Submitted Insurance Customers Eye tests, {todate}"
                email_message = MIMEMultipart("alternative")
                email_message["From"] = sender_email
                email_message["To"] = r','.join(receiver_email)
                email_message["Subject"] = subject
                email_message.attach(MIMEText(html, "html"))


                if branch_email not in return_sent_emails(log_file):
                    context = ssl.create_default_context()
                    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                        server.login(sender_email, password)
                        server.sendmail(
                            sender_email, 
                            receiver_email, 
                            email_message.as_string()
                        )
                        record_sent_branch(
                            branch_email, 
                            log_file
                        )

                else:
                    continue #Jump to the next branch if the branch email has already been sent

            else:
                continue

if __name__ == '__main__':
    send_pending_insurance()
        
