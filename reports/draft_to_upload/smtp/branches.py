import smtplib
import pygsheets
from airflow.models import variable
import ssl
import os
import random
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import pandas as pd
from reports.draft_to_upload.html.html import (branch_efficiency_html)

from sub_tasks.libraries.utils import (
    assert_date_modified,
    record_sent_branch,
    return_sent_emails,
    create_initial_file,
    get_four_weeks_date_range,
)

from reports.draft_to_upload.utils.utils import (
    highlight_efficiency
)

from sub_tasks.libraries.styles import (properties, ug_styles)

load_dotenv()
your_email = os.getenv("douglas_email")
password = os.getenv("douglas_password")
date_ranges = get_four_weeks_date_range()
start_date = date_ranges[3][0].strftime('%Y-%b-%d')
end_date = date_ranges[3][1].strftime('%Y-%b-%d')


def send_branches_efficiency(path, target, branch_data, log_file):
    create_initial_file(log_file)
    sales_persons = f"{path}draft_upload/draft_to_upload_sales_efficiency.xlsx"
    branches = f"{path}draft_upload/draft_to_upload_branch_efficiency.xlsx"
    branches_data = f"{path}/draft_upload/efficiency_raw_data.xlsx"

    sales_persons_report = pd.ExcelFile(sales_persons)
    branches_report = pd.ExcelFile(branches)
    export_data = pd.ExcelFile(branches_data)

    if not assert_date_modified([sales_persons, branches, branches_data]):
        return
    
    else:
        data = branch_data.set_index("Outlet")
        branch_list = branch_data['Outlet'].tolist()
        random_branch = random.choice(branch_list)

        for branch in branch_list:
            if branch in export_data.sheet_names:
                branch_name = data.loc[branch, "Branch"]
                branch_email = data.loc[branch, "Email"]
                rm_email = data.loc[branch, "RM Email"]

                sales_report = pd.DataFrame(sales_persons_report.parse(branch, index_col=False))
                branch_report = pd.DataFrame(branches_report.parse(branch, index_col=False))

                sales_style = sales_report.style.hide_index().applymap(
                    highlight_efficiency, subset=[f"% Efficiency (Target: {target} mins)"]
                ).set_properties(**properties).set_table_styles(ug_styles)

                branch_style = branch_report.style.hide_index().applymap(
                    highlight_efficiency, subset=[f"% Efficiency (Target: {target} mins)"]
                ).set_properties(**properties).set_table_styles(ug_styles)

                sales_report_html = sales_style.to_html(doctype_html = True)
                branch_html = branch_style.to_html(doctype_html = True)

                html = branch_efficiency_html.format(
                    branch_name = branch_name,
                    branch_report_html = branch_html,
                    sales_person_report_html = sales_report_html
                )

                if branch == random_branch:
                    receiver_email = [
                        rm_email, 
                        "wairimu@optica.africa", 
                        branch_email
                    ]
                
                elif branch == "YOR":
                    receiver_email = [
                        rm_email,
                        "yh.manager@optica.africa",
                        branch_email
                    ]
                
                elif branch == "OHO":
                    receiver_email = [
                        rm_email,
                        "duncan.muchai@optica.africa",
                        "susan@optica.africa",
                        branch_email
                    ]
                
                else:
                    receiver_email = [rm_email, branch_email]
                

                email_message = MIMEMultipart("alternative")
                email_message["From"] = your_email
                email_message["To"] = r','.join(receiver_email)
                email_message["Subject"] = f"{branch_name} Draft to Upload Efficiency Report from {start_date} to {end_date}."
                email_message.attach(MIMEText(html, "html"))

                def attach_file(email_message, filename):
                    with open(filename, "rb") as f:
                        file_attachment = MIMEApplication(f.read())

                        file_attachment.add_header(
                            "Content-Disposition",
                            f"attachment; filename= {filename}"
                        )

                        email_message.attach(file_attachment)
                    
                raw_data = export_data.parse(branch, index_col=False)
                filename = f"{path}draft_upload/{branch_name} Draft to Upload Data.xlsx"
                writer = pd.ExcelWriter(filename, engine='xlsxwriter')
                raw_data.to_excel(writer, sheet_name='Data', index=False)
                writer.save()
                writer.close()
                attach_file(email_message, filename)

                if branch_email not in return_sent_emails(log_file):
                    context = ssl.create_default_context()
                    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                        server.login(your_email, password)
                        server.sendmail(
                            your_email, 
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
    send_branches_efficiency()
        
