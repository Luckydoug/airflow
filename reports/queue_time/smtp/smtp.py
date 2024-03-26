import smtplib
from airflow.models import variable
import ssl
import os
import random
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from reports.queue_time.html.html import branch_queue_time_html, management_html, queue_time_weekly
from reports.queue_time.utils.utils import transform_multindex
from sub_tasks.libraries.utils import (
    attach_file,
    save_file,
    get_yesterday_date,
    assert_date_modified,
    record_sent_branch,
    return_sent_emails,
    create_initial_file,
    get_four_weeks_date_range,
    get_comparison_months,
    fourth_week_start,
    fourth_week_end
)
from sub_tasks.libraries.styles import (properties, ug_styles)
from reports.queue_time.smtp.emails import kenya, uganda, rwanda, test

load_dotenv()
your_email = os.getenv("douglas_email")
password = os.getenv("douglas_password")
date_ranges = get_four_weeks_date_range()
start_date = date_ranges[3][0].strftime('%Y-%b-%d')
end_date = date_ranges[3][1].strftime('%Y-%b-%d')
todate = get_yesterday_date(truth=True)
first_month, second_month = get_comparison_months()


def send_to_management(path,country, selection) -> None:
    road = f"{path}queue_time/queue_report.xlsx"
    if not assert_date_modified([road]):
        return
    
    queue_report = pd.ExcelFile(road)
    if selection == "Weekly":
        subject = f"{country} {selection} Average Queue from {fourth_week_start} to {fourth_week_end}"
    elif selection == "Monthly":
        subject = f"{country} Average Queue for {first_month} and  {second_month}."
    else:
        return 
    
    if selection == "Weekly":
        country_summary_html = transform_multindex(
           queue_report,
           parse="Country Summary",
           initial="Country"
        )

        branch_summary_html = transform_multindex(
           queue_report,
           parse="Branch Summary",
           initial="Branch"
        )

        html = management_html.format(
           branch_html = branch_summary_html,
           country_html = country_summary_html
       )

    elif selection == "Monthly":
       country_summary_html = transform_multindex(
           queue_report,
           parse="Country Summary",
           initial="Country"
       )

       branch_summary_html = transform_multindex(
           queue_report,
           parse="Branch Summary",
           initial="Branch"
       )

       html = management_html.format(
           branch_html = branch_summary_html,
           country_html = country_summary_html
       )

    else:
        return
    
    if country == "Kenya":
        receiver_email = kenya

    elif country == "Uganda":
        receiver_email = uganda

    elif country == "Rwanda":
        receiver_email = rwanda
    
    elif country == "Test":
        receiver_email = test

    else:
        return
    
    email_message = MIMEMultipart("alternative")
    email_message["From"] = your_email
    email_message["To"] = r','.join(receiver_email)
    email_message["Subject"] = subject
    email_message.attach(MIMEText(html, "html"))

    attach_file(
        email_message=email_message,
        filename=road,
        name=f"{country} Average Queue Time.xlsx"
    )
    

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(your_email, password)
        server.sendmail(
            your_email, 
            receiver_email, 
            email_message.as_string()
        )



def send_branches_queue_time(path, branch_data, log_file, selection, country):
    count = 0
    create_initial_file(log_file)
    road = f"{path}queue_time/queue_report.xlsx"
    report = pd.ExcelFile(road)
    branches_report = report.parse("Branch Summary", index_col=False)
    queues_data = report.parse(f"{selection} Data")
    optoms_report = report.parse("Optom Summary", index_col=False)
    branch_export = pd.DataFrame([])

    if selection == "Daily":
        queue_html = branch_queue_time_html
    elif selection == "Weekly":
        queue_html = queue_time_weekly
    else:
        return

    if not assert_date_modified([road]):
        return
    
    else:
        data = branch_data.set_index("Outlet")
        branch_list = branch_data['Outlet'].tolist()
        random_branch = random.choice(branch_list)

        for branch in branch_list:
            if selection == "Daily":
                if branch in queues_data["Branch"].to_list():
                    branch_name = data.loc[branch, "Branch"]
                    branch_email = data.loc[branch, "Email"]
                    rm_email = data.loc[branch, "RM Email"]

                    optom_report = optoms_report[optoms_report["Branch"] == branch]
                    branch_report = branches_report[branches_report["Branch"] == branch]
                    branch_export = queues_data[queues_data["Branch"] == branch]

                    optom_report_style=  optom_report.style.hide_index().set_properties(**properties).set_table_styles(ug_styles)

                    branch_style_style = branch_report.style.hide_index().set_properties(**properties).set_table_styles(ug_styles)

                    optom_report_html = optom_report_style.to_html(doctype_html = True)
                    branch_html = branch_style_style.to_html(doctype_html = True)

                    test_sample = branch_report.copy()
                    test_sample = test_sample.set_index("Branch")

                    if country == "Kenya" and int(test_sample.loc[branch, "Avg Queue Time"]) < 13:
                        continue
                    

            elif selection == "Weekly":
                
                branches_report = report.parse(
                    "Branch Summary",
                    header=[0, 1],
                    index_col=[0]
                )

                print(branches_report.index.to_list())

                optoms_report = report.parse(
                    "Optom Summary",
                    header=[0,1],
                    index_col=[0, 1]
                )

                if branch in branches_report.index.to_list():
                    branch_name = data.loc[branch, "Branch"]
                    branch_email = data.loc[branch, "Email"]
                    rm_email = data.loc[branch, "RM Email"]

                    branch_report = branches_report[branches_report.index == branch]
                    branch_report = branch_report.rename(columns = {"Branch": ""}, level = 0)
                    branch_report = branch_report.rename(columns = {"": "Branch"}, level = 1)

                    optom_report = optoms_report[optoms_report.index.get_level_values(1) == branch]
                    optom_report = optom_report.rename(columns = {"Optom Name": ""}, level = 0)
                    optom_report = optom_report.rename(columns = {"Unnamed: 2_level_1": "Optom Name"}, level = 1)

                    branch_export = queues_data[queues_data["Branch"] == branch]

                    optom_report_style = optom_report.style.hide_index().set_properties(**properties).set_table_styles(ug_styles)
                    branch_report_style = branch_report.style.hide_index().set_properties(**properties).set_table_styles(ug_styles)

                    optom_report_html = optom_report_style.to_html(doctype_html = True)
                    branch_html = branch_report_style.to_html(doctype_html = True)

                    # LaTeX formula
                else:
                    continue

            if not len(branch_export) and selection == "Daily":
                continue
    
            html = queue_html.format(
                branch_name = branch_name,
                branch_report_html = branch_html,
                optom_report_html = optom_report_html
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

            elif country == "Uganda":
                receiver_email = [
                    rm_email,
                    "wairimu@optica.africa",
                    branch_email
                ]
            
            elif country == "Test":
                receiver_email = ["tstbranch@gmail.com"]
            
            else:
                receiver_email = [rm_email, branch_email] 
    
            if selection == "Daily":
                subject = f"{branch_name} Queue Time Report for {todate}"
            elif selection == "Weekly":
                subject = f"{branch_name} Queue Time  Report from {start_date} to {end_date}."

            elif selection == "Monthly":
                subject = f"{branch_name} Queue Time  Report for {second_month}"
            

            email_message = MIMEMultipart("alternative")
            email_message["From"] = your_email
            email_message["To"] = r','.join(receiver_email)
            email_message["Subject"] = subject
            email_message.attach(MIMEText(html, "html"))


            if len(branch_export):
                save_file(
                    email_message=email_message, 
                    reports = {
                        "Queue": branch_export
                    }, 
                    branch_name= branch_name, 
                    file_name="Queue Time.xlsx", 
                    path=f"{path}queue_time/",
                )

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

                    count += 1

            else:
                continue 

if __name__ == '__main__':
    send_branches_queue_time()
    send_to_management()
        
