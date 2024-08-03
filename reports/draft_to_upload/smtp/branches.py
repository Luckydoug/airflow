import smtplib
from airflow.models import variable
import ssl
import os
import random
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from reports.draft_to_upload.html.html import (branch_efficiency_html)
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
todate = get_yesterday_date(truth=True)
first_month, second_month = get_comparison_months()


def send_branches_efficiency(path, target, branch_data, log_file, selection, country):
    if selection == "Daily":
        return
    create_initial_file(log_file)
    sales_persons = f"{path}draft_upload/draft_to_upload_sales_efficiency.xlsx"
    branches = f"{path}draft_upload/draft_to_upload_branch_efficiency.xlsx"
    branches_data = f"{path}/draft_upload/efficiency_raw_data.xlsx"
    branches_mtd = f"{path}draft_upload/draft_to_upload.xlsx"
    eyetests_complete = f"{path}draft_upload/et_to_order.xlsx"

    sales_persons_report = pd.ExcelFile(sales_persons)
    branches_report = pd.ExcelFile(branches)
    export_data = pd.ExcelFile(branches_data)
    mtd_report = pd.ExcelFile(branches_mtd)

    if os.path.exists(eyetests_complete):
        eyetest_order = pd.ExcelFile(eyetests_complete)
        et_order_data = eyetest_order.parse("Data", index_col=False)
    


    if not assert_date_modified([sales_persons, branches, branches_data]):
        return
    
    else:
        data = branch_data.set_index("Outlet")
        branch_list = branch_data['Outlet'].tolist()
        random_list = export_data.sheet_names
        random_branch = random.choice(random_list)

        for branch in branch_list:
            et_order_branch = []
            if branch in export_data.sheet_names:
                branch_name = data.loc[branch, "Branch"]
                branch_email = data.loc[branch, "Email"]
                rm_email = data.loc[branch, "RM Email"]
                sales_report = pd.DataFrame(sales_persons_report.parse(branch, index_col=False))
                branch_report = pd.DataFrame(branches_report.parse(branch, index_col=False))
                all_mtd = mtd_report.parse("mtd-update", index_col=False)
                branch_mtd = all_mtd[all_mtd["Outlet"] == branch]

                sales_style = sales_report.style.hide(axis='index').applymap(
                    highlight_efficiency, subset=[f"% Efficiency (Target: {target} mins)"]
                ).set_properties(**properties).set_table_styles(ug_styles)

                branch_style = branch_report.style.hide(axis='index').applymap(
                    highlight_efficiency, subset=[f"% Efficiency (Target: {target} mins)"]
                ).set_properties(**properties).set_table_styles(ug_styles)

                sales_report_html = sales_style.to_html(doctype_html = True)
                branch_html = branch_style.to_html(doctype_html = True)
                branch_export = export_data.parse(branch, index_col=False)
                branch_export = branch_export[branch_export["Draft to Upload"] > 8]
                
                if os.path.exists(eyetests_complete) and branch in et_order_data["order_branch"].to_list():
                    et_order_branch = et_order_data[et_order_data["order_branch"] == branch]
                    et_order_branch = et_order_branch[[
                        "visit_id",
                        "order_number",
                        "creator",
                        "order_creator",
                        "customer_code",
                        "et_completed_time",
                        "order_date",
                        "order_type",
                        "criteria",
                        "time_taken"
                    ]]
                    et_order_style = et_order_branch.style.hide_index().set_properties(**properties).set_table_styles(ug_styles)
                    et_order_html = et_order_style.to_html(doctype_html = True)
                else:
                    order_et_html = ""


                if not len(branch_export):
                    color = "green"
                    message = "You have no late orders for the above period. Thanks for maintaining 100% efficiency."
                else:
                    color = "red"
                    message = """Please find the attached late orders data. <br>
                    On the attachment, there are two sheets named as follows; <br> <br>
                    1) MTD - Update - This sheet will show the %Efficiency for your branch organized in days <br>
                    2) Late Orders - This sheet contains all orders that took more than 8 minutes from Draft Order Created to Upload Attachment.
                    """

                if not len(branch_export) and selection == "Daily":
                    continue
                # LaTeX formula

                if len(et_order_branch):
                    order_et_html = et_order_html
                    msg = """
                     <b> 3) Time from Eye Test Completion to Draft Order Created </b>
                        <p>Please help us understand why the below client(s) <br>
                        had to wait more than one hour after an eye test for you to draft the order.</p>
                    """
                else:
                    order_et_html = ""
                    msg = ""

                html = branch_efficiency_html.format(
                    branch_name = branch_name,
                    branch_report_html = branch_html,
                    sales_person_report_html = sales_report_html,
                    color = color,
                    message = message,
                    et_order_html = order_et_html,
                    msg = msg
                )

                if branch == "KAM" or branch == "OAS":
                    continue

                if branch == random_branch:
                    receiver_email = [
                        "wazeem@optica.africa",
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
                        "kush@optica.africa",
                        rm_email,
                        "wairimu@optica.africa",
                        branch_email
                    ]
                
                else:
                    receiver_email = [rm_email, branch_email]

                if selection == "Daily":
                    subject = f"{branch_name} Draft to Upload Efficiency Report for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch_name} Draft to Upload Efficiency Report from {start_date} to {end_date}."

                elif selection == "Monthly":
                    subject = f"{branch_name} Draft to Upload Efficiency Report for {second_month}"
                
                email_message = MIMEMultipart("alternative")
                email_message["From"] = your_email
                email_message["To"] = r','.join(receiver_email)
                email_message["Subject"] = subject
                email_message.attach(MIMEText(html, "html"))


                if len(branch_export):
                    save_file(
                        email_message=email_message, 
                        reports = {
                            "MTD-Update": branch_mtd, 
                            "Late Orders": branch_export
                        }, 
                        branch_name= branch_name, 
                        file_name="Insurance Efficiency.xlsx", 
                        path=f"{path}draft_upload/",
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

                else:
                    continue #Jump to the next branch if the branch email has already been sent

            else:
                continue

if __name__ == '__main__':
    send_branches_efficiency()
        
