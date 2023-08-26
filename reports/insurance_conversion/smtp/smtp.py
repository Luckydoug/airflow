import smtplib
import ssl
import os
import random
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import pandas as pd
from airflow.models import variable
from sub_tasks.libraries.utils import (
    fourth_week_start,
    fourth_week_end,
    clean_folder,
    create_initial_file,
    assert_date_modified,
    save_file,
    return_sent_emails,
    record_sent_branch
)
from sub_tasks.libraries.styles import ug_styles, properties
from reports.insurance_conversion.html.html import (
    management_html,
    branches_html
)
from reports.insurance_conversion.smtp.emails import (
    test,
    kenya,
    uganda
)

email = os.getenv("douglas_email")
password = os.getenv("douglas_password")
receiver_email = []

def highlight(row) -> list:
    value = int(row['Actual Conversion'].replace("%", ""))
    target = int(row['Target Conversion'].replace("%", ""))
    if value >= target:
        colour = "green"
    elif (target == 95 or target == 100) and value >= 90:
        colour = "yellow"
    elif target == 20 and value >= 15:
        colour = "yellow"
    else:
        colour = "red"
    return ['background-color: {}'.format(colour) if row.name == 'Actual Conversion' else ''] * len(row)


def attach_file(email_message, filename, name):
    with open(filename, "rb") as f:
        file_attachment = MIMEApplication(f.read())

    file_attachment.add_header(
        "Content-Disposition",
        f"attachment; filename= {name}"
    )

    email_message.attach(file_attachment)



def send_to_management(selection, country, path) -> None:
    if selection == "Weekly":
        subject = f"{country} {selection} Insurance Conversion Report From {fourth_week_start} to {fourth_week_end}"

    path_appended = f"{path}insurance_conversion/"
    non_conversion = f"{path_appended}mng_noncoverted.xlsx"
    # test = pd.read_excel(f"{path}insurance_conversion/conversion_management.xlsx")
    # print(test)
    management_report = pd.ExcelFile(
        f"{path_appended}conversion_management.xlsx"
    )
    branches_conversion = management_report.parse(
        "all_branches",
        index_col=[0],
        header=[0, 1]
    )

    branches_conversion = branches_conversion.reset_index(level=0)
    branches_conversion = branches_conversion.rename(
        columns={
            "Unnamed: 1_level_0": "",
            "Unnamed: 2_level_0": "",
            "Unnamed: 3_level_0": ""
        },
        level=0
    )

    # branches_conversion = branches_conversion.drop(
    #     columns=[('Unnamed: 0_level_0', 'Unnamed: 0_level_1')],
    #     axis=1
    # )

    branches_conversion = branches_conversion.rename(
        columns={"index": ""},
        level=0
    )

    branches_conversion = branches_conversion.rename(
        columns={"": "Outlet"},
        level=1
    )

    company_conversion = management_report.parse(
        "overall",
        index_col=[0],
        header=[0, 1]
    )

    company_conversion.index = company_conversion.index.get_level_values(0)
    company_conversion = company_conversion.reset_index(level=0)
    company_conversion = company_conversion.rename(columns={"Feedback": ""})
    company_conversion = company_conversion.rename(
        columns={"": "Feedback"},
        level=1
    )

    branches_conversion_html = branches_conversion.to_html(index=False)
    company_conversion = company_conversion.style.hide_index(
    ).set_properties(**properties).set_table_styles(ug_styles)
    company_conversion_html = company_conversion.to_html(doctype_html=True)

    html = management_html.format(
        branches_conversion_html=branches_conversion_html,
        company_conversion_html=company_conversion_html
    )

    if country == "Test":
        receiver_email = test
    elif country == "Kenya":
        receiver_email = kenya
    elif country == "Uganda":
        receiver_email = uganda
    else:
        return

    email_message = MIMEMultipart("alternative")
    email_message["From"] = email
    email_message["To"] = r','.join(receiver_email)
    email_message["Subject"] = subject
    email_message.attach(MIMEText(html, "html"))

    attach_file(email_message, non_conversion, "Non Converted Orders.xlsx")

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(email, password)
        server.sendmail(email, receiver_email, email_message.as_string())


def send_to_branches(
    path: str,
    branch_data: pd.DataFrame,
    filename: str
) -> None:  # This function explicitly returns Nothing, or in other words, it returns None.
    # This function will send the Insurance Conversion Report to the branches
    # To prevent the report being sent to a branch that it has already sent,
    # We utilize the save_email and return_sent_emails function so that it checks
    # if the report for that day has already been sent. If it has already been sent,
    # then the loop jumps to the next iteration. This is necessary because the pipeline
    # has some retries, let say we have 3 retries. In the event where there internet issues
    # and the pipeline fails after sending the first branch, the pipeline will be triggered again
    # and this will cause the email to be sent to a branch that has already been sent. So we use this
    # function here to prevent the occurence of this.
    create_initial_file(filename)

    individual = f"{path}/insurance_conversion/individual.xlsx"
    staff_conversion = pd.ExcelFile(individual)
    non_converted = f"{path}insurance_conversion/noncoverted.xlsx"
    overall = f"{path}insurance_conversion/overall.xlsx"
    branch_conversion = pd.ExcelFile(overall)
    branches_non_conversions = pd.ExcelFile(non_converted)
    files = [individual, non_converted, overall]

    if not assert_date_modified(files):
        return

    branch_list = branch_data["Outlet"].tolist()
    branch_data = branch_data.set_index("Outlet")
    random_branch = random.choice(branch_list)


    for branch in branch_list:
        if branch in branch_conversion.sheet_names:
            branch_name = branch_data.loc[branch, "Branch"]
            branch_email = branch_data.loc[branch, "Email"]
            rm_email = branch_data.loc[branch, "RM Email"]

            overall_feedbacks = branch_conversion.parse(
                branch, 
                index_col=False
            )
            individual_feedback = staff_conversion.parse(
                branch, 
                index_col=[0, 1], 
                header=[0, 1]
            )
            individual_feedback.index = individual_feedback.index.get_level_values(1)
            individual_feedback = individual_feedback.reset_index(level=0)
            individual_feedback = individual_feedback.rename(columns={"Order Creator": ""})
            individual_feedback = individual_feedback.rename(columns={"": "Order Creator"}, level=1)

            print(overall_feedbacks.columns)
            
            overall_feedbacks = overall_feedbacks.style.hide_index().set_properties(**properties).set_table_styles(ug_styles)

            individual_feedback = individual_feedback.style.hide_index(
            ).set_properties(**properties).set_table_styles(ug_styles)

            overall_feedbacks_html = overall_feedbacks.to_html(
                doctype_html=True
            )
            individual_feedback_html = individual_feedback.to_html(
                doctype_html=True
            )

            html = branches_html.format(
                branch_name=branch_name,
                overall_feedbacks_html=overall_feedbacks_html,
                individual_feedback_html=individual_feedback_html
            )

            if branch == random_branch:
                receiver_email = [
                    rm_email,
                    "wairimu@optica.africa",
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
            
            elif branch == "YOR":
                receiver_email = [
                    rm_email,
                    "yh.manager@optica.africa",
                    "insurance@optica.africa",
                    branch_email
                ]
            
            else:
                receiver_email = [
                  rm_email,
                  branch_email
                ]

            email_message = MIMEMultipart("alternative")
            email_message["From"] = email
            email_message["To"] = r','.join(receiver_email)
            email_message["Subject"] = f"{branch_name} Insurance Conversion Report from {fourth_week_start} to {fourth_week_end}."
            email_message.attach(MIMEText(html, "html"))

            if branch in branches_non_conversions.sheet_names:
                branch_export = branches_non_conversions.parse(branch)
                save_file(
                    email_message=email_message,
                    reports={"Data": branch_export},
                    branch_name=branch_name,
                    file_name="Non Converted Feedbacks.xlsx",
                    path=f"{path}insurance_conversion"
                )


            if branch_email not in return_sent_emails(filename):
                context = ssl.create_default_context()
                with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                    server.login(email, password)
                    server.sendmail(
                        email,
                        receiver_email,
                        email_message.as_string()
                    )
                    record_sent_branch(
                        branch_email,
                        filename
                    )
            else:
                continue

        else:
            continue


def mop_folder(path):
    clean_folder(f"{path}insurance_conversion/")


if __name__ == '__main__':
    send_to_management()
    send_to_branches()
