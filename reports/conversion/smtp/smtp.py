import os
import ssl
import smtplib
import pandas as pd
from dotenv import load_dotenv
from airflow.models import variable
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from sub_tasks.libraries.styles import (ug_styles, properties)
from reports.conversion.html.html import (conversion_html, branches_html)
from reports.conversion.smtp.emails import (uganda, kenya, test, rwanda)
from sub_tasks.libraries.utils import (
    save_file,
    return_sent_emails,
    record_sent_branch,
    create_initial_file,
    get_todate,
    get_todate,
    clean_folder,
    fourth_week_end,
    style_dataframe,
    fourth_week_start,
    apply_multiindex_format,
    get_comparison_months
)

load_dotenv()

# Basic Configuration
your_email = os.getenv("douglas_email")
password = os.getenv("douglas_password")
to_date = get_todate()
receiver_email = []



def attach_file(email_message, filename, name):
    with open(filename, "rb") as f:
        file_attachment = MIMEApplication(f.read())

    file_attachment.add_header(
        "Content-Disposition",
        f"attachment; filename= {name}"
    )

    email_message.attach(file_attachment)

def send_management_report(path, country, selection):
    weekly_monthly = ""
    if selection == "Weekly":
        weekly_monthly = "Weekly"
        subject = f"{country} Registrations, Eyes Tests, and View RX Conversion Report from {fourth_week_start} to {fourth_week_end}"
        # Registrations
        registrations_path = f"{path}conversion/registrations/overall.xlsx"
        reg_non_conversions = f"{path}conversion/registrations/non_conversions.xlsx"

        overall_reg_report = pd.ExcelFile(registrations_path)
        reg_summary_conv = overall_reg_report.parse(
            "summary", index_col=[0], header=[0, 1]
        )
        branches_reg_summary_conv = overall_reg_report.parse(
            "per_branch", index_col=[0], header=[0, 1]
        )

        reg_summary_html = apply_multiindex_format(
            dataframe=reg_summary_conv,
            old="Country",
            new="Country",
            styles=ug_styles,
            properties=properties
        )

        branches_reg_summary_html = apply_multiindex_format(
            dataframe=branches_reg_summary_conv,
            old="Outlet",
            new="Outlet",
            styles=ug_styles,
            properties=properties
        )

        # Eye Tests
        eyetests_path = f"{path}conversion/eyetests/overall.xlsx"
        ets_non_conversions = f"{path}conversion/eyetests/overall.xlsx"

        overall_et_report = pd.ExcelFile(eyetests_path)
        overall_et_conv = overall_et_report.parse(
            "Summary_Conversion", 
            index_col=[0], 
            header=[0, 1]
        )
        branches_et_summary = overall_et_report.parse(
            "Branches_Conversion", 
            index_col=[0], 
            header=[0, 1]
        )
        summary_higrx_conv = overall_et_report.parse(
            "Highrx_Conversion", 
            index_col=[0], 
            header=[0, 1]
        )
        branch_high_rx = overall_et_report.parse(
            "high_rx_branch",
            index_col = [0],
            header = [0, 1]
        )

        overall_et_html = apply_multiindex_format(
            dataframe=overall_et_conv,
            new="Country",
            old="Country",
            styles=ug_styles,
            properties=properties
        )

        branches_et_html = apply_multiindex_format(
            dataframe=branches_et_summary,
            old="branch_code",
            new="Outlet",
            styles=ug_styles,
            properties=properties
        )

        summary_higrx_html = apply_multiindex_format(
            dataframe=summary_higrx_conv,
            old="Country",
            new="Country",
            styles=ug_styles,
            properties=properties
        )

        branches_highrx_html = apply_multiindex_format(
            dataframe=branch_high_rx,
            old="branch_code",
            new="Outlet",
            styles=ug_styles,
            properties=properties
        )

        # View RX

        view_rx_path = f"{path}conversion/viewrx/overall.xlsx"
        views_non_conversions = f"{path}conversion/viewrx/non_conversions.xlsx"

        overall_views_report = pd.ExcelFile(view_rx_path)
        overall_views_conv = overall_views_report.parse(
            "Summary_Conversion", 
            index_col=[0], 
            header=[0, 1]
        )
        branches_views_summary = overall_views_report.parse(
            "Branches_Conversion", 
            index_col=[0], 
            header=[0, 1]
        )

        overall_views_html = apply_multiindex_format(
            dataframe=overall_views_conv,
            new="Country",
            old="Country",
            styles=ug_styles,
            properties=properties
        )

        branches_views_html = apply_multiindex_format(
            dataframe=branches_views_summary,
            old="Branch",
            new="Outlet",
            styles=ug_styles,
            properties=properties
        )

    if selection == "Monthly":
        weekly_monthly = "Monthly"
        first_month, second_month = get_comparison_months()
        subject = f"{country} Updated Monthly Registrations, Eyes Tests, and View RX Conversion Report for {first_month} and {second_month}"
        # Registrations
        registrations_path = f"{path}conversion/registrations/overall.xlsx"

        overall_reg_report = pd.ExcelFile(registrations_path)
        reg_summary_conv = overall_reg_report.parse(
            "monthly_summary", index_col=[0], header=[0, 1]
        )
        branches_reg_summary_conv = overall_reg_report.parse(
            "per_branch", index_col=[0], header=[0, 1]
        )

        reg_summary_html = apply_multiindex_format(
            dataframe=reg_summary_conv,
            old="Country",
            new="Country",
            styles=ug_styles,
            properties=properties
        )

        branches_reg_summary_html = apply_multiindex_format(
            dataframe=branches_reg_summary_conv,
            old="Outlet",
            new="Outlet",
            styles=ug_styles,
            properties=properties
        )

        # Eye Tests
        eyetests_path = f"{path}conversion/eyetests/overall.xlsx"

        overall_et_report = pd.ExcelFile(eyetests_path)
        overall_et_conv = overall_et_report.parse(
            "Monthly_Conversion", 
            index_col=[0], 
            header=[0, 1]
        )
        branches_et_summary = overall_et_report.parse(
            "Branches_Conversion", 
            index_col=[0], 
            header=[0, 1]
        )
        summary_higrx_conv = overall_et_report.parse(
            "Highrx_Conversion", 
            index_col=[0], 
            header=[0, 1]
        )

        branches_highrx_conv = overall_et_report.parse(
            "branch_highrx",
            index_col=[0],
            header=[0, 1]
        )

        branches_highrx_html = apply_multiindex_format(
            dataframe=branches_highrx_conv,
            old="branch_code",
            new="Outlet",
            styles=ug_styles,
            properties=properties
        )

        overall_et_html = apply_multiindex_format(
            dataframe=overall_et_conv,
            new="Country",
            old="Country",
            styles=ug_styles,
            properties=properties
        )

        branches_et_html = apply_multiindex_format(
            dataframe=branches_et_summary,
            old="branch_code",
            new="Outlet",
            styles=ug_styles,
            properties=properties
        )

        summary_higrx_html = apply_multiindex_format(
            dataframe=summary_higrx_conv,
            old="Country",
            new="Country",
            styles=ug_styles,
            properties=properties
        )

        # View RX

        view_rx_path = f"{path}conversion/viewrx/overall.xlsx"

        overall_views_report = pd.ExcelFile(view_rx_path)
        overall_views_conv = overall_views_report.parse(
            "Monthly_Conversion", 
            index_col=[0], 
            header=[0, 1]
        )
        branches_views_summary = overall_views_report.parse(
            "Branches_Conversion", 
            index_col=[0], 
            header=[0, 1]
        )

        overall_views_html = apply_multiindex_format(
            dataframe=overall_views_conv,
            new="Country",
            old="Country",
            styles=ug_styles,
            properties=properties
        )

        branches_views_html = apply_multiindex_format(
            dataframe=branches_views_summary,
            old="Branch",
            new="Outlet",
            styles=ug_styles,
            properties=properties
        )

    html = conversion_html.format(
        overall_et_html=overall_et_html,
        branches_et_html=branches_et_html,
        reg_summary_html=reg_summary_html,
        overall_views_html=overall_views_html,
        summary_higrx_html=summary_higrx_html,
        branches_views_html=branches_views_html,
        branches_reg_summary_html=branches_reg_summary_html,
        weekly_monthly = weekly_monthly,
        branches_highrx_html = branches_highrx_html
    )


    if country == "Test":
        receiver_email = test
    elif country == "Kenya":
        receiver_email = kenya
    elif country == "Uganda":
        receiver_email = uganda
    elif country == "Rwanda":
        receiver_email = rwanda
    else:
        return


    email_message = MIMEMultipart("alternative")
    email_message["From"] = your_email
    email_message["To"] = r','.join(receiver_email)
    email_message["Subject"] = subject
    email_message.attach(MIMEText(html, "html"))

    attach_file(email_message, registrations_path, "Registrations Report.xlsx")
    attach_file(email_message, eyetests_path, "Eye Tests Report.xlsx")
    attach_file(email_message, view_rx_path, "ViewRX Report.xlsx")

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(your_email, password)
        server.sendmail(your_email, receiver_email, email_message.as_string())


def send_branches_report(path, branch_data, selection):
    create_initial_file(
        filename=f"{path}conversion/log.txt"
    )

    if selection == "Monthly":
        return
    else:
        branch_list = branch_data["Outlet"].to_list()
        ug_srm_rm_index = branch_data.set_index("Outlet")


        # Registrations
        reg_branch_path = f"{path}conversion/registrations/branch.xlsx"
        reg_salesperson_path = f"{path}conversion/registrations/sales_persons.xlsx"
        reg_branch = pd.ExcelFile(reg_branch_path)
        reg_salespersons = pd.ExcelFile(reg_salesperson_path)
        

        #Eye Tests
        eyetest_branch_path = f"{path}conversion/eyetests/branches.xlsx"
        eyetest_optom_path = f"{path}conversion/eyetests/opthoms.xlsx"
        eyetest_salesperson_path = f"{path}conversion/eyetests/sales_persons.xlsx"
        eyetest_branch = pd.ExcelFile(eyetest_branch_path)
        eyetest_salespersons = pd.ExcelFile(eyetest_salesperson_path)
        eyetest_optom = pd.ExcelFile(eyetest_optom_path)

        #High RX
        highrx_branch_path = f"{path}conversion/eyetests/highrx_branches.xlsx"
        highrx_optom_path = f"{path}conversion/eyetests/highrx_opthoms.xlsx"
        highrx_saleper_path = f"{path}conversion/eyetests/highrx_sales_persons.xlsx"
        highrx_branch = pd.ExcelFile(highrx_branch_path)
        highrx_optom = pd.ExcelFile(highrx_optom_path)
        highrx_salesper = pd.ExcelFile(highrx_saleper_path)

        #Views
        views_branch_path = f"{path}conversion/viewrx/branches.xlsx"
        views_salespersons_path = f"{path}conversion/viewrx/sales_persons.xlsx"
        views_branch = pd.ExcelFile(views_branch_path)
        views_salespersons = pd.ExcelFile(views_salespersons_path)

        for branch in branch_list:
            if branch in reg_branch.sheet_names and branch in eyetest_branch.sheet_names and branch in views_branch.sheet_names:
                branch_name = ug_srm_rm_index.loc[branch, "Branch"]
                branch_email = ug_srm_rm_index.loc[branch, "Email"]
                branch_rm = ug_srm_rm_index.loc[branch, "RM Email"]

                """Registrations Parsing, Style and HTML"""
                branch_reg_report = reg_branch.parse(str(branch), index_col=False).iloc[:, 1:]
                salesperson_reg_report = reg_salespersons.parse(str(branch), index_col=False).iloc[:, 1:]

                # reg_copy = branch_reg_report.copy()
                # reg_copy = reg_copy.set_index("Outlet")

                branch_reg_html = style_dataframe(branch_reg_report, ug_styles, properties)
                salespersons_reg_html = style_dataframe(salesperson_reg_report, ug_styles, properties)
                
                # if reg_copy.loc[branch, "Conversion"].split("%").str[0].astype(int) == 100:
                #     branch_reg_html = "<p style = color: green; >Registration Conversion - 100 %</p>"
                #     salespersons_reg_html = "<p></p>"

                branch_eyetest_report = eyetest_branch.parse(str(branch), index_col=False).iloc[:, 1:]
                salespersons_eyetests_report = eyetest_salespersons.parse(str(branch), index_col=False).iloc[:, 1:]
                optom_eyetests_report = eyetest_optom.parse(str(branch), index_col=False).iloc[:, 1:]
                branch_eyetest_html = style_dataframe(branch_eyetest_report, ug_styles, properties)
                salesperson_eyetest_html = style_dataframe(salespersons_eyetests_report, ug_styles, properties)
                optom_eyetest_html = style_dataframe(optom_eyetests_report, ug_styles, properties)

                highrx_branch_report = highrx_branch.parse(str(branch), index_col=False).iloc[:, 1:]
                highrx_optom_report = highrx_optom.parse(str(branch), index_col=False).iloc[:, 1:]
                highrx_salesper_report = highrx_salesper.parse(str(branch), index_col=False).sort_values(by="%Conversion", ascending=False)


                highrx_branch_html = style_dataframe(highrx_branch_report, ug_styles, properties)
                highrx_optom_html = style_dataframe(highrx_optom_report, ug_styles, properties)
                highrx_salesper_html = style_dataframe(highrx_salesper_report, ug_styles, properties)


                views_branch_report = views_branch.parse(str(branch), index_col=False).iloc[:, 1:]
                salespersons_views_report = views_salespersons.parse(str(branch), index_col=False).iloc[:, 1:]
                views_branch_html = style_dataframe(views_branch_report, ug_styles, properties)
                salespersons_views_html = style_dataframe(salespersons_views_report, ug_styles, properties)

                #Non Conversions
                eyetests_non_conversions = pd.ExcelFile(f"{path}conversion/eyetests/non_conversions.xlsx")
                registrations_non_conversions = pd.ExcelFile(f"{path}conversion/registrations/non_conversions.xlsx")
                views_non_conversions = pd.ExcelFile(f"{path}conversion/viewrx/non_conversions.xlsx")

                html = branches_html.format(
                    branch_name = branch_name,
                    branch_reg_html = branch_reg_html,
                    salespersons_reg_html =  salespersons_reg_html,
                    branch_eyetest_html = branch_eyetest_html,
                    salesperson_eyetest_html = salesperson_eyetest_html,
                    optom_eyetest_html = optom_eyetest_html,
                    views_branch_html = views_branch_html,
                    salespersons_views_html = salespersons_views_html,
                    branch_highrx_html = highrx_branch_html,
                    optom_highrx_html = highrx_optom_html,
                    salesperson_highrx_html = highrx_salesper_html
                )


                if branch == "OHO":
                    receiver_email = [
                        branch_rm,
                        "duncan.muchai@optica.africa",
                        "susan@optica.africa",
                        branch_email
                    ]
                elif branch == "YOR":
                    receiver_email = [
                        branch_rm,
                        "yh.manager@optica.africa",
                        branch_email
                    ]
                
                elif branch in ["ACA", "KAM", "ARE", "OAS"]:
                    receiver_email = [
                        "raghav@optica.africa",
                        branch_rm,
                        branch_email,
                    ]

                else:
                    receiver_email = [
                        branch_rm,
                        branch_email,
                    ]


                email_message = MIMEMultipart("alternative")
                email_message["From"] = your_email
                email_message["To"] = r','.join(receiver_email)
                email_message["Subject"] = f"{branch_name} Updated Registrations, Eye Tests, and View RX Conversion Report from {fourth_week_start} to {fourth_week_end}"
                email_message.attach(MIMEText(html, "html"))

                if branch in eyetests_non_conversions.sheet_names:
                    branch_export = eyetests_non_conversions.parse(branch, index_col=False)
                    save_file(
                        email_message=email_message, 
                        reports = {
                            "Non Converted": branch_export, 
                        }, 
                        branch_name= branch_name, 
                        file_name="EyeTests Non Conversions.xlsx", 
                        path = f"{path}conversion/eyetests/",
                    )
                if branch in registrations_non_conversions.sheet_names:
                    branch_export = registrations_non_conversions.parse(branch, index_col=False)
                    save_file(
                        email_message=email_message, 
                        reports = {
                            "Non Converted": branch_export, 
                        }, 
                        branch_name= branch_name, 
                        file_name="Registrations Non Conversions.xlsx", 
                        path = f"{path}conversion/registrations/",
                    )
                
                if branch in views_non_conversions.sheet_names:
                    branch_export = views_non_conversions.parse(branch, index_col=False)
                    save_file(
                        email_message=email_message, 
                        reports = {
                            "Non Converted": branch_export, 
                        }, 
                        branch_name= branch_name, 
                        file_name="ViewRX Non Conversions.xlsx", 
                        path = f"{path}conversion/viewrx/",
                    )

                if branch_email not in return_sent_emails(f"{path}conversion/log.txt"):
                    context = ssl.create_default_context()
                    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                        server.login(your_email, password)
                        server.sendmail(your_email, receiver_email, email_message.as_string())

                        record_sent_branch(
                            branch_email=branch_email,
                            filename=f"{path}conversion/log.txt"
                        )
                else:
                    continue




def clean_registrations(path):
    clean_folder(dir_name=f"{path}conversion/registrations/")


def clean_eyetests(path):
    clean_folder(dir_name=f"{path}conversion/eyetests/")


def clean_views(path):
    clean_folder(dir_name=f"{path}conversion/viewrx/")


if __name__ == '__main__':
    send_management_report()
    send_branches_report()
    clean_registrations()
    clean_eyetests()
    clean_views()

