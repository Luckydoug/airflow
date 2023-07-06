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
from sub_tasks.libraries.utils import (
    save_file,
    fetch_gsheet_data,
    get_todate,
    uganda_path,
    get_todate,
    clean_folder,
    fourth_week_end,
    style_dataframe,
    fourth_week_start,
    apply_multiindex_format,
)

load_dotenv()

# Basic Configuration
your_email = os.getenv("douglas_email")
password = os.getenv("douglas_password")
to_date = get_todate()
receiver_email = []
subject = f"Uganda Registrations, Eyes Tests, and View RX Conversion Report from {fourth_week_start} to {fourth_week_end}"


def attach_file(email_message, filename, name):
    with open(filename, "rb") as f:
        file_attachment = MIMEApplication(f.read())

    file_attachment.add_header(
        "Content-Disposition",
        f"attachment; filename= {name}"
    )

    email_message.attach(file_attachment)

def send_management_report():
    # Registrations
    registrations_path = f"{uganda_path}conversion/registrations/overall.xlsx"
    reg_non_conversions = f"{uganda_path}conversion/registrations/non_conversions.xlsx"

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
    eyetests_path = f"{uganda_path}conversion/eyetests/overall.xlsx"
    ets_non_conversions = f"{uganda_path}conversion/eyetests/non_conversions.xlsx"

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

    view_rx_path = f"{uganda_path}conversion/viewrx/overall.xlsx"
    views_non_conversions = f"{uganda_path}conversion/viewrx/non_conversions.xlsx"

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

    html = """
    <!DOCTYPE html>
    <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta http-equiv="X-UA-Compatible" content="IE=edge">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Conversion Report</title>

            <style>
                table {{border-collapse: collapse;font-family:Trebuchet MS; font-size:9}}
                th {{text-align: left;font-family:Trebuchet MS; font-size:9; padding: 3px;}}
                body, p, h3, div, span, var {{font-family:Trebuchet MS; font-size:13}}
                td {{text-align: left;font-family:Trebuchet MS; font-size:9; padding: 8px;}}
                h4 {{font-size: 12px; font-family: Verdana, sans-serif;}}

                .content {{
                    margin-top: 20px !important;
                    border: none;
                    background-color: white;
                    padding: 4%;
                    width: 85%;
                    margin: 0 auto;
                    border-radius: 3px;
                }}

                .salutation {{
                    width: 20%;
                    margin: 0 auto;
                    margin-top: 20px;
                    font-size: 10px;
                }}

                .division-line {{
                    display: inline-block;
                    border-top: 1px solid black;
                    margin: 0 5px;
                    padding: 0 2px;
                    vertical-align: middle;
                }}

            </style>

        </head>
        <body style = "background-color: #F0F0F0; padding: 2% 2% 2% 2%;">
            <div class="content">
                <div>
                    <ol>
                        <li>
                            <h3>Registrations Conversion</h3>
                            <br><br>
                            <ol>
                                <li><b>Weekly Overall Comparison</b>
                                <br> <br>
                                    <table>{reg_summary_html}</table>
                                </li>
                                    <br><br>
                                <li><b>Branch Wise Weekly Comparison</b>
                                <br> <br>
                                    <table>{branches_reg_summary_html}</table>
                                </li>
                            </ol>
                        </li>

                         <li>
                            <h3>Eyetest Conversions</h3>
                            <ol>
                                <li><b>Weekly Overall Comparison</b>
                                <br> <br>
                                    <table>{overall_et_html}</table>
                                </li>
                                <br><br>
                                <li><b>Branchwise Weekly Conversion Comparison</b>
                                <br>
                                    <p><b>Note</b> The below eye tests excludes referrals to the opthalmologists.</p>
                                    <table>{branches_et_html}</table>
                                </li>
                                <br><br>
                                <li><b>High RX Weekly Conversion Comparison</b>
                                <br><br>
                                    <table>{summary_higrx_html}</table>
                                </li>
                            </ol>
                        </li>

                        <li>
                            <h3>View RX Conversions</h3>
                            <ol>
                                <li><b>Weekly Overall Comparison</b>
                                <br> <br>
                                    <table>{overall_views_html}</table>
                                </li>
                                <br><br>
                                <li><b>Branchwise Weekly Conversion Comparison</b>
                                <br>
                                    <table>{branches_views_html}</table>
                                </li>
                                <br><br>
                            </ol>
                        </li>
                    </ol>

                    <br><br>
                    Also see the non-conversions on the attachments.
                </div>
            </div>

            <div class = "salutation">
                <b><i>
                    Best Regards <br>
                    Optica Data Team 
                </i></b>
            </div>
        </body>
    </html>
    """.format(

        overall_et_html=overall_et_html,
        branches_et_html=branches_et_html,
        reg_summary_html=reg_summary_html,
        overall_views_html=overall_views_html,
        summary_higrx_html=summary_higrx_html,
        branches_views_html=branches_views_html,
        branches_reg_summary_html=branches_reg_summary_html
    )


    receiver_email = [
        "kush@optica.africa", 
        "raghav@optica.africa", 
        "yuri@optica.africa", 
        "shaik@optica.africa",
        "wairimu@optica.africa", 
        "ian.gathumbi@optica.africa"
    ]

    email_message = MIMEMultipart("alternative")
    email_message["From"] = your_email
    email_message["To"] = r','.join(receiver_email)
    email_message["Subject"] = subject
    email_message.attach(MIMEText(html, "html"))

    attach_file(email_message, reg_non_conversions, "Regisrations Non-Conversions.xlsx")
    attach_file(email_message, ets_non_conversions, "Eye Tests Non_Conversions.xlsx")
    attach_file(email_message, views_non_conversions, "ViewRX Non-Conversions.xlsx")

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(your_email, password)
        server.sendmail(your_email, receiver_email, email_message.as_string())


def send_branches_report():
    ug_srm_rm = fetch_gsheet_data()["ug_srm_rm"]
    #Fetch Uganda's SRM and RM List
    branch_list = ug_srm_rm["Outlet"].to_list()
    ug_srm_rm_index = ug_srm_rm.set_index("Outlet")


    # Registrations
    reg_branch_path = f"{uganda_path}conversion/registrations/branch.xlsx"
    reg_salesperson_path = f"{uganda_path}conversion/registrations/sales_persons.xlsx"
    reg_branch = pd.ExcelFile(reg_branch_path)
    reg_salespersons = pd.ExcelFile(reg_salesperson_path)

    #Eye Tests
    eyetest_branch_path = f"{uganda_path}conversion/eyetests/branches.xlsx"
    eyetest_optom_path = f"{uganda_path}conversion/eyetests/opthoms.xlsx"
    eyetest_salesperson_path = f"{uganda_path}conversion/eyetests/sales_persons.xlsx"
    eyetest_branch = pd.ExcelFile(eyetest_branch_path)
    eyetest_salespersons = pd.ExcelFile(eyetest_salesperson_path)
    eyetest_optom = pd.ExcelFile(eyetest_optom_path)

    #Views
    views_branch_path = f"{uganda_path}conversion/viewrx/branches.xlsx"
    views_salespersons_path = f"{uganda_path}conversion/viewrx/sales_persons.xlsx"
    views_branch = pd.ExcelFile(views_branch_path)
    views_salespersons = pd.ExcelFile(views_salespersons_path)

    for branch in branch_list:
        if branch in reg_branch.sheet_names and branch in eyetest_branch.sheet_names and branch in views_branch.sheet_names:
            branch_name = ug_srm_rm_index.loc[branch, "Branch"]
            branch_email = ug_srm_rm_index.loc[branch, "Email"]
            branch_rm = ug_srm_rm_index.loc[branch, "RM Email"]

            """Registrations Parsing, Style and HTML"""
            branch_reg_report = reg_branch.parse(str(branch), index_col=False)
            salesperson_reg_report = reg_salespersons.parse(str(branch), index_col=False)
            branch_reg_html = style_dataframe(branch_reg_report, ug_styles, properties)
            salespersons_reg_html = style_dataframe(salesperson_reg_report, ug_styles, properties)

            branch_eyetest_report = eyetest_branch.parse(str(branch), index_col=False)
            salespersons_eyetests_report = eyetest_salespersons.parse(str(branch), index_col=False)
            optom_eyetests_report = eyetest_optom.parse(str(branch), index_col=False)
            branch_eyetest_html = style_dataframe(branch_eyetest_report, ug_styles, properties)
            salesperson_eyetest_html = style_dataframe(salespersons_eyetests_report, ug_styles, properties)
            optom_eyetest_html = style_dataframe(optom_eyetests_report, ug_styles, properties)

            views_branch_report = views_branch.parse(str(branch), index_col=False)
            salespersons_views_report = views_salespersons.parse(str(branch), index_col=False)
            views_branch_html = style_dataframe(views_branch_report, ug_styles, properties)
            salespersons_views_html = style_dataframe(salespersons_views_report, ug_styles, properties)

            #Non Conversions
            eyetests_non_conversions = pd.ExcelFile(f"{uganda_path}conversion/eyetests/non_conversions.xlsx")
            registrations_non_conversions = pd.ExcelFile(f"{uganda_path}conversion/registrations/non_conversions.xlsx")
            views_non_conversions = pd.ExcelFile(f"{uganda_path}conversion/viewrx/non_conversions.xlsx")

            html = """
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta http-equiv="X-UA-Compatible" content="IE=edge">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>Uganda Outlets Conversion Report</title>

                    <style>
                        table {{border-collapse: collapse;font-family:Trebuchet MS; font-size:9;}}
                        th {{text-align: left;font-family:Trebuchet MS; font-size:9; padding: 3px;}}
                        body, p, h3, div, span, var {{font-family:Trebuchet MS; font-size:13}}
                        td {{text-align: left;font-family:Trebuchet MS; font-size:9; padding: 8px;}}
                        h4 {{font-size: 12px; font-family: Verdana, sans-serif;}}

                        .content {{
                            margin-top: 20px !important;
                            border: none;
                            background-color: white;
                            padding: 4%;
                            width: 85%;
                            margin: 0 auto;
                            border-radius: 3px;
                        }}

                        .salutation {{
                            width: 20%;
                            margin: 0 auto;
                            margin-top: 20px;
                            font-size: 10px;
                        }}

                        .inner {{
                            margin: 5px 0px 5px 0px;
                            padding: 4px;
                        }}

                    </style>
                </head>
                <body style = "background-color: #F0F0F0; padding: 2% 2% 2% 2%;">
                    <div class = "content">
                        <div>
                            <div>

                            <h3>Conversion Report</h3>
                            <b>Dear {branch_name},</b>
                            <p>This report shows conversions for registrations, eye tests, and view RX. 
                            Kindly review this report to identify any areas that need improvement. </p>
                            </div>

                            <ol>
                                <li><h3>Registrations Conversion</h3>
                                    <ol>
                                        <li class = "inner">Branch Conversion
                                            <table>{branch_reg_html}</table>
                                        </li>

                                        <li class = "inner">Staff Conversion <br><br>
                                            <tabel>{salespersons_reg_html}</tabel>
                                        </li>
                                    </ol>
                                </li>


                                <li><h3>Eye Tests Conversion</h3>
                                    <ol>
                                        <li class = "inner">Branch Conversion
                                            <table>{branch_eyetest_html}</table>
                                        </li>

                                        <li class = "inner"> Conversion by Optom <br><br>
                                            <table>{optom_eyetest_html}</table>
                                        </li>

                                        <li class = "inner">Conversion by EWC Handover<br><br>
                                            <table>{salesperson_eyetest_html}</table>
                                        </li>
                                    </ol>

                                </li>

                                <li><h3>View RX Conversion</h3>
                                    <ol>
                                        <li class = "inner">Branch Conversion
                                            <table>{views_branch_html}</table>
                                        </li>

                                        <li class = "inner">Staff Conversion <br><br>
                                            <tabl>{salespersons_views_html}</tabl>
                                        </li>
                                    </ol>

                                </li>
                            </ol>

                            <p>Kindly see a list of non-conversions of the attachments.</p>

                        </div>

                    </div>
                </body>
                </html>
            """.format(

                branch_name = branch_name,
                branch_reg_html = branch_reg_html,
                salespersons_reg_html =  salespersons_reg_html,
                branch_eyetest_html = branch_eyetest_html,
                salesperson_eyetest_html = salesperson_eyetest_html,
                optom_eyetest_html = optom_eyetest_html,
                views_branch_html = views_branch_html,
                salespersons_views_html = salespersons_views_html

            )

            receiver_email = [
                "raghav@optica.africa",
                branch_rm,
                "wairimu@optica.africa", 
                "ian.gathumbi@optica.africa",
                branch_email
            ]
            
            email_message = MIMEMultipart("alternative")
            email_message["From"] = your_email
            email_message["To"] = r','.join(receiver_email)
            email_message["Subject"] = f"{branch_name} Registrations, Eye Tests, and View RX Conversion Report from {fourth_week_start} to {fourth_week_end}"
            email_message.attach(MIMEText(html, "html"))

            if branch in eyetests_non_conversions.sheet_names:
                save_file(email_message, eyetests_non_conversions, branch, branch_name, "EyeTests")
            
            if branch in registrations_non_conversions.sheet_names:
                save_file(email_message, registrations_non_conversions, branch, branch_name, "Registrations")
            
            if branch in views_non_conversions.sheet_names:
                save_file(email_message, views_non_conversions, branch, branch_name, "ViewRX")
                

            context = ssl.create_default_context()
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                server.login(your_email, password)
                server.sendmail(your_email, receiver_email, email_message.as_string())




def clean_registrations():
    clean_folder(dir_name=f"{uganda_path}conversion/registrations/")


def clean_eyetests():
    clean_folder(dir_name=f"{uganda_path}conversion/eyetests/")


def clean_views():
    clean_folder(dir_name=f"{uganda_path}conversion/viewrx/")


if __name__ == '__main__':
    send_management_report()
    send_branches_report()
    clean_registrations()
    clean_eyetests()
    clean_views()
