import quopri
import random
import os
import ssl
import smtplib
import pandas as pd
from airflow.models import variable
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from email.mime.text import MIMEText
from sub_tasks.libraries.utils import (
    attach_file,
    clean_folder,
    get_yesterday_date,
    first_week_start,
    fourth_week_start,
    fourth_week_end,
    get_comparison_months,
    create_initial_file,
    record_sent_branch,
    return_sent_emails,
    assert_date_modified
)
from reports.draft_to_upload.smtp.emails import (
    test,
    kenya_daily,
    kenya_weekly,
    kenya_monthly,
    uganda_daily,
    uganda_weekly,
    uganda_monthly,
    rwanda_daily,
    rwanda_weekly,
    rwanda_monthly
)
from sub_tasks.libraries.styles import ug_styles, properties
from sub_tasks.libraries.utils import (highlight_spaces)
from reports.draft_to_upload.utils.utils import highlight_efficiency
from reports.draft_to_upload.html.html import (
    drafts_html, 
    branches_html, 
    html_rejections, 
    html_planos,
    html_no_feedback,
    no_feedback_rejections_html,
    rejections_daily_message,
    rejections_weekly_message,
    plano_daily_message,
    plano_weekly_message,
    feedback_daily_message,
    feedback_weekly_message,
    all_reports_html,
    html_feedback,
    feedback_planos,
    feedback_rejection,
    no_feedback_daily_message,
    feedback_no_feedback_html,
    pl_rej_nof_html,
    plano_feedback_nofeedback_html,
    rejections_feedback_nofeedback_html,
    all_four_html,
    html_plano_no_feedback
)

from reports.draft_to_upload.templates.template import (
    rejections_template,
    planos_template,
    feedback_template,
    no_feedback_template
)


load_dotenv()
sender_email = os.getenv("douglas_email")
password = os.getenv("douglas_password")
receiver_email = ""
subject = ""


def highlight_rejections_sops(value):
    if value > 5:
        colour = "red"
    else:
        colour = "white"

    return 'background-color: {}'.format(colour)


def round_columns(cols, style):
    formatting_dict = {column: "{:,.0f}" for column in cols}
    style = style.format(formatting_dict)
    html = style.to_html(doctype_html=True)

    return html


"""
Simple Mail Transfer Protocol (SMTP) Documentation
Note: Please do not make any changes to this document unless you intend to add a table.

HTML Module
To add a table to the HTML, navigate to the HTML module located in the same folder as this SMTP document.

Testing the Email
If you wish to test the email before sending it to the respective recipients, follow these steps:

Locate the file specific to the country you are targeting.
Find the function named "trigger_country_smtp" and replace the word "country" with the actual country you are targeting.
Modify the country parameter of the "trigger_country_smtp" function to "Test".
Please ensure that the word "Test" is written exactly as shown above to prevent accidental email sending.

Alert for Missing Data or Empty Reports
Even if the pipelines fail and there is no data available, the report will still run.

It is important to regularly check if the pipelines are running properly to avoid sending incorrect or incomplete reports.
"""


def send_draft_upload_report(
    selection: str, 
    country: str, 
    path: str, 
    target: int
    ):
    lower = selection.lower()
    draft_path = f"{path}draft_upload/draft_to_upload.xlsx"
    rejections_path = f"{path}draft_upload/rejections_report.xlsx"
    sops_path = f"{path}draft_upload/sop_compliance.xlsx"
    planos_path = f"{path}draft_upload/planorx_not_submitted.xlsx"
    dectractors_path = f"{path}draft_upload/detractors_report.xlsx"
    opening_path = f"{path}draft_upload/opening_time.xlsx"
    non_views_path = f"{path}draft_upload/non_view.xlsx"
    mtd_insurance_path = f"{path}draft_upload/mtd_insurance_conversion.xlsx"

    files = [
        draft_path,
        rejections_path,
        sops_path,
        planos_path,
        dectractors_path,
        opening_path,
        non_views_path
    ]

    # if not assert_date_modified(files):
    #     return


    if country == "Kenya" and not os.path.exists(draft_path) and not os.path.exists(rejections_path):
        return
    
    if not os.path.exists(draft_path) and not os.path.exists(rejections_path) and not os.path.exists(sops_path) and not os.path.exists(planos_path):
        return

    draft_html = ""
    draft_attachment = ""
    rejections_html = ""
    rejections_attachment = ""
    sops_html = ""
    sops_attachment = ""
    plano_html = ""
    plano_attachment = ""
    detractors_html = ""
    detractors_attachment = ""
    opening_html = ""
    no_view_html = ""
    no_view_attachment = ""
    mtd_insurance_attachment = ""

    if selection == "Daily":
        todate = get_yesterday_date(truth=True)
        subject = f"{country} {selection} Report - Insurance Rejections/ Draft to Upload / Detractors / SOPs / Plano RX, {todate}"
        if os.path.exists(draft_path):
            draft = pd.ExcelFile(draft_path)
            draft_toupload_report = draft.parse(
                f"{lower}_summary", index_col=False).fillna(" ")
            draft_style = draft_toupload_report.style.hide_index().set_table_styles(ug_styles).apply(
                highlight_spaces, axis=1).applymap(highlight_efficiency, subset=[f"% Efficiency (Target: {target} mins)"])
            draft_html = draft_style.to_html(doctype_html=True)
            draft_attachment = draft_path
        else:
            draft_html = "<p>Seems like nobody uploaded attachment during the above period</p>"

        if os.path.exists(rejections_path):
            rejections = pd.ExcelFile(rejections_path)
            rejections_report = rejections.parse(
                f"daily_summary", index_col=False).fillna(" ")
            rejections_style = rejections_report.style.hide_index().set_table_styles(ug_styles).apply(
                highlight_spaces, axis=1).applymap(highlight_rejections_sops, subset=["% Rejected"])
            rejections_html = rejections_style.to_html(doctype_html=True)
            rejections_attachment = rejections_path
        else:
            rejections_html = "<p>No insurance orders were rejected during the above period!</p>"

        if os.path.exists(sops_path):
            sops = pd.ExcelFile(sops_path)
            sops_report = sops.parse(
                (f"daily_summary"), index_col=False).fillna(" ")
            sops_styles = sops_report.style.hide_index().set_table_styles(ug_styles).apply(
                highlight_spaces, axis=1).applymap(highlight_rejections_sops, subset=["% SOP/Customers"])
            sops_html = sops_styles.to_html(doctype_html=True)
            sops_attachment = sops_path

        else:
            sops_html = "All the Branches were compliant for the above period."

        if os.path.exists(planos_path):
            plano_html = "Please find attached the report for Plano Eye Tests for the period on the subject line."
            plano_attachment = planos_path

        else:
            plano_html = "There were no plano eye tests for the above period."

        if os.path.exists(non_views_path):
            no_view_html = "Please find attached a list of non-converted eye tests that were not viewed."
            no_view_attachment = non_views_path
        
        else:
            no_view_html = "All non-converted eye tests were viewed."

        if os.path.exists(mtd_insurance_path):
            no_view_html = "Please find attached a mtd insurance conversion report"
            mtd_insurance_attachment = mtd_insurance_path

        if os.path.exists(dectractors_path):
            detractors = pd.ExcelFile(dectractors_path)
            detractors_report = detractors.parse(
                f"daily_summary", index_col=False).fillna(" ")
            detractors_style = detractors_report.style.hide_index(
            ).set_table_styles(ug_styles).apply(highlight_spaces, axis=1)
            detractors_html = detractors_style.to_html(doctype_html=True)
            detractors_attachment = dectractors_path
        else:
            detractors_html = "No Detractors So Far!"

        if os.path.exists(opening_path):
            opening_report = pd.read_excel(opening_path, index_col=False)
            opening_style = opening_report.style.hide_index().set_table_styles(
                ug_styles).apply(highlight_spaces, axis=1)
            opening_html = opening_style.to_html(doctype_html=True)
        else:
            opening_html = "No Branch Opened After their Opening Time."

        if country == "Kenya":
            receiver_email = kenya_daily
        elif country == "Uganda":
            receiver_email = uganda_daily
        elif country == "Rwanda":
            receiver_email = rwanda_daily
        elif country == "Test":
            receiver_email = test
        else:
            return

    if selection == "Weekly":
        subject = f"{country} {selection} Report - Insurance Rejections/ Draft to Upload / Detractors / SOPs / Plano RX Not Submitted, from {fourth_week_start} to {fourth_week_end}"
        if os.path.exists(draft_path):
            draft = pd.ExcelFile(draft_path)
            draft_toupload_report = draft.parse(
                f"{lower}_summary", index_col=False)
            draft_style = draft_toupload_report.style.hide_index().set_table_styles(ug_styles)
            columns_to_format = draft_toupload_report.columns[3:]
            draft_html = round_columns(columns_to_format, draft_style)
            draft_attachment = draft_path
        else:
            draft_html = "<p>Seems like nobody uploaded attachment during the above period</p>"

        if os.path.exists(rejections_path):
            rejections = pd.ExcelFile(rejections_path)
            rejections_report = rejections.parse(
                f"{lower}_summary",
                index_col=[0],
                header=[0, 1]
            ).dropna(axis=0).reset_index()

            rejections_report = rejections_report.rename(
                columns={
                    "index": "", 
                    "Unnamed: 1_level_0": "",
                    "Week Range": ""
                },
                level=0
            )

            rejections_report = rejections_report.rename(
                columns={
                    "": "Outlet", 
                    "Unnamed: 1_level_1": "RM",
                    "Unnamed: 2_level_1": "SRM"
                },
                level=1
            )

            rejections_style = rejections_report.style.hide_index().set_table_styles(ug_styles)
            columns_to_format = rejections_report.columns[3:]
            rejections_html = round_columns(
                columns_to_format, 
                rejections_style
            )
            rejections_attachment = rejections_path

        else:
            rejections_html = "<p>No insurance orders were rejected during the above period!</p>"

        if os.path.exists(sops_path):
            sops = pd.ExcelFile(sops_path)
            sops_report = sops.parse(
                f"{lower}_summary",
                index_col=[0],
                header=[0, 1]
            ).dropna(axis=0)

            sops_report = sops_report.rename(
                columns={
                    "Unnamed: 1_level_1": 
                    "Outlet", 
                    "Unnamed: 2_level_1": "RM", 
                    "Unnamed: 3_level_1": "SRM"
                }, level=1
            )
            sops_report = sops_report.rename(
                columns={"Outlet": "", "RM": "", "SRM": ""}, level=0)
            sops_styles = sops_report.style.hide_index().set_table_styles(ug_styles)
            sops_html = sops_styles.to_html(doctype_html=True)
            sops_attachment = sops_path

        else:
            sops_html = "All the Branches were compliant for the above period."

        if os.path.exists(dectractors_path):
            detractors = pd.ExcelFile(dectractors_path)
            detractors_report = detractors.parse(
                f"{lower}_summary", index_col=False).fillna(" ")
            detractors_style = detractors_report.style.hide_index(
            ).set_table_styles(ug_styles).apply(highlight_spaces, axis=1)
            detractors_html = detractors_style.to_html(doctype_html=True)
            detractors_attachment = dectractors_path

        else:
            detractors_html = "No Detractors So Far!"

        if os.path.exists(planos_path):
            plano_html = "Please find attached the report for Plano Eye Tests for the period on the subject line."
            plano_attachment = planos_path

        else:
            plano_html = "There were no plano eye tests for the above period."

        if os.path.exists(non_views_path):
            no_view_html = "Please find attached a list of non-converted eye tests that were not viewed."
            no_view_attachment = non_views_path
        
        else:
            no_view_html = "All non-converted eye tests were viewed."

        if os.path.exists(mtd_insurance_path):
            no_view_html = "Please find attached a mtd insurance conversion report"
            mtd_insurance_attachment = mtd_insurance_path

        if os.path.exists(opening_path):
            opening_report = pd.read_excel(opening_path, index_col=False)
            opening_style = opening_report.style.hide_index().set_table_styles(
                ug_styles).apply(highlight_spaces, axis=1)
            opening_html = opening_style.to_html(doctype_html=True)
        else:
            opening_html = "No Branch Opened After their Opening Time."

        if country == "Kenya":
            receiver_email = kenya_weekly
        elif country == "Uganda":
            receiver_email = uganda_weekly
        elif country == "Rwanda":
            receiver_email = rwanda_weekly
        elif country == "Test":
            receiver_email = test
        else:
            return

    if selection == "Monthly":
        first_month, second_month = get_comparison_months()
        subject = f"{country} {selection} Report - Insurance Rejections/ Draft to Upload / Detractors / SOPs / Plano RX Not Submitted, for {first_month} and {second_month}"
        if os.path.exists(draft_path):
            draft = pd.ExcelFile(draft_path)
            draft_toupload_report = draft.parse(
                f"{lower}_summary",
                index_col=[0],
                header=[0, 1]
            )

            draft_toupload_report = draft_toupload_report.drop(draft_toupload_report.index[0])

            draft_toupload_report = draft_toupload_report.reset_index()
            draft_toupload_report = draft_toupload_report.rename(
                columns={"index": "", "Unnamed: 1_level_0": "", "Month": ""}, level=0)
            draft_toupload_report = draft_toupload_report.rename(
                columns={"": "Outlet", "Unnamed: 1_level_1": "RM", "Unnamed: 2_level_1": "SRM"}, level=1)
            cols = draft_toupload_report.columns[3:]
            draft_style = draft_toupload_report.style.hide_index().set_table_styles(ug_styles)
            draft_html = round_columns(cols, draft_style)
            draft_attachment = draft_path
        else:
            draft_html = "<p>Seems like nobody uploaded attachment during the above period</p>"

        if os.path.exists(rejections_path):
            rejections = pd.ExcelFile(rejections_path)
            rejections_report = rejections.parse(
                f"{lower}_summary",
                index_col=[0],
                header=[0, 1]
            ).fillna(" ").rename(columns={"Unnamed: 1_level_0": "", "Unnamed: 2_level_0": "", "Unnamed: 3_level_0": ""}, level=0)

            rejections_style = rejections_report.style.hide_index().set_table_styles(ug_styles)
            rejections_html = rejections_style.to_html(doctype_html=True)
            rejections_attachment = rejections_path

        else:
            rejections_html = "<p>No insurance orders were rejected during the above period!</p>"

        if os.path.exists(sops_path):
            sops = pd.ExcelFile(sops_path)
            sops_report = sops.parse(
                f"{lower}_summary",
                index_col=[0],
                header=[0, 1]
            ).dropna(axis=0).reset_index()

            sops_report = sops_report.rename(
                columns={"Unnamed: 1_level_0": "", "Month": "", "index": ""}, level=0)
            sops_report = sops_report.rename(
                columns={"": "Outlet", "Unnamed: 1_level_1": "RM", "Unnamed: 2_level_1": "SRM"}, level=1
            )
            cols = sops_report.columns[3:]
            sops_styles = sops_report.style.hide_index().set_table_styles(ug_styles)
            sops_html = round_columns(cols, sops_styles)
            sops_attachment = sops_path

        else:
            sops_html = "All the Branches were compliant for the above period."

        if os.path.exists(dectractors_path):
            detractors = pd.ExcelFile(dectractors_path)
            detractors_report = detractors.parse(
                f"{lower}_summary", index_col=False).fillna(" ")
            detractors_style = detractors_report.style.hide_index(
            ).set_table_styles(ug_styles).apply(highlight_spaces, axis=1)
            detractors_html = detractors_style.to_html(doctype_html=True)
            detractors_attachment = dectractors_path

        else:
            detractors_html = "No Detractors So Far!"

        if os.path.exists(planos_path):
            plano_html = "Please find attached the report for Plano Eye Tests for the period on the subject line."
            plano_attachment = planos_path

        else:
            plano_html = "There were no plano eye tests for the above period."

        if os.path.exists(non_views_path):
            no_view_html = "Please find attached a list of non-converted eye tests that were not viewed."
            no_view_attachment = non_views_path
        
        else:
            no_view_html = "All non-converted eye tests were viewed."

        if os.path.exists(mtd_insurance_path):
            no_view_html = "Please find attached a mtd insurance conversion report"
            mtd_insurance_attachment = mtd_insurance_path

        if os.path.exists(opening_path):
            opening_report = pd.read_excel(opening_path, index_col=False)
            opening_style = opening_report.style.hide_index().set_table_styles(
                ug_styles).apply(highlight_spaces, axis=1)
            opening_html = opening_style.to_html(doctype_html=True)
        else:
            opening_html = "No Branch Opened After their Opening Time."


        if country == "Kenya":
            receiver_email = kenya_monthly
        elif country == "Uganda":
            receiver_email = uganda_monthly
        elif country == "Rwanda":
            receiver_email = rwanda_monthly
        elif country == "Test":
            receiver_email = test
        else:
            return

    html = drafts_html.format(
        draft_html=draft_html,
        rejections_html=rejections_html,
        sops_html=sops_html,
        plano_html=plano_html,
        detractors_html=detractors_html,
        opening_html=opening_html,
        no_view_html = no_view_html
    )

    html_content = quopri.encodestring(html.encode("utf-8")).decode("utf-8")
    email_message = MIMEMultipart("alternative")
    email_message["From"] = sender_email
    email_message["To"] = ",".join(receiver_email)
    email_message["Subject"] = subject

    html_part = MIMEText(html_content, "html")
    html_part.replace_header(
        "Content-Transfer-Encoding", "quoted-printable")
    email_message.attach(html_part)

    if os.path.exists(draft_attachment):
        attach_file(
            email_message, 
            draft_attachment, 
            "draft_to_upload.xlsx"
        )

    if os.path.exists(rejections_attachment):
        attach_file(
            email_message, 
            rejections_attachment,
            "rejections_report.xlsx"
        )

    if os.path.exists(sops_attachment):
        attach_file(
            email_message, 
            sops_attachment, 
            "sop_compliance.xlsx"
        )

    if os.path.exists(plano_attachment):
        attach_file(
            email_message, 
            plano_attachment, 
            "planorx.xlsx"
        )

    if os.path.exists(detractors_attachment):
        attach_file(
            email_message,
            detractors_attachment,
            "detractors_report.xlsx"
        )

    if os.path.exists(no_view_attachment):
        attach_file(
            email_message,
            no_view_attachment,
            "Non_Converted_Non_Views.xlsx"
        )

    if os.path.exists(mtd_insurance_attachment):
        attach_file(
            email_message,
            mtd_insurance_attachment,
            "MTD_Insurance_Conversion.xlsx"
        )

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(
            sender_email,
            receiver_email,
            email_message.as_string()
        )


req_columns = [
    "Code",
    "Create Date",
    "RX Type",
    "Status",
    "Customer Code",
    "Insurance Company",
    "Opthom Name",
    "EWC Handover",
    "Who Viewed RX",
    "Submission"
]

rej_cols = [
    "Order Number",
    "Date",
    "Front Desk",
    "Creator",
    "Order Creator",
    "Created User",
    "Remarks"
]


def send_to_branches(branch_data, selection, path, filename, country):
    create_initial_file(filename)
    todate = get_yesterday_date(truth=True)
    branches = branch_data["Outlet"].to_list()
    html = ""
    branch_data = branch_data.set_index("Outlet")
    selections_lower = selection.lower()
    rejections_path = f"{path}draft_upload/rejections_report.xlsx"
    planos_path = f"{path}draft_upload/planorx_not_submitted.xlsx"
    feedback = f"{path}draft_upload/insurance_daily.xlsx"
    no_feedbacks = f"{path}draft_upload/no_feedbacks.xlsx"

    # files = [
    #     rejections_path, 
    #     planos_path, 
    #     feedback, 
    #     no_feedbacks
    # ]

    # if not assert_date_modified(files):
    #     return

    if os.path.exists(rejections_path) or os.path.exists(planos_path) or os.path.exists(feedback) or os.path.exists(no_feedbacks):
        rejection_branches = []
        planos_branches = []
        feedback_branches = []
        no_feedback_branches = []

        if os.path.exists(feedback):
            feedbacks = pd.ExcelFile(feedback)
            feedbacks_data = feedbacks.parse(
                "daily_data",
                index_col=False
            )
            feedback_branches = feedbacks_data["Outlet"].to_list()

        if os.path.exists(no_feedbacks):
            non_feedback = pd.ExcelFile(no_feedbacks)
            no_feedbacks_data = non_feedback.parse(
                "no_feedback",
                index_col=False
            )
            no_feedback_branches = no_feedbacks_data["Outlet"].to_list()

        if os.path.exists(rejections_path):
            rejections = pd.ExcelFile(rejections_path)
            rejections_data = rejections.parse(
                f"{selections_lower}_rejections_data", 
                index_col=False
            )
            rejection_branches = rejections_data["Outlet"].to_list()
            
        if os.path.exists(planos_path):
            planos = pd.ExcelFile(planos_path)
            planos_data = planos.parse(f"{selections_lower}_data", index_col=False)
            planos_data = planos_data[
                planos_data["Submission"]== "Not Submitted"
            ]

            planos_branches = planos_data["Branch"].to_list()

        all_branches = planos_branches + rejection_branches + feedback_branches + no_feedback_branches
        random_branch = random.choice(all_branches)
        
        if selection == "Daily":
            plano_message = plano_daily_message
            rejection_message = rejections_daily_message
            feedback_message = feedback_daily_message
            no_feedback_message = no_feedback_daily_message
        elif selection == "Weekly":
            plano_message = plano_weekly_message
            rejection_message = rejections_weekly_message
            feedback_message = feedback_weekly_message
        else:
            return

        for branch in branches:
            branch_name = branch_data.loc[branch, "Branch"]
            branch_email = branch_data.loc[branch, "Email"]
            branch_manager = branch_data.loc[branch, "Branch Manager"].split(" ")[0]
            rm_email = branch_data.loc[branch, "RM Group"]
            srm_email = branch_data.loc[branch, "SRM Email"]

            if branch not in rejection_branches and branch not in planos_branches and branch not in feedback_branches:
                continue
                
                """
                Combination: Rejections, Planos, and Feedbacks
                """

            elif branch in planos_branches and branch in rejection_branches and branch in feedback_branches and branch in no_feedback_branches:
                if selection == "Daily":
                    subject = f"{branch} Non Converted Approved Insurance Orders, Insurance Errors, Insurance Orders with no Feedback, and Plano NoN Submissions for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch} Non Converted Approved Insurance Orders, Insurance Errors, Insurance Orders with no Feedback, and Plano NoN Submissions from {fourth_week_start} to {fourth_week_end}"
                else:
                    return
                
                planos_html = planos_template(
                    req_columns=req_columns,
                    planos_data=planos_data,
                    branch=branch
                )

                rejections_html = rejections_template(
                    branch=branch,
                    rejections_data=rejections_data,
                    rej_cols=rej_cols,
                )

                feedback_data_html = feedback_template(
                    branch=branch,
                    feedbacks_data=feedbacks_data
                )

                no_feedback_html = no_feedback_template(
                    branch=branch,
                    data=no_feedbacks_data
                )

                html = all_four_html.format(
                    no_feedback_html = no_feedback_html,
                    branch = branch,
                    branch_manager = branch_manager,
                    no_feedback_message = no_feedback_message,
                    planos=planos_html,
                    rejections=rejections_html,
                    plano_message = plano_message,
                    rejection_message = rejection_message,
                    feedback_data_html = feedback_data_html,
                    feedback_message = feedback_message
                )



            elif branch in rejection_branches and branch in planos_branches and branch in feedback_branches and branch not in no_feedback_branches:
                if selection == "Daily":
                    subject = f"{branch} Non Converted Approved Insurance Orders, Insurance Errors and Plano NoN Submissions for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch} Non Converted Approved Insurance Orders, Insurance Errors and Plano NoN Submissions from {fourth_week_start} to {fourth_week_end}"
                else:
                    return
                
                planos_html = planos_template(
                    req_columns=req_columns,
                    planos_data=planos_data,
                    branch=branch
                )

                rejections_html = rejections_template(
                    branch=branch,
                    rejections_data=rejections_data,
                    rej_cols=rej_cols,
                )

                feedback_data_html = feedback_template(
                    branch=branch,
                    feedbacks_data=feedbacks_data,
                )

                html = all_reports_html.format(
                    planos=planos_html,
                    rejections=rejections_html,
                    branch=branch_name,
                    branch_manager=branch_manager,
                    plano_message = plano_message,
                    rejection_message = rejection_message,
                    feedback_data_html = feedback_data_html,
                    feedback_message = feedback_message
                )

                """
                Combination: Plano and Rejections"
                """
            elif branch in rejection_branches and branch in planos_branches and branch not in feedback_branches and branch not in no_feedback_branches:
                if selection == "Daily":
                    subject = f"{branch} Insurance Errors and Plano NoN Submissions for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch} Insurance Errors and Plano NoN Submissions from {fourth_week_start} to {fourth_week_end}"
                else:
                    return

                planos_html = planos_template(
                    req_columns=req_columns,
                    planos_data=planos_data,
                    branch=branch
                )

                rejections_html = rejections_template(
                    branch=branch,
                    rejections_data=rejections_data,
                    rej_cols=rej_cols,
                )

                html = branches_html.format(
                    planos=planos_html,
                    rejections=rejections_html,
                    branch=branch_name,
                    branch_manager=branch_manager,
                    plano_message = plano_message,
                    rejection_message = rejection_message
                )

            
                """
                Combination: "Feedbacks Only"
                """
            elif branch in feedback_branches and branch not in rejection_branches and branch not in planos_branches and branch not in no_feedback_branches:
                if selection == "Daily":
                    subject = f"{branch} Non Converted Approved Insurance Orders for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch} Non Converted Approved Insurance Orders from {fourth_week_start} to {fourth_week_end}"
                else:
                    return
                
                feedback_data_html = feedback_template(
                    branch=branch,
                    feedbacks_data=feedbacks_data
                )

                html = html_feedback.format(
                    branch=branch_name,
                    branch_manager=branch_manager,
                    feedback_data_html = feedback_data_html,
                    feedback_message = feedback_message
                )


                """
                Combination: No Feedback Only
                """
            elif branch in no_feedback_branches and branch not in rejection_branches and branch not in planos_branches and branch not in feedback_branches:
                if selection == "Daily":
                    subject = f"{branch} Insurance Orders With No Feedback for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch} Insurance Orders With No Feedback from {fourth_week_start} to {fourth_week_end}"
                else:
                    return
                
                no_feedback_html = no_feedback_template(
                    branch=branch,
                    data=no_feedbacks_data
                )

                html = html_no_feedback.format(
                    no_feedback_html = no_feedback_html,
                    branch = branch,
                    branch_manager = branch_manager,
                    no_feedback_message = no_feedback_message
                )

                """
                Combination: Rejections Only
                """
            elif branch in rejection_branches and branch not in planos_branches and branch not in feedback_branches and branch not in no_feedback_branches:
                if selection == "Daily":
                    subject = f"{branch} Insurance Errors for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch} Insurance Errors from {fourth_week_start} to {fourth_week_end}"
                else:
                    return
               
                rejections_html = rejections_template(
                    branch=branch,
                    rejections_data=rejections_data,
                    rej_cols=rej_cols
                )

                html = html_rejections.format(
                    rejections=rejections_html,
                    branch=branch_name,
                    branch_manager=branch_manager,
                    rejection_message = rejection_message
                )

                """
                Combination: Planos Only
                """
            elif branch in planos_branches and branch not in rejection_branches and branch not in feedback_branches and branch not in no_feedback_branches:
                if selection == "Daily":
                    subject = f"{branch} Plano No Submissions for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch} Plano No Submissions from {fourth_week_start} to {fourth_week_end}"
                else:
                    return

                planos_html = planos_template(
                    req_columns=req_columns,
                    planos_data=planos_data,
                    branch=branch
                )

                html = html_planos.format(
                    planos=planos_html,
                    branch=branch_name,
                    branch_manager=branch_manager,
                    plano_message = plano_message
                )

                """
                Combination: Feedbacks and Rejections
                """
            elif branch in feedback_branches and branch in rejection_branches and branch not in planos_branches and branch not in no_feedback_branches:
                if selection == "Daily":
                    subject = f"{branch} Non Converted Approved Insurance Orders and Insurance Errors for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch} Non Converted Approved Insurance Orders and Insurance Errors from {fourth_week_start} to {fourth_week_end}"
                else:
                    return

                rejections_html = rejections_template(
                    branch=branch,
                    rejections_data=rejections_data,
                    rej_cols=rej_cols,
                )

                feedback_data_html = feedback_template(
                    branch = branch,
                    feedbacks_data=feedbacks_data
                )


                html = feedback_rejection.format(
                    rejections=rejections_html,
                    feedback_message = feedback_message,
                    branch=branch_name,
                    branch_manager=branch_manager,
                    rejection_message = rejection_message,
                    feedback_data_html=feedback_data_html
                )

                """
                Combination: Feedbacks and Planos
                """
            elif branch in feedback_branches and  branch in planos_branches and branch not in rejection_branches and branch not in no_feedback_branches:
                if selection == "Daily":
                    subject = f"{branch} Non Converted Approved Insurance Orders and Plano No Submissions for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch} Non Converted Approved Insurance Orders and Plano No Submissions from {fourth_week_start} to {fourth_week_end}"
                else:
                    return
                

                planos_html = planos_template(
                    req_columns=req_columns,
                    planos_data=planos_data,
                    branch=branch
                )

                feedback_data_html = feedback_template(
                    branch = branch,
                    feedbacks_data=feedbacks_data
                )

                
                html = feedback_planos.format(
                    planos=planos_html,
                    branch=branch_name,
                    branch_manager=branch_manager,
                    plano_message = plano_message,
                    feedback_data_html=feedback_data_html,
                    feedback_message = feedback_message
                )

                """
                Combination: No Feedback and Planos
                """
            elif branch in planos_branches and branch in no_feedback_branches and branch not in rejection_branches and branch not in feedback_branches:
                if selection == "Daily":
                    subject = f"{branch} Insurance Orders with no Feedback and Plano NON Submission for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch} Insurance Orders with no Feedback and Plano NON Submission from {fourth_week_start} to {fourth_week_end}"
                else:
                    return
                

                planos_html = planos_template(
                    req_columns=req_columns,
                    planos_data=planos_data,
                    branch=branch
                )

                no_feedback_html = no_feedback_template(
                    branch=branch,
                    data=no_feedbacks_data
                )

                html = html_plano_no_feedback.format(
                    no_feedback_html = no_feedback_html,
                    branch = branch,
                    branch_manager = branch_manager,
                    no_feedback_message = no_feedback_message,
                    planos = planos_html,
                    plano_message = plano_message
                )
            
                """
                Combination: "Rejections and No Feedback
                """
            
            elif branch in rejection_branches and branch in no_feedback_branches and branch not in planos_branches and branch not in feedback_branches:
                if selection == "Daily":
                    subject = f"{branch} Insurance Errors and Orders with No Feedback for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch}  Insurance Errors and Orders with No Feedback from {fourth_week_start} to {fourth_week_end}"
                else:
                    return
                
                rejections_html = rejections_template(
                    branch=branch,
                    rejections_data=rejections_data,
                    rej_cols=rej_cols,
                )

                no_feedback_html = no_feedback_template(
                    branch=branch,
                    data=no_feedbacks_data
                )

                html = no_feedback_rejections_html.format(
                    rejections=rejections_html,
                    branch=branch_name,
                    branch_manager=branch_manager,
                    rejection_message = rejection_message,
                    no_feedback_message = no_feedback_message,
                    no_feedback_html = no_feedback_html
                )

                """
                Combination: Feedback and No Feedback
                """
            elif branch in feedback_branches and branch in no_feedback_branches and branch not in planos_branches and branch not in rejection_branches:
                if selection == "Daily":
                    subject = f"{branch} NON Converted Approved Insurance Orders and Orders with no Feedback for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch} NON Converted Approved Insurance Orders and Orders with no Feedback from {fourth_week_start} to {fourth_week_end}"
                else:
                    return
                
                feedback_data_html = feedback_template(
                    branch=branch,
                    feedbacks_data=feedbacks_data
                )

                no_feedback_html = no_feedback_template(
                    branch=branch,
                    data=no_feedbacks_data
                )

                html = feedback_no_feedback_html.format(
                    branch=branch_name,
                    branch_manager=branch_manager,
                    feedback_data_html = feedback_data_html,
                    feedback_message = feedback_message,
                    no_feedback_message = no_feedback_message,
                    no_feedback_html = no_feedback_html
                )

                """
                Combination: Rejections, No Feedback, and Planos
                """
            elif branch in planos_branches and branch in rejection_branches and branch in no_feedback_branches and branch not in feedback_branches:
                if selection == "Daily":
                    subject = f"{branch} Insurance Errors, Insurance Orders with no Feedback, and Plano NON Submission for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch}  Insurance Errors, Insurance Orders with no Feedback, and Plano NON Submission from {fourth_week_start} to {fourth_week_end}"
                else:
                    return
                
                planos_html = planos_template(
                    req_columns=req_columns,
                    planos_data=planos_data,
                    branch=branch
                )

                rejections_html = rejections_template(
                    branch=branch,
                    rejections_data=rejections_data,
                    rej_cols=rej_cols
                )

                no_feedback_html = no_feedback_template(
                    branch=branch,
                    data=no_feedbacks_data
                )

                html =  pl_rej_nof_html.format(
                    rejections=rejections_html,
                    branch=branch_name,
                    branch_manager=branch_manager,
                    rejection_message = rejection_message,
                    no_feedback_message = no_feedback_message,
                    no_feedback_html = no_feedback_html,
                    planos = planos_html,
                    plano_message = plano_message
                )

                """
                Combination: Planos, Feedbacks and No Feedbacks
                """
            elif branch in planos_branches and branch in feedback_branches and branch in no_feedback_branches and branch not in rejection_branches:
                if selection == "Daily":
                    subject = f"{branch} Approved NON Converted Insurance Orders, Insurance Orders with no Feedback, and Plano NON Submission for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch}  Approved NON Converted Insurance Orders, Insurance Orders with no Feedback, and Plano NON Submission from {fourth_week_start} to {fourth_week_end}"
                else:
                    return

                planos_html = planos_template(
                    req_columns=req_columns,
                    planos_data=planos_data,
                    branch=branch
                )

                feedback_data_html = feedback_template(
                    branch=branch,
                    feedbacks_data=feedbacks_data
                )

                no_feedback_html = no_feedback_template(
                    branch=branch,
                    data=no_feedbacks_data
                )

                html = plano_feedback_nofeedback_html.format(
                    branch=branch_name,
                    branch_manager=branch_manager,
                    no_feedback_message = no_feedback_message,
                    no_feedback_html = no_feedback_html,
                    planos = planos_html,
                    plano_message = plano_message,
                    feedback_data_html = feedback_data_html,
                    feedback_message = feedback_message,
                )

            
            elif branch in rejection_branches and branch in feedback_branches and branch in no_feedback_branches and branch not in planos_branches:
                if selection == "Daily":
                    subject = f"{branch} Approved NON Converted Insurance Orders, Insurance Errors, and Insurance Orders with no Feedback for {todate}"
                elif selection == "Weekly":
                    subject = f"{branch}  Approved NON Converted Insurance Orders, Insurance Errors, and Insurance Orders with no Feedback from {fourth_week_start} to {fourth_week_end}"
                else:
                    return

                rejections_html = rejections_template(
                    branch=branch,
                    rejections_data=rejections_data,
                    rej_cols=rej_cols
                )

                feedback_data_html = feedback_template(
                    branch=branch,
                    feedbacks_data=feedbacks_data,
                )

                no_feedback_html = no_feedback_template(
                    branch=branch,
                    data=no_feedbacks_data
                )

                html = rejections_feedback_nofeedback_html.format(
                    branch=branch_name,
                    branch_manager=branch_manager,
                    no_feedback_message = no_feedback_message,
                    no_feedback_html = no_feedback_html,
                    feedback_data_html = feedback_data_html,
                    feedback_message = feedback_message,
                    rejections=rejections_html,
                    rejection_message = rejection_message
                )

            if branch == random_branch and country == "Kenya" and branch != "OHO":
                receiver_email = [
                    "wazeem@optica.africa",
                    "yuri@optica.africa",
                    "andrew@optica.africa",
                    srm_email,
                    rm_email,
                    branch_email,
                    "wairimu@optica.africa"
                ]

            elif branch == "YOR":
                receiver_email = [
                    srm_email,
                    rm_email,
                    "insurance@optica.africa",
                    "yh.manager@optica.africa",
                    branch_email
                ]

            elif branch == "OHO":
                receiver_email = [
                    srm_email,
                    rm_email,
                    "duncan.muchai@optica.africa",
                    "susan@optica.africa",
                    "insuranceoh@optica.africa",
                    branch_email
                ]

            
           
            elif country == "Uganda":
                receiver_email = [
                    "kush@optica.africa",
                    "raghav@optica.africa",
                    "larry.larsen@optica.africa",
                    'fredrick@optica.africa',
                    'tiffany@optica.africa',
                    "wairimu@optica.africa",
                    branch_email
                ]
            
            elif country == "Rwanda":
                receiver_email = [
                    "kush@optica.africa",
                    "raghav@optica.africa",
                    "wairimu@optica.africa",
                    branch_email
                ]

            else:
                receiver_email = [srm_email, rm_email, branch_email]

            
            if country == "Test":
                receiver_email = ["tstbranch@gmail.com"]


            html_content = quopri.encodestring(
                html.encode("utf-8")
            ).decode("utf-8")
            email_message = MIMEMultipart("alternative")
            email_message["From"] = sender_email
            email_message["To"] = ",".join(receiver_email)
            email_message["Subject"] = subject

            html_part = MIMEText(html_content, "html")
            html_part.replace_header(
                "Content-Transfer-Encoding", "quoted-printable")
            email_message.attach(html_part)

            if branch_email not in return_sent_emails(filename):
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
                        filename
                    )
            else:
                continue


def clean_folders(path):
    clean_folder(dir_name=f"{path}draft_upload/")


if __name__ == '__main__':
    send_draft_upload_report()
    clean_folders()
    send_to_branches()


"""
Please DO NOT Remove the above lines at any point not matter what.
Removing the above lines will cause the email to send endless times.

"""
