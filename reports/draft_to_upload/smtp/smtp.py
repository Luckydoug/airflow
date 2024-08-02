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
    assert_date_modified,
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
    rwanda_monthly,
)

from sub_tasks.libraries.styles import ug_styles, styles_daily
from sub_tasks.libraries.utils import highlight_spaces
from reports.draft_to_upload.utils.utils import highlight_efficiency
from reports.draft_to_upload.utils.utils import generate_html_and_subject
from reports.draft_to_upload.html.html import drafts_html
from sub_tasks.libraries.utils import return_evn_credentials
from sub_tasks.libraries.utils import service_file
import pygsheets

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

    return "background-color: {}".format(colour)


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


def send_draft_upload_report(selection: str, country: str, path: str, target: int):
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
        non_views_path,
    ]

    # if not assert_date_modified(files):
    #     return

    if (
        country == "Kenya"
        and not os.path.exists(draft_path)
        and not os.path.exists(rejections_path)
    ):
        return

    if (
        not os.path.exists(draft_path)
        and not os.path.exists(rejections_path)
        and not os.path.exists(sops_path)
        and not os.path.exists(planos_path)
        and not os.path.exists(dectractors_path)
        and not os.path.exists(opening_path)
        and not os.path.exists(non_views_path)
    ):
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
                f"{lower}_summary", index_col=False
            ).fillna(" ")
            draft_style = (
                draft_toupload_report.style.hide_index()
                .set_table_styles(ug_styles)
                .apply(highlight_spaces, axis=1)
                .applymap(
                    highlight_efficiency,
                    subset=[f"% Efficiency (Target: {target} mins)"],
                )
            )
            draft_html = draft_style.to_html(doctype_html=True)
            draft_attachment = draft_path
        else:
            draft_html = (
                "<p>Seems like nobody uploaded attachment during the above period</p>"
            )

        if os.path.exists(rejections_path):
            rejections = pd.ExcelFile(rejections_path)
            rejections_report = rejections.parse(
                f"daily_summary", index_col=False
            ).fillna(" ")
            rejections_style = (
                rejections_report.style.hide_index()
                .set_table_styles(ug_styles)
                .apply(highlight_spaces, axis=1)
                .applymap(highlight_rejections_sops, subset=["% Rejected"])
            )
            rejections_html = rejections_style.to_html(doctype_html=True)
            rejections_attachment = rejections_path
        else:
            rejections_html = (
                "<p>No insurance orders were rejected during the above period!</p>"
            )

        if os.path.exists(sops_path):
            sops = pd.ExcelFile(sops_path)
            sops_report = sops.parse((f"daily_summary"), index_col=False).fillna(" ")
            sops_styles = (
                sops_report.style.hide_index()
                .set_table_styles(ug_styles)
                .apply(highlight_spaces, axis=1)
                .applymap(highlight_rejections_sops, subset=["% SOP/Customers"])
            )
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
                f"daily_summary", index_col=False
            ).fillna(" ")
            detractors_style = (
                detractors_report.style.hide_index()
                .set_table_styles(ug_styles)
                .apply(highlight_spaces, axis=1)
            )
            detractors_html = detractors_style.to_html(doctype_html=True)
            detractors_attachment = dectractors_path
        else:
            detractors_html = "No Detractors So Far!"

        if os.path.exists(opening_path):
            opening_report = pd.read_excel(opening_path, index_col=False)
            opening_style = (
                opening_report.style.hide_index()
                .set_table_styles(ug_styles)
                .apply(highlight_spaces, axis=1)
            )
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
            draft_toupload_report = draft.parse(f"{lower}_summary", index_col=False)
            draft_style = draft_toupload_report.style.hide_index().set_table_styles(
                ug_styles
            )
            columns_to_format = draft_toupload_report.columns[3:]
            draft_html = round_columns(columns_to_format, draft_style)
            draft_attachment = draft_path
        else:
            draft_html = (
                "<p>Seems like nobody uploaded attachment during the above period</p>"
            )

        if os.path.exists(rejections_path):
            rejections = pd.ExcelFile(rejections_path)
            rejections_report = (
                rejections.parse(f"{lower}_summary", index_col=[0], header=[0, 1])
                .dropna(axis=0)
                .reset_index()
            )

            rejections_report = rejections_report.rename(
                columns={"index": "", "Unnamed: 1_level_0": "", "Week Range": ""},
                level=0,
            )

            rejections_report = rejections_report.rename(
                columns={
                    "": "Outlet",
                    "Unnamed: 1_level_1": "RM",
                    "Unnamed: 2_level_1": "SRM",
                },
                level=1,
            )

            rejections_style = rejections_report.style.hide_index().set_table_styles(
                ug_styles
            )
            columns_to_format = rejections_report.columns[3:]
            rejections_html = round_columns(columns_to_format, rejections_style)
            rejections_attachment = rejections_path

        else:
            rejections_html = (
                "<p>No insurance orders were rejected during the above period!</p>"
            )

        if os.path.exists(sops_path):
            sops = pd.ExcelFile(sops_path)
            sops_report = sops.parse(
                f"{lower}_summary", index_col=[0], header=[0, 1]
            ).dropna(axis=0)

            sops_report = sops_report.rename(
                columns={
                    "Unnamed: 1_level_1": "Outlet",
                    "Unnamed: 2_level_1": "RM",
                    "Unnamed: 3_level_1": "SRM",
                },
                level=1,
            )
            sops_report = sops_report.rename(
                columns={"Outlet": "", "RM": "", "SRM": ""}, level=0
            )
            sops_styles = sops_report.style.hide_index().set_table_styles(ug_styles)
            sops_html = sops_styles.to_html(doctype_html=True)
            sops_attachment = sops_path

        else:
            sops_html = "All the Branches were compliant for the above period."

        if os.path.exists(dectractors_path):
            detractors = pd.ExcelFile(dectractors_path)
            detractors_report = detractors.parse(
                f"{lower}_summary", index_col=False
            ).fillna(" ")
            detractors_style = (
                detractors_report.style.hide_index()
                .set_table_styles(ug_styles)
                .apply(highlight_spaces, axis=1)
            )
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
            opening_style = (
                opening_report.style.hide_index()
                .set_table_styles(ug_styles)
                .apply(highlight_spaces, axis=1)
            )
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
                f"{lower}_summary", index_col=[0], header=[0, 1]
            )

            draft_toupload_report = draft_toupload_report.drop(
                draft_toupload_report.index[0]
            )

            draft_toupload_report = draft_toupload_report.reset_index()
            draft_toupload_report = draft_toupload_report.rename(
                columns={"index": "", "Unnamed: 1_level_0": "", "Month": ""}, level=0
            )
            draft_toupload_report = draft_toupload_report.rename(
                columns={
                    "": "Outlet",
                    "Unnamed: 1_level_1": "RM",
                    "Unnamed: 2_level_1": "SRM",
                },
                level=1,
            )
            cols = draft_toupload_report.columns[3:]
            draft_style = draft_toupload_report.style.hide_index().set_table_styles(
                ug_styles
            )
            draft_html = round_columns(cols, draft_style)
            draft_attachment = draft_path
        else:
            draft_html = (
                "<p>Seems like nobody uploaded attachment during the above period</p>"
            )

        if os.path.exists(rejections_path):
            rejections = pd.ExcelFile(rejections_path)
            rejections_report = (
                rejections.parse(f"{lower}_summary", index_col=[0], header=[0, 1])
                .fillna(" ")
                .rename(
                    columns={
                        "Unnamed: 1_level_0": "",
                        "Unnamed: 2_level_0": "",
                        "Unnamed: 3_level_0": "",
                    },
                    level=0,
                )
            )

            rejections_style = rejections_report.style.hide_index().set_table_styles(
                ug_styles
            )
            rejections_html = rejections_style.to_html(doctype_html=True)
            rejections_attachment = rejections_path

        else:
            rejections_html = (
                "<p>No insurance orders were rejected during the above period!</p>"
            )

        if os.path.exists(sops_path):
            sops = pd.ExcelFile(sops_path)
            sops_report = (
                sops.parse(f"{lower}_summary", index_col=[0], header=[0, 1])
                .dropna(axis=0)
                .reset_index()
            )

            sops_report = sops_report.rename(
                columns={"Unnamed: 1_level_0": "", "Month": "", "index": ""}, level=0
            )
            sops_report = sops_report.rename(
                columns={
                    "": "Outlet",
                    "Unnamed: 1_level_1": "RM",
                    "Unnamed: 2_level_1": "SRM",
                },
                level=1,
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
                f"{lower}_summary", index_col=False
            ).fillna(" ")
            detractors_style = (
                detractors_report.style.hide_index()
                .set_table_styles(ug_styles)
                .apply(highlight_spaces, axis=1)
            )
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
            opening_style = (
                opening_report.style.hide_index()
                .set_table_styles(ug_styles)
                .apply(highlight_spaces, axis=1)
            )
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
        no_view_html=no_view_html,
    )

    html_content = quopri.encodestring(html.encode("utf-8")).decode("utf-8")
    email_message = MIMEMultipart("alternative")
    email_message["From"] = sender_email
    email_message["To"] = ",".join(receiver_email)
    email_message["Subject"] = subject

    html_part = MIMEText(html_content, "html")
    html_part.replace_header("Content-Transfer-Encoding", "quoted-printable")
    email_message.attach(html_part)

    if os.path.exists(draft_attachment):
        attach_file(email_message, draft_attachment, "draft_to_upload.xlsx")

    if os.path.exists(rejections_attachment):
        attach_file(email_message, rejections_attachment, "rejections_report.xlsx")

    if os.path.exists(sops_attachment):
        attach_file(email_message, sops_attachment, "sop_compliance.xlsx")

    if os.path.exists(plano_attachment):
        attach_file(
            email_message, plano_attachment, "Insurance Clients Not Submitted.xlsx"
        )

    if os.path.exists(detractors_attachment):
        attach_file(email_message, detractors_attachment, "detractors_report.xlsx")

    # if os.path.exists(no_view_attachment):
    #     attach_file(
    #         email_message,
    #         no_view_attachment,
    #         "Non_Converted_Non_Views.xlsx"
    #     )

    # if os.path.exists(mtd_insurance_attachment):
    #     attach_file(
    #         email_message,
    #         mtd_insurance_attachment,
    #         "MTD_Insurance_Conversion.xlsx"
    #     )

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, email_message.as_string())


req_columns = [
    "Code",
    "Plano RX",
    "Customer Code",
    "Insurance Company",
    "Opthom Name",
    "EWC Handover",
    "Who Viewed RX",
]

rej_cols = ["Order Number", "Order Creator", "Created User", "Remarks"]


def send_to_branches(branch_data, selection, path, filename, country):
    if selection != "Daily":
        return
    create_initial_file(filename)
    todate = get_yesterday_date(truth=False)
    branch_dat = branch_data.copy()
    branches = branch_data["Outlet"].to_list()

    if country == "Kenya":
        branches += ["DUNCAN", "EUNICE"]

    html = ""
    branch_data = branch_data.set_index("Outlet")
    selections_lower = selection.lower()
    rejections_path = f"{path}draft_upload/rejections_report.xlsx"
    planos_path = f"{path}draft_upload/planorx_not_submitted.xlsx"
    feedback = f"{path}draft_upload/insurance_daily.xlsx"
    no_feedbacks = f"{path}draft_upload/no_feedbacks.xlsx"
    eyetest_order = f"{path}draft_upload/et_to_order.xlsx"
    before_feedback_desk = f"{path}draft_upload/insurance_desk.xlsx"
    before_feedback_nodesk = f"{path}draft_upload/no_insurance_desk.xlsx"
    after_feedback_desk = f"{path}draft_upload/after_desk.xlsx"
    after_feedback_nodesk = f"{path}draft_upload/no_desk_after.xlsx"
    direct_insurance = f"{path}/draft_upload/direct_insurance.xlsx"
    approval_update = f"{path}draft_upload/approval_update.xlsx"

    file_paths = [
        rejections_path,
        planos_path,
        feedback,
        no_feedbacks,
        eyetest_order,
        before_feedback_desk,
        before_feedback_nodesk,
        after_feedback_desk,
        after_feedback_nodesk,
        direct_insurance,
        approval_update
    ]

    if any(os.path.exists(file_path) for file_path in file_paths):
        rejection_branches = []
        planos_branches = []
        feedback_branches = []
        no_feedback_branches = []
        et_order_branches = []
        bottom_branches = []
        before_feedback_desk_branches = []
        before_feedback_nodesk_branches = []
        after_feedback_desk_branches = []
        after_feedback_nodesk_branches = []
        direct_insurance_branches = []
        approval_update_branches = []

        dataframes = {}

        if os.path.exists(feedback):
            feedbacks = pd.ExcelFile(feedback)
            feedbacks_data = feedbacks.parse("daily_data", index_col=False)

            feedbacks_data = feedbacks_data[
                ~feedbacks_data["Current Status"].isin(
                    [
                        "SMART Forwarded to Approvals Team",
                        "Sales Order Created",
                        "Customer Confirmed Order",
                        "Sent Forms to Invoice Desk",
                    ]
                )
            ]

            feedback_branches = feedbacks_data["Outlet"].to_list()
            value = {"Approved Insurance Orders not Converted": feedbacks_data}
            dataframes.update(value)

            br = (
                feedbacks_data.groupby("Outlet")["Outlet"]
                .count()
                .nlargest(3)
                .index.tolist()
            )
            bottom_branches.extend(br)

        if os.path.exists(direct_insurance):
            direct_insurance = pd.ExcelFile(direct_insurance)
            direct_insurance_data = direct_insurance.parse("Data", index_col=False)

            direct_insurance_branches = direct_insurance_data["Outlet"].to_list()
            value = {
                "Direct Insurance Orders not Converted": pd.DataFrame(
                    direct_insurance_data
                )
            }
            dataframes.update(value)

            br = (
                direct_insurance_data.groupby("Outlet")["Outlet"]
                .count()
                .nlargest(3)
                .index.tolist()
            )
            bottom_branches.extend(br)

        if os.path.exists(rejections_path):
            rejections = pd.ExcelFile(rejections_path)
            rejections_data = rejections.parse(
                f"{selections_lower}_rejections_data", index_col=False
            )
            rejection_branches = rejections_data["Outlet"].to_list()
            value = {"Insurance Errors": rejections_data}
            dataframes.update(value)

            br = (
                rejections_data.groupby("Outlet")["Outlet"]
                .count()
                .nlargest(3)
                .index.tolist()
            )
            bottom_branches.extend(br)

        if os.path.exists(planos_path):
            planos = pd.ExcelFile(planos_path)
            planos_data = planos.parse(f"{selections_lower}_data", index_col=False)
            planos_data = planos_data[planos_data["Submission"] == "Not Submitted"]

            planos_data = planos_data.rename(columns={"Branch": "Outlet"})
            planos_data["Outlet"] = planos_data["Outlet"].replace("OHO", "DUNCAN")
            planos_data["Outlet"] = planos_data["Outlet"].replace("YOR", "EUNICE")
            planos_branches = planos_data["Outlet"].to_list()
            value = {"Insurance Clients not Submitted": planos_data}
            dataframes.update(value)

            br = (
                planos_data.groupby("Outlet")["Outlet"]
                .count()
                .nlargest(3)
                .index.tolist()
            )
            bottom_branches.extend(br)

        if os.path.exists(no_feedbacks):
            non_feedback = pd.ExcelFile(no_feedbacks)
            no_feedbacks_data = non_feedback.parse("no_feedback", index_col=False)
            no_feedback_branches = no_feedbacks_data["Outlet"].to_list()
            value = {"Insurance Orders With No Feedback": no_feedbacks_data}
            dataframes.update(value)

            br = (
                no_feedbacks_data.groupby("Outlet")["Outlet"]
                .count()
                .nlargest(3)
                .index.tolist()
            )
            bottom_branches.extend(br)

        if os.path.exists(eyetest_order):
            eyetest_order = pd.ExcelFile(eyetest_order)
            eyetest_order_data = eyetest_order.parse("Data", index_col=False)
            eyetest_order_data = eyetest_order_data.rename(
                columns={"et_branch": "Outlet"}
            )
            eyetest_order_data["Outlet"] = eyetest_order_data["Outlet"].replace(
                "OHO", "DUNCAN"
            )
            eyetest_order_data["Outlet"] = eyetest_order_data["Outlet"].replace(
                "YOR", "EUNICE"
            )
            et_order_branches = eyetest_order_data["Outlet"].to_list()
            value = {
                "Time from Eye Test Completed to Draft Order Created": pd.DataFrame(
                    eyetest_order_data
                )
            }
            dataframes.update(value)

            br = (
                eyetest_order_data.groupby("Outlet")["Outlet"]
                .count()
                .nlargest(3)
                .index.tolist()
            )
            bottom_branches.extend(br)

        if os.path.exists(before_feedback_desk):
            before_feedback_desk = pd.ExcelFile(before_feedback_desk)
            before_feedback_desk_data = before_feedback_desk.parse(
                "Data", index_col=False
            )

            before_feedback_desk_branches = before_feedback_desk_data[
                "Outlet"
            ].to_list()
            value = {
                "Delayed Orders from Draft to Upload": pd.DataFrame(
                    before_feedback_desk_data
                )
            }
            dataframes.update(value)

            br = (
                before_feedback_desk_data.groupby("Outlet")["Outlet"]
                .count()
                .nlargest(3)
                .index.tolist()
            )
            bottom_branches.extend(br)

        if os.path.exists(before_feedback_nodesk):
            before_feedback_nodesk = pd.ExcelFile(before_feedback_nodesk)
            before_feedback_nodesk_data = before_feedback_nodesk.parse(
                "Data", index_col=False
            )

            before_feedback_nodesk_branches = before_feedback_nodesk_data[
                "Outlet"
            ].to_list()
            value = {
                "Delayed Orders from Upload Attachment to Sent - Preauth": pd.DataFrame(
                    before_feedback_nodesk_data
                )
            }
            dataframes.update(value)

            br = (
                before_feedback_nodesk_data.groupby("Outlet")["Outlet"]
                .count()
                .nlargest(3)
                .index.tolist()
            )
            bottom_branches.extend(br)

        if os.path.exists(after_feedback_desk):
            after_feedback_desk = pd.ExcelFile(after_feedback_desk)
            after_feedback_desk_data = after_feedback_desk.parse(
                "Data", index_col=False
            )

            after_feedback_desk_branches = after_feedback_desk_data["Outlet"].to_list()
            value = {
                "Delayed Orders after Insurance Feedback": pd.DataFrame(
                    after_feedback_desk_data
                )
            }
            dataframes.update(value)

            br = (
                after_feedback_desk_data.groupby("Outlet")["Outlet"]
                .count()
                .nlargest(3)
                .index.tolist()
            )
            bottom_branches.extend(br)

        if os.path.exists(after_feedback_nodesk):
            after_feedback_nodesk = pd.ExcelFile(after_feedback_nodesk)
            after_feedback_nodesk_data = after_feedback_desk.parse(
                "Data", index_col=False
            )

            after_feedback_nodesk_branches = after_feedback_nodesk_data[
                "Outlet"
            ].to_list()
            value = {
                "Delayed Orders after Insurance Feedback": pd.DataFrame(
                    after_feedback_nodesk_data
                )
            }
            dataframes.update(value)

            br = (
                after_feedback_nodesk_data.groupby("Outlet")["Outlet"]
                .count()
                .nlargest(3)
                .index.tolist()
            )
            bottom_branches.extend(br)

        if os.path.exists(approval_update):
            sap_update = pd.ExcelFile(approval_update)
            sap_update_data = sap_update.parse("Data", index_col=False)
            no_feedback_branches = sap_update_data["Outlet"].to_list()
            value = {"Delayed Orders from Received approval from the insurance company to Update on SAP": sap_update_data}
            dataframes.update(value)

            br = (
                sap_update_data.groupby("Outlet")["Outlet"]
                .count()
                .nlargest(3)
                .index.tolist()
            )
            bottom_branches.extend(br)

        all_branches = (
            planos_branches
            + rejection_branches
            + feedback_branches
            + no_feedback_branches
            + et_order_branches
            + before_feedback_desk_branches
            + before_feedback_nodesk_branches
            + after_feedback_desk_branches
            + after_feedback_nodesk_branches
            + direct_insurance_branches
        )
        # random_branch = random.choice(all_branches)
        bt_branches = list(set(bottom_branches))
        final_bt_list = []

        if country == "Kenya":
            final_bt_list = random.sample(bt_branches, 2)

        email_subjects = {
            "Date": [],
            "Branch": [],
            "Email": [],
            "Subject": [],
            "Retail Analyst": [],
            "Status": [],
        }

        for branch in branches:
            if branch == "DUNCAN":
                branch_email = "duncan.muchai@optica.africa"
                branch_manager = branch_data.loc["OHO", "Branch Manager"].split(" ")[0]
                retail_analyst = branch_data.loc["OHO", "Retail Analyst"]
                srm_email = branch_data.loc["OHO", "Escalation Email"]
                rm_email = branch_data.loc["OHO", "RM Email"]

            elif branch == "EUNICE":
                branch_email == "yh.manager@optica.africa"
                branch_manager = branch_data.loc["YOR", "Branch Manager"].split(" ")[0]
                retail_analyst = branch_data.loc["YOR", "Retail Analyst"]
                srm_email = branch_data.loc["OHO", "Escalation Email"]
                rm_email = branch_data.loc["YOR", "RM Email"]

            else:
                branch_email = branch_data.loc[branch, "Email"]
                branch_manager = branch_data.loc[branch, "Branch Manager"].split(" ")[0]
                retail_analyst = branch_data.loc[branch, "Retail Analyst"]
                srm_email = branch_data.loc[branch, "Escalation Email"]
                rm_email = branch_data.loc[branch, "RM Email"]

            if branch == "OHO" or branch == "YOR":
                branch_manager = "Insurance Desk"
            
            if branch == "DUNCAN":
                branch_manager = "Paulette"

            html, subject = generate_html_and_subject(
                branch=branch,
                branch_manager=branch_manager.capitalize(),
                dataframe_dict=dataframes,
                styles=styles_daily,
                date=todate,
            )

            if html is None and subject is None:
                continue

            if branch in final_bt_list and country == "Kenya" and branch != "OHO":
                receiver_email = [
                    "kush@optica.africa",
                    "wazeem@optica.africa",
                    "andrew@optica.africa",
                    srm_email,
                    branch_email,
                    "wairimu@optica.africa",
                    "douglas.kathurima@optica.africa",
                ]

            elif branch == "YOR":
                receiver_email = [
                    "insurance@optica.africa",
                    "yh.manager@optica.africa",
                    "kimstone@optica.africa",
                    srm_email,
                ]

            elif branch == "OHO":
                receiver_email = [
                    "insuranceoh@optica.africa",
                    "duncan.muchai@optica.africa",
                    "susan@optica.africa",
                    "kimstone@optica.africa",
                    srm_email,
                ]

            elif branch == "DUNCAN":
                subject = subject.replace("DUNCAN", "Optica House")
                receiver_email = [
                    "kimstone@optica.africa",
                    "insuranceoh@optica.africa",
                    "duncan.muchai@optica.africa",
                    "susan@optica.africa",
                    srm_email,
                ]

            elif branch == "EUNICE":
                subject = subject.replace("EUNICE", "York House")

                receiver_email = [
                    "kimstone@optica.africa",
                    "yh.manager@optica.africa",
                    srm_email,
                ]

            elif country == "Uganda":
                receiver_email = [
                    "kush@optica.africa",
                    "raghav@optica.africa",
                    "larry.larsen@optica.africa",
                    "wairimu@optica.africa",
                    branch_email,
                ]

            elif country == "Rwanda":
                receiver_email = [
                    "kush@optica.africa",
                    "raghav@optica.africa",
                    "nilesh@optica.africa",
                    "wairimu@optica.africa",
                    branch_email,
                ]

            else:
                receiver_email = [branch_email, srm_email]

            # if branch in [
            #     "HUB","MSA","NYA",
            #     "ELD","JUN","SIG",
            #     "TMA","TRM","TWO",
            #     "COR","DON","GAT",
            #     "INM","OHO","YOR",
            #     "CAP","EBK","IMA",
            #     "KIS","NAK","NWE"
            # ]:

            receiver_email += ["christopher@optica.africa"]

            if country == "Test":
                receiver_email = ["tstbranch@gmail.com"]

            if country == "Kenya":
                sender_email, password = return_evn_credentials(retail_analyst.lower())
                if not (sender_email is not None and password is not None):
                    sender_email, password = return_evn_credentials("douglas")

            elif country == "Uganda":
                sender_email, password = return_evn_credentials(retail_analyst.lower())
                if not (sender_email is not None and password is not None):
                    sender_email, password = return_evn_credentials("douglas")

            elif country == "Rwanda":
                sender_email, password = return_evn_credentials(retail_analyst.lower())
                if not (sender_email is not None and password is not None):
                    sender_email, password = return_evn_credentials("douglas")
            else:
                sender_email, password = return_evn_credentials("douglas")

            html_content = quopri.encodestring(html.encode("utf-8")).decode("utf-8")

            email_message = MIMEMultipart("alternative")
            email_message["From"] = sender_email
            email_message["To"] = ",".join(receiver_email)
            email_message["Subject"] = subject

            html_part = MIMEText(html_content, "html")
            html_part.replace_header("Content-Transfer-Encoding", "quoted-printable")
            email_message.attach(html_part)

            if branch_email not in return_sent_emails(filename):
                context = ssl.create_default_context()
                with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                    server.login(sender_email, password)
                    server.sendmail(
                        sender_email, receiver_email, email_message.as_string()
                    )
                    record_sent_branch(branch_email, filename)



                    email_subjects["Branch"].append(branch)
                    if branch == "OHO":
                        email_subjects["Email"].append("insuranceoh@optica.africa")
                    elif branch == "DUNCAN":
                        email_subjects["Email"].append("duncan.muchai@optica.africa")
                    else:
                        email_subjects["Email"].append(branch_email)
                    
                    email_subjects["Date"].append(todate)
                    email_subjects["Status"].append("Open")
                    email_subjects["Subject"].append(subject)
                    email_subjects["Retail Analyst"].append(retail_analyst.capitalize())
            else:
                continue

        if country == "Kenya":
            email_dataframe = pd.DataFrame(email_subjects)
            gc = pygsheets.authorize(
                service_file=r"/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json"
            )
            sh = gc.open_by_key("1m-CCmDtvnqGv6FWuZuzQLLdG5jxLaXoMH6VStZyd32s")
            worksheet = sh.worksheet_by_title("Insurance")
            existing_data = pd.DataFrame(worksheet.get_all_records())

            new_data = pd.concat([existing_data, email_dataframe], ignore_index=True)
            new_data = new_data.fillna("")
            new_data = new_data.drop_duplicates(subset=["Subject"], keep="first")
            worksheet.set_dataframe(new_data, start="A1")


def clean_folders(path):
    clean_folder(dir_name=f"{path}draft_upload/")


if __name__ == "__main__":
    send_draft_upload_report()
    clean_folders()
    send_to_branches()


"""
Please DO NOT Remove the above lines at any point not matter what.
Removing the above lines will cause the email to send endless times.

"""
