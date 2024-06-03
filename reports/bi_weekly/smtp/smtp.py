import pandas as pd
from airflow.models import variable
from sub_tasks.libraries.styles import bi_weekly
from reports.bi_weekly.html.html import branch_html
from reports.bi_weekly.utils.utils import html_style_dataframe, highlight_threshold, highlight_first_row, parse_data, return_branch_data, write_to_excel
from email.mime.image import MIMEImage
import quopri
import os
import ssl
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import matplotlib.pyplot as plt
from reports.bi_weekly.utils.utils import second_range_start, second_range_end
from reports.bi_weekly.utils.utils import metrics_missed


def send_to_management() -> None:
    pass


sender_email = os.getenv("douglas_email")
password = os.getenv("douglas_password")

receiver_email = [
    "kush@optica.africa", 
    "wazeem@optica.africa", 
    "yuri@optica.africa", 
    "wairimu@optica.africa", 
    "ian.gathumbi@optica.africa"
]


def send_to_branch(path, branch_data) -> None:
    report = pd.read_excel(f"{path}bi_weekly_report/report.xlsx")
    raw_conversion_data = pd.read_excel(f"{path}bi_weekly_report/conversion_trend.xlsx")

    raw_data_path = f"{path}bi_weekly_report/raw_data.xlsx"
    raw_data_excel = pd.ExcelFile(raw_data_path)

    

    opening_time = parse_data(raw_data_excel,"Branch Opening Late")
    et_order = parse_data(raw_data_excel,"Eye Test to Order Delays")
    eyetests_not_converted = parse_data(raw_data_excel, "Eye Tests not Converted")
    identifier_delays = parse_data(raw_data_excel,"Printing Identifier Delays")
    passives_comments = parse_data(raw_data_excel,"Passive Comments")
    poor_google_reviews = parse_data(raw_data_excel, "Poor Google Reviews")
    sops_not_complied = parse_data(raw_data_excel,"SOP not Complied")
    frame_only_orders = parse_data(raw_data_excel,"Frame only Orders")
    feedacks_non_conversions = parse_data(raw_data_excel,"Insurance Feedbacks")



    comparison_data = pd.read_excel(
        f"{path}bi_weekly_report/comparison_report.xlsx",
        header=[0, 1],
        index_col=[0, 1, 2, 3, 4, 5],
    )

    for branch in branch_data["Outlet"].to_list():

        opening_time_branch = return_branch_data(opening_time, branch)
        et_order_branch = return_branch_data(et_order, branch)
        eyetests_not_converted_branch = return_branch_data(eyetests_not_converted, branch)
        identifier_delays_branch = return_branch_data(identifier_delays, branch)
        passives_comments_branch = return_branch_data(passives_comments, branch)
        poor_google_reviews_branch = return_branch_data(poor_google_reviews, branch)
        sops_not_complied_branch = return_branch_data(sops_not_complied, branch)
        frame_only_orders_branch = return_branch_data(frame_only_orders, branch)
        feedacks_non_conversions_branch = return_branch_data(feedacks_non_conversions, branch)

        conversion_trend_data = raw_conversion_data[
            raw_conversion_data["Branch"] == branch
        ]

        conversion_data = conversion_trend_data["Overall Conversion"].to_list()

        plt.figure(figsize=(10, 2))
        plt.plot(conversion_data, marker="o", linestyle="-")

        for i, value in enumerate(conversion_data):
            plt.text(i, value, str(value), ha="center", va="bottom", fontsize=12)

        plt.ylim(20, 105)
        plt.axis("off")
        plt.savefig(f"{path}bi_weekly_report/conversion_trend.png", bbox_inches="tight")
        plt.close()

        conversion_trend = f"{path}bi_weekly_report/conversion_trend.png"

        branch_attachment = report.query("Branch == @branch")

        comparison = (
            comparison_data.loc[
                comparison_data.index.get_level_values("Branch") == str(branch)
            ]
            .reset_index()
            .drop(columns=[("Payroll Number"), ("RM"), ("SRM")], level=0)
            .set_index([("Branch"), ("Staff Name"), ("Designation")])
        )

        branch_attachment_sorted = branch_attachment.sort_values(
            by="Payroll Number", key=lambda x: x != "OVERALL"
        )
        branch_comparison = comparison  # .sort_values(by='Payroll Number', key=lambda x: x != 'OVERALL')

        branch_performance = pd.DataFrame(
            report.query("`Payroll Number` == 'OVERALL' and Branch == @branch")
        )

        branch_performance = branch_performance[
            [
                "Branch",
                "Times Opened Late(Thresh = 0)",
                "SOPs/ Customers",
                "NPS Score(Target = 90)",
                "Google Reviews Average Rating",
            ]
        ]

        branch_performance["Google Reviews Average Rating"] = (
            branch_performance["Google Reviews Average Rating"].astype(str) + "g"
        )

        optom_performance = report.query(
            "Branch == @branch and (Designation == 'Optom' or Designation.isnull())"
        )

        optom_performance = optom_performance[
            [
                "Branch",
                "Staff Name",
                "Optom Low RX Conversion (Target = 65)",
                "Viewed Eyetest Older than 30 Days Conversion",
                "EWC Overall Conversion (Target = 75)",
                "Average Eye Test Time",
            ]
        ]

        optom_performance["Staff Name"] = optom_performance["Staff Name"].fillna(
            "OVERALL"
        )
        optom_performance = (
            optom_performance.set_index(["Branch", "Staff Name"])
            .dropna(axis=0, how="all")
            .reset_index()
        ).sort_values(by="Staff Name", key=lambda x: x != "OVERALL")

        salesperson_performance = report.query(
            "Branch == @branch and (Designation == 'Sales Person' or Designation.isnull())"
        )
        salesperson_performance = salesperson_performance[
            [
                "Branch",
                "Staff Name",
                "EWC Low RX Conversion (Target = 65)",
                "Viewed Eyetest Older than 30 Days Conversion",
                "Eye Test to Order Efficiency (Target = 90% in 45 minutes)",
                "Printing Identifier Efficiency (Target = 5 Mins)"
            ]
        ]

        salesperson_performance["Staff Name"] = salesperson_performance[
            "Staff Name"
        ].fillna("OVERALL")
        salesperson_performance = (
            salesperson_performance.set_index(["Branch", "Staff Name"])
            .dropna(axis=0, how="all")
            .reset_index()
        ).sort_values(by="Staff Name", key=lambda x: x != "OVERALL")

        insurance_performance = report.query(
            "Branch == @branch and (Designation == 'Sales Person' or Designation.isnull())"
        )
        insurance_performance["Staff Name"] = insurance_performance[
            "Staff Name"
        ].fillna("OVERALL")

        insurance_performance = insurance_performance[
            [
                "Branch",
                "Staff Name",
                "Use Available Amount Conversion",
                "Declined Conversion",
                "Approval Received from Insurance to Update Approval on SAP (Target = 90% in 5 Minutes)",
                "Insurance Feedback to Customer Contacted time taken (Target = 90% in 60 Minutes)",
            ]
        ]

        insurance_performance = (
            insurance_performance.set_index(["Branch", "Staff Name"])
            .dropna(axis=0, how="all")
            .reset_index()
        ).sort_values(by="Staff Name", key=lambda x: x != "OVERALL")

        branch_per_html = html_style_dataframe(branch_performance)
        optom_html = html_style_dataframe(optom_performance)
        salesperson_html = html_style_dataframe(salesperson_performance)
        insurance_html = html_style_dataframe(insurance_performance)

        if branch not in ["COR"]:
            continue

        html = branch_html.format(
            branch_name=branch,
            branch_performance_html=branch_per_html,
            optom_performance_html=optom_html,
            salesperson_performance_html=salesperson_html,
            insurance_performance_html=insurance_html,
        )

        subject = f"{branch} Bi-Weekly KPIs Report for the Period {pd.to_datetime(second_range_start).strftime('%d-%b-%y')} to {pd.to_datetime(second_range_end).strftime('%d-%b-%y')}"


    
        branch_attachment_style = branch_attachment_sorted.style.hide_index().apply(highlight_first_row, axis = 1).apply(
            highlight_threshold, axis=1
        )

        branch_comparison = branch_comparison.style.hide_index()

        from openpyxl import load_workbook

        file_name = f"{path}bi_weekly_report/{branch} bi_weekly report.xlsx"
        with pd.ExcelWriter(file_name, engine="xlsxwriter") as writer:

            border_format = writer.book.add_format({"border": 1, "font_size": 10})
            header_format = writer.book.add_format(
                {
                    "bold": True,
                    "text_wrap": True,
                    "align": "center",
                    "valign": "vcenter",
                    "fg_color": "#DCE6F1", 
                }
            )

            branch_attachment_style.to_excel(
                writer, index=False, sheet_name="Branch Summary"
            )
            worksheet = writer.sheets["Branch Summary"]
            worksheet.set_column(0, len(branch_attachment_style.columns) - 1, 15)

            for col_num, value in enumerate(branch_attachment_style.columns.values):
                worksheet.write(0, col_num, value, header_format)

            worksheet.conditional_format(
                0,
                0,
                branch_attachment_style.data.shape[0],
                branch_attachment_style.data.shape[1],
                {"type": "no_errors", "format": border_format},
            )
            worksheet.freeze_panes(1, 3)

            branch_comparison.to_excel(writer, sheet_name="Comparison")
            worksheet = writer.sheets["Comparison"]
            worksheet.set_column("C:C", 30)
            worksheet.set_column("D:D", 12)
            worksheet.set_row(0, 50)

            worksheet.conditional_format(
                0,
                0,
                branch_comparison.data.shape[0],
                branch_comparison.data.shape[1],
                {"type": "no_errors", "format": border_format},
            )
            worksheet.freeze_panes(2, 4)

            dataframes = {
                "Branch Opening Late": opening_time_branch,
                "Eye Test to Order Delays": et_order_branch,
                "Eye Tests not Converted": eyetests_not_converted_branch,
                "Printing Identifier Delays": identifier_delays_branch,
                "Passive Comments": passives_comments_branch,
                "Poor Google Reviews": poor_google_reviews_branch,
                "SOPs not Complied": sops_not_complied_branch,
                "Frame only Orders": frame_only_orders_branch,
                "Insurance Feedbacks": feedacks_non_conversions_branch
            }

            write_to_excel(dataframes, writer)

        wb = load_workbook(file_name)
        wb.save(file_name)

        html_content = quopri.encodestring(html.encode("utf-8")).decode("utf-8")
        email_message = MIMEMultipart("alternative")
        email_message["From"] = sender_email
        email_message["To"] = ",".join(receiver_email)
        email_message["Subject"] = subject

        html_part = MIMEText(html_content, "html")
        html_part.replace_header("Content-Transfer-Encoding", "quoted-printable")
        email_message.attach(html_part)

        with open(conversion_trend, "rb") as f:
            closed_tickets = f.read()

        conversion_trend_image = MIMEImage(closed_tickets)
        conversion_trend_image.add_header("Content-ID", "<conversion_trend_image>")
        email_message.attach(conversion_trend_image)

        def attach_file(email_message, filename, name):
            with open(filename, "rb") as f:
                file_attachment = MIMEApplication(f.read())

            file_attachment.add_header(
                "Content-Disposition", f"attachment; filename= {name}"
            )

            email_message.attach(file_attachment)

        attach_file(email_message, file_name, f"{branch} bi_weekly report.xlsx")

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, email_message.as_string())
            return


def send_to_regional_managers(path, branch_data) -> None:
    pass
