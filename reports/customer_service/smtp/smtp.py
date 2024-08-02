import pandas as pd
import ssl
import smtplib
import os
from reports.customer_service.html.html import html
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


"""
While everyone can write code, the real challenge lies in writing code that is modular. 
Code that won't give another developer a migraine when they try to understand it.
That's what I aim for and what I anticipate: clean, organized, and modular code.
And one more thing: remember the DRY principle (Don't Repeat Yourself).

-- Douglas Kathurima - Data Anaylst
"""


def send_customer_service_report(path) -> None:
    sender_email = os.getenv("douglas_email")
    sender_password = os.getenv("douglas_password")

    receiver_email = []
    email_subject = ""

    insurance_conversion = pd.ExcelFile(f"{path}customer_service/insurance_conversion.xlsx")
    online_orders = pd.ExcelFile(f"{path}customer_service/online_orders.xlsx")
    home_deliveries = pd.ExcelFile(f"{path}customer_service/home_deliveries.xlsx")
    online_appointments = pd.ExcelFile(f"{path}customer_service/online_appointments.xlsx")
    overseas_delays = pd.ExcelFile(f"{path}customer_service/overseas_delays.xlsx")
    internal_delays = pd.ExcelFile(f"{path}customer_service/internal_delays.xlsx")

    """
    SECTION I:
    Insurance Conversion
    """

    """
    SECTION II:
    Online Orders
    """

    """
    SECTION III:
    Home Deliveries
    """

    """
    SECTION IV:
    Online Customer Service Appointments
    """

    """
    SECTION V:
    Overseas Order Delays
    """

    """
    SECTION VI
    Internal Order Delays
    """

    email_message = MIMEMultipart("alternative")
    email_message["From"] = sender_email
    email_message["To"] = r",".join(receiver_email)
    email_message["subject"] = email_subject
    email_message.attach(MIMEText(html))

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context) as server:
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, receiver_email, email_message.as_string())
