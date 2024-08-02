from airflow.models import variable
import pandas as pd
import os
import os
import re
import pandas as pd
from sub_tasks.data.connect import pg_execute 
from email.policy import default
from pangres import upsert
import imaplib
import email
import imaplib
import email
from datetime import datetime
from email.mime.text import MIMEText
import smtplib
from sub_tasks.libraries.utils import path
from sub_tasks.libraries.utils import createe_engine
import os
from sub_tasks.libraries.utils import clean_folder
from sub_tasks.libraries.utils import createe_engine

"""
Who loves manual, repetitive tasks? Definitely not us! 
That's why we've created a nifty script to handle your email chores with flair. Here's what our script does:
Login Like a Pro: It starts by logging into your email account—no more manual sign-ins needed!

Find the Needle in the Haystack: Our script hunts down an email with a specific ID that you've saved. 
It's like having a superpower that lets you find exactly what you need in your inbox.

Attachment Detective: It then checks if there's an attachment in the email. 
If an attachment is found, the script checks whether it was shared today. Because who wants yesterday's news?

The Email Butler: If today's attachment is missing, the script drafts a polite email requesting the latest MPESA statement. 
Think of it as your digital assistant making sure you always have what you need.

No Attachment? No Problem: If today's email is there but without an attachment, 
the script sends out a friendly reminder asking for the attachment to be shared. 
It's like having a little helper that makes sure nothing falls through the cracks.
With this script, you can sit back and relax while it handles the mundane tasks. 
No more repetitive work—just smooth, automated email management!
"""

IMAP_SERVER = "imap.gmail.com"
SMTP_SERVER = "smtp.gmail.com"
EMAIL_ACCOUNT = "douglas.kathurima@optica.africa"
PASSWORD = "Ilovedouglas!"
FILE_PATH = f"{path}mpesa_statements"
EMAIL_SUBJECT = "MPESA STATEMENTS"

def login_to_email():
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL_ACCOUNT, PASSWORD)
    return mail

def download_attachment(mail, email_id):
    return
    result, data = mail.fetch(email_id, '(RFC822)')
    raw_email = data[0][1]
    email_message = email.message_from_bytes(raw_email, policy=default)

    attachment_downloaded = False
    downloaded_filenames = set() 

    for part in email_message.iter_parts():
        content_disposition = part.get('Content-Disposition', None)
        if content_disposition and 'attachment' in content_disposition:
            filename = part.get_filename()

            if filename and filename not in downloaded_filenames:
                if not os.path.exists(FILE_PATH):
                    os.makedirs(FILE_PATH)
                filepath = os.path.join(FILE_PATH, filename)
                with open(filepath, "wb") as f:
                    f.write(part.get_payload(decode=True))
                print(f"Downloaded attachment: {filename}")
                attachment_downloaded = True
                downloaded_filenames.add(filename)
            else:
                print(f"Skipped downloading duplicate attachment: {filename}")

    return attachment_downloaded, email_message


def send_reply(email_message, subject, message):
    return
    engine = createe_engine()
    msg = MIMEText(message)
    msg["Subject"] = subject
    msg["From"] = EMAIL_ACCOUNT

    msg["To"] = email_message["From"]
    msg["In-Reply-To"] = email_message["Message-ID"]
    msg["References"] = email_message["Message-ID"]

    with smtplib.SMTP(SMTP_SERVER, 587) as server:
        server.starttls()
        server.login(EMAIL_ACCOUNT, PASSWORD)
        server.sendmail(EMAIL_ACCOUNT, email_message["From"], msg.as_string())


def send_updated_reply(subject, message):
    engine = createe_engine()
    query = "SELECT message_id, message_from FROM reports_tables.email_message"
    message_df = pd.read_sql_query(query, con=engine)
    email_from = message_df['message_from'][0]
    message_id = message_df['message_id'][0]

    msg = MIMEText(message)
    msg["Subject"] = subject
    msg["From"] = EMAIL_ACCOUNT
    msg["To"] = email_from
    msg["In-Reply-To"] = message_id
    msg["References"] =  message_id
  
    with smtplib.SMTP(SMTP_SERVER, 587) as server:
        server.starttls()
        server.login(EMAIL_ACCOUNT, PASSWORD)
        server.sendmail(EMAIL_ACCOUNT, email_from, msg.as_string())

def get_email_date(email_message):
    email_date = email.utils.parsedate_to_datetime(email_message["Date"])
    return email_date.date()

def search_emails_by_subject(mail, subject):
    mail.select("inbox")
    result, data = mail.search(None, f'(SUBJECT "{subject}")')
    email_ids = data[0].split()
    return email_ids

def download_transactions():
    return
    engine = createe_engine()
    mail = login_to_email()
    email_ids = search_emails_by_subject(mail, EMAIL_SUBJECT)
    today = datetime.today().date()

    if email_ids:
        found_attachment = False
        latest_email_message = None
        latest_email_with_attachment = None

        for email_id in reversed(email_ids):
            result, data = mail.fetch(email_id, "(RFC822)")
            raw_email = data[0][1]
            email_message = email.message_from_bytes(raw_email, policy=default)

            email_date = get_email_date(email_message)

            if email_date == today:
                attachment_downloaded, _ = download_attachment(mail, email_id)

                # Update the latest email with an attachment
                if attachment_downloaded:
                    latest_email_with_attachment = email_message
                    found_attachment = True
                else:
                    # Reply indicating no attachment found
                    send_reply(
                        email_message,
                        f"{EMAIL_SUBJECT}",
                        "Hi DSR, There is no attachment in your email. Please resend it with the attachment."
                    )

        if found_attachment and latest_email_with_attachment:
            # Record the latest email with an attachment in the database
            data = pd.DataFrame({
                "message_id": latest_email_with_attachment["Message-ID"],
                "message_from": latest_email_with_attachment["From"]
            }, index=[0])

            upsert(
                engine=engine,
                df=data.set_index("message_id"),
                schema="reports_tables",
                table_name="email_message",
                if_row_exists="update",
                create_table=True,
            )
            print("Attachment(s) found and downloaded.")
        else:
            # Send reminder if no attachments were found in any email
            send_reply(
                None, f"{EMAIL_SUBJECT}",
                "Hi DSR, A polite reminder to please share the MPESA statement for today."
            )

    # else:
    #     # send_reply(
    #     #     None, f"{EMAIL_SUBJECT}",
    #     #     "Hi DSR, A polite reminder to please share the MPESA statement for today."
    #     # )

    # mail.logout()


def upsert_to_source_mpesa_transactions():
    return
    if not os.listdir(FILE_PATH):
        return

    branch_data_query = """
    select mpesa_name as branch_name, branch_code 
    from reports_tables.branch_data bd 
    """

    engine = createe_engine()
    branch_data = pd.read_sql_query(branch_data_query, con=engine)
    dataframes = []
    pattern = re.compile(r'^Mpesa_\d+_\d+$') 

    for filename in os.listdir(FILE_PATH):
        if filename.endswith(".xlsx"):
            file_path = os.path.join(FILE_PATH, filename)
            with pd.ExcelFile(file_path) as excel_file:
                target_sheets = [
                    sheet for sheet in excel_file.sheet_names if pattern.match(sheet)
                ]
                if target_sheets:
                    transactions = pd.read_excel(file_path, sheet_name=target_sheets[0])
                    transactions = transactions[
                        (
                            transactions["TRANSACTION_TYPE"]
                            == "Customer Merchant Payment"
                        )
                        & (
                            transactions["TRANSACTION_PARTY_DETAILS"]
                            != "Pay merchant Charge"
                        )
                    ]
                    transactions_rename = transactions.copy()
                    transactions_rename = transactions_rename.rename(
                        columns={
                            "START_TIMESTAMP": "mpesa_date",
                            "RECEIPT_NUMBER": "transaction_code",
                            "STORE_NAME": "branch_name",
                            "CREDIT_AMOUNT": "mpesa_amount",
                            "TRANSACTION_PARTY_DETAILS": "customer_name",
                        }
                    )[
                        [
                            "mpesa_date",
                            "transaction_code",
                            "branch_name",
                            "mpesa_amount",
                            "customer_name",
                        ]
                    ]
                    transactions_rename["customer_name"] = (
                        transactions_rename["customer_name"].str.split("-").str[1]
                    )

                    dataframes.append(transactions_rename)

    concatenated_df = pd.concat(dataframes, ignore_index=True)
    concatenated_df = concatenated_df[concatenated_df["branch_name"] != "Optica Ltd"]
    concatenated_df = pd.merge(
        concatenated_df, branch_data, on="branch_name", how="left"
    )

    upsert_data = concatenated_df[
        [
            "transaction_code",
            "mpesa_date",
            "branch_name",
            "branch_code",
            "customer_name",
            "mpesa_amount",
        ]
    ]

    upsert_data["mpesa_amount"] = upsert_data["mpesa_amount"].astype(int)

    upsert(
        engine=engine,
        df=upsert_data.set_index("transaction_code").head(5),
        schema="mabawa_staging",
        table_name="source_mpesa_transactions",
        if_row_exists="update",
        create_table=True,
    )

    send_updated_reply(
        f"{EMAIL_SUBJECT}", 
        "Hi, This has been updated.", 
    )


def cleann_folder() -> None:
    clean_folder(dir_name=FILE_PATH)



def truncate_email_message() -> None:
    query = "truncate reports_tables.email_message"
    pg_execute(query)

    

if __name__ == "__main__":
    pass
    download_transactions()
    upsert_to_source_mpesa_transactions()
    cleann_folder()



