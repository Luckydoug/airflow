import pandas as pd
import numpy as np
import imaplib
import email
import os
import datetime
import requests
from bs4 import BeautifulSoup
import urllib3
from dotenv import load_dotenv

def download_file_from_email(email_id, mail):
    _, email_data = mail.fetch(email_id, '(RFC822)')
    email_message = email.message_from_bytes(email_data[0][1])

    soup = BeautifulSoup(email_message.get_payload(), 'html.parser')
    links = soup.find_all('a')
    if len(links) >= 2:
        link = links[0]['href']
    else:
        print("No link found in the email body.")
        return None

    response = requests.get(link, verify = False)
    file_name = link.split('/')[-1]
    with open(r"/home/opticabi/Documents/optica_reports/{}".format(file_name = file_name), 'wb') as f:
        f.write(response.content)
    return os.path.abspath(file_name)


def get_download_data():
    load_dotenv()
    global file_paths
    user = "douglas.kathurima@optica.africa"
    password = "kathurima1999"
    print(password)
    file_paths = []
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    imap_url = 'imap.gmail.com'

    mail = imaplib.IMAP4_SSL(imap_url)

    mail.login(user, password)

    mail.select('inbox')
    current_date = datetime.datetime.now()
    if current_date.weekday() == 0:  # 0 means Monday
        days_back = datetime.timedelta(days=2)
    else:
        days_back = datetime.timedelta(days=1)

    report_day = current_date

    date_since = report_day.strftime('%d-%b-%Y')

    search_string1 = 'FROM "noreply@3cx.net" SUBJECT "Your 3CX Report CS Outbound Calls Daily Audit is ready" SENTSINCE ' + date_since
    search_string2 = 'FROM "noreply@3cx.net" SUBJECT "Your 3CX Report CS Inbound Calls Daily Audit is ready" SENTSINCE ' + date_since
    results1, data1 = mail.search(None, search_string1)
    results2, data2 = mail.search(None, search_string2)
    email_ids1 = data1[0].split()
    email_ids2 = data2[0].split()

    ids1 = email_ids1[-1]
    ids2 = email_ids2[-1]
    if email_ids1:
        file_path1 = download_file_from_email(ids1, mail)
        if file_path1:
            file_paths.append(file_path1)

    if email_ids2:
        file_path2 = download_file_from_email(ids2, mail)
        if file_path2:
            file_paths.append(file_path2)

    if not file_paths:
        print("No email found with the specified criteria.")

    mail.logout()


get_download_data()


    
   