import os
import re
import pandas as pd
import email
import imaplib
import datetime
import requests
from datetime import time
import holidays as pyholidays
import businesstimedelta
import pygsheets
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from airflow.models import variable
from sub_tasks.libraries.utils import return_evn_credentials
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert


def report_date():
    current_data = datetime.datetime.now()
    if current_data.weekday() == 0:
        days_back = datetime.timedelta(days=1)
        report_day = current_data - days_back

    else:
        report_day = current_data

    date_since = report_day.strftime("%d-%b-%Y")

    return date_since


def login_to_email():
    user = "_3cx"
    user, password = return_evn_credentials(user.lower())
    print(user, password)
    imap_url = "imap.gmail.com"

    mail = imaplib.IMAP4_SSL(imap_url)
    mail.login(user, password)
    mail.select("inbox")

    return mail


def download_file_from_email(mail, email_id, path):
    _, email_data = mail.fetch(email_id, "(RFC822)")
    email_message = email.message_from_bytes(email_data[0][1])

    soup = BeautifulSoup(email_message.get_payload(), "html.parser")
    links = soup.find_all("a")
    print(links)
    if len(links) >= 2:
        link = links[1]["href"]
    else:
        print("No link found in the email body.")
        return None

    response = requests.get(link, verify=False)
    file_name = link.split("/")[-1]
    with open(f"{path}3CX/{file_name}", "wb") as f:
        f.write(response.content)
    return f"{path}3CX/{file_name}"


def save_file(sender, subject, has_attachment, date_since, path):
    current_date = datetime.datetime.now()
    mail = login_to_email()
    search_string = 'FROM "{}" SUBJECT "{}" SENTSINCE {}'.format(
        sender, subject, date_since
    )
    results, data = mail.search(None, search_string)
    email_id = data[0].split()
    if has_attachment:
        for num in email_id:
            result, data = mail.fetch(num, "RFC822")
            raw_email = data[0][1]
            msg = email.message_from_bytes(raw_email)
            for part in msg.walk():
                if part.get_content_maintype() == "multipart":
                    continue
                if part.get("Content-Disposition") is None:
                    continue

                filename = part.get_filename()
                if filename:
                    file_path = os.path.join(f"{path}3CX/", filename)
                    print(file_path)
                    with open(file_path, "wb") as fp:
                        fp.write(part.get_payload(decode=True))
                    return file_path
    else:
        if current_date.weekday() == 1:
            ids = email_id[-2]
        else:
            ids = email_id[-1]
        if email_id:
            file_path = download_file_from_email(mail, ids, path)
        if not email_id:
            print("No email found with the specified criteria.")

        return file_path


def count_direct(x):
    return (x == "Direct").sum()


def count_na(x):
    return x.isna().sum()


def call_status(row):
    if row["Status"] == "unanswered":
        return "Unreachable", 0, 0
    if row["Status"] == "answered" and row["TT Time"] <= 10:
        return "Unanswered", 0, row["Total Time"]
    return "Answered", row["Talk Time"], 0


def custom_mean(series):
    filtered_values = series[series > 0]
    if len(filtered_values) > 0:
        return filtered_values.mean()
    else:
        return None


def extract_phone_number(text):
    pattern = r"\b\d{9}\b"
    matches = re.findall(pattern, text)
    return matches[0] if matches else None


def return_status(row):
    if row["Status"] == "answered":
        return row["Talking"]
    else:
        return row["Status"]


def get_yesterday_date():
    import datetime
    from datetime import timedelta

    today = datetime.date.today()
    if today.weekday() == 0:
        days_substract = 3
    else:
        days_substract = 2

    yesterday = today - timedelta(days=days_substract)
    return yesterday


def return_call_status(row):
    if pd.isna(row["Talking"]):
        return "Not Yet Called"
    elif row["Talking"] == "unanswered":
        return "Unanswered"
    elif (not pd.isna(row["Talking"])) and (row["Time"] <= 10):
        return "Unanswered"
    else:
        return "Answered"


def check_time_range(timestamp_str):
    if pd.isna(timestamp_str):
        return "Not Called"
    timestamp = datetime.strptime(timestamp_str, "%m/%d/%Y %I:%M:%S %p")
    time_val = timestamp.time()
    if time(9, 0) <= time_val <= time(9, 30):
        return "9:00AM - 9:30AM"
    elif time(9, 31) <= time_val <= time(10, 0):
        return "9:31AM - 10:00AM"
    elif time(10, 1) <= time_val <= time(10, 30):
        return "10:01AM - 10:30AM"
    elif time(10, 31) <= time_val <= time(11, 0):
        return "10:31AM - 11:00AM"
    else:
        return "11: 01AM - 5:59PM"


def return_business_hours(workday, holiday_dict):
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    vic_holidays = vic_holidays.append(holiday_dict)
    businesshrs = businesstimedelta.Rules([workday, holidays])
    return businesshrs


def calculate_time_difference(row, x, y):
    service_key = pygsheets.authorize(
        service_file=r"C:\Users\Lucky\Downloads\opticabi-f0b48389fa76.json"
    )
    sheet = service_key.open_by_key("1yRlrhjTOv94fqNpZIiA9xToKbDi9GaWkdoGrG4FcdKs")
    to_drop = pd.DataFrame(sheet[0].get_all_records())

    date_objects = pd.to_datetime(to_drop["Date"], format="%Y-%m-%d").dt.date
    holiday_dict = dict(zip(date_objects, to_drop["Holiday"]))

    import datetime

    normal_day_rule = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(18),
        working_days=[0, 1, 2, 3, 4],
    )

    normal_business_hours = return_business_hours(
        normal_day_rule, holiday_dict=holiday_dict
    )
    saturday_rule = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9), end_time=datetime.time(16), working_days=[5]
    )

    saturday_business_hours = return_business_hours(saturday_rule, holiday_dict)

    start = row[x]
    end = row[y]
    if end > start:
        return (
            (
                normal_business_hours.difference(start, end).hours
                + float(normal_business_hours.difference(start, end).seconds) / 3600
            )
            + (
                saturday_business_hours.difference(start, end).hours
                + float(saturday_business_hours.difference(start, end).seconds) / 3600
            )
        ) * 60
    else:
        return 0


def categorize_call_duration(row):
    if row["Time Difference"] <= 5:
        return "0 - 5 (Mins)"
    elif row["Time Difference"] > 5 and row["Time Difference"] <= 10:
        return "6 - 10 (Mins)"
    elif row["Time Difference"] > 10 and row["Time Difference"] <= 15:
        return "11 - 15 (Mins)"
    elif row["Time Difference"] > 15 and row["Time Difference"] <= 20:
        return "16 - 20 (Mins)"
    else:
        return "20 (Mins) +"


# save_file(
#     sender="noreply@3cx.net",
#     subject="Your 3CX Report CS Inbound Calls is ready",
#     has_attachment=False,
#     date_since="25-Apr-2024",
#     path=uganda_path
# )


def upsert(engine, df, schema, table_name, if_row_exists="update"):
    table = Table(table_name, MetaData(), autoload_with=engine, schema=schema)
    conflict_columns = ["date", "caller_id", "destination"]

    insert_stmt = insert(table).values(df.to_dict(orient="records"))

    if if_row_exists == "update":
        update_dict = {
            c.name: c
            for c in insert_stmt.excluded
            if c.name not in conflict_columns and c.name != "id"
        }
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=conflict_columns, set_=update_dict
        )
    else:
        stmt = insert_stmt.on_conflict_do_nothing(index_elements=conflict_columns)

    with engine.connect() as conn:
        conn.execute(stmt)


def return_business_hours(workday):
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    vic_holidays = vic_holidays.append({})
    businesshrs = businesstimedelta.Rules([workday, holidays])
    return businesshrs


def calculate_time_difference(row, x, y):
    import datetime

    normal_day_rule = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9),
        end_time=datetime.time(18),
        working_days=[0, 1, 2, 3, 4],
    )

    normal_business_hours = return_business_hours(normal_day_rule)
    saturday_rule = businesstimedelta.WorkDayRule(
        start_time=datetime.time(9), end_time=datetime.time(16), working_days=[5]
    )

    saturday_business_hours = return_business_hours(saturday_rule)

    start = row[x]
    end = row[y]
    if end > start:
        return (
            (
                normal_business_hours.difference(start, end).hours
                + float(normal_business_hours.difference(start, end).seconds) / 3600
            )
            + (
                saturday_business_hours.difference(start, end).hours
                + float(saturday_business_hours.difference(start, end).seconds) / 3600
            )
        ) * 60
    else:
        return 0
