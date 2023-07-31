from airflow.models import variable
import datetime
import numpy as np
from dotenv import load_dotenv
from datetime import timedelta
from email.mime.application import MIMEApplication
import os
import pathlib
import smtplib
import urllib
import ssl
import time
import pandas as pd
from colorama import Fore
import calendar
import pygsheets
from sqlalchemy import create_engine

load_dotenv()

service_file = r"/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json"
uganda_path = r"/home/opticabi/Documents/uganda_reports/"
rwanda_path = r"/home/opticabi/Documents/rwanda_reports/"
path = r"/home/opticabi/Documents/optica_reports/"
# This line sets the target(in minutes) for the draft to upload report
target = 8

"""
Acess the Data Team Repo Gsheet and pull all the necessary data
We are acessing by title so that if the order of the gsheet is altered at any given time,
then we don't run into errors.

"""

"""
This documentation serves as a guide for working with a diverse file. 
It is important to exercise caution when making modifications or additions to the code within this file, as such changes may have an impact on one or more reports. 
Prior to making any alterations, it is advisable to identify the reports that rely on the functions present in this file.
Should the need arise to make adjustments, it is recommended to utilize if and elif statements and ensure their alignment caters to your specific requirements. 
By adhering to this approach, you can mitigate potential issues and maintain the integrity of the file and its associated reports.
"""

def fetch_gsheet_data():
    service_key = pygsheets.authorize(service_file=service_file)
    sheet = service_key.open_by_key('1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8')
    staff = pd.DataFrame(sheet.worksheet_by_title("Emails").get_all_records())
    branch_data = pd.DataFrame(sheet.worksheet_by_title("SRM_RM_List").get_all_records())
    branch_data["Outlet"] = branch_data["Outlet"].fillna("NAN")
    working_hours = pd.DataFrame(sheet.worksheet_by_title("Working Hours").get_all_records())
    working_hours["Warehouse Code"] = working_hours["Warehouse Code"].fillna("NAN")
    opening_time = pd.DataFrame(sheet.worksheet_by_title("Kenya Opening Time").get_all_records())
    uganda_opening = pd.DataFrame(sheet.worksheet_by_title("Uganda Opening Time").get_all_records())
    ug_srm_rm = pd.DataFrame(sheet.worksheet_by_title("UG_SRM_RM").get_all_records())
    ug_working_hours = pd.DataFrame(sheet.worksheet_by_title("UG_Working_Hours").get_all_records())
    rw_working_hours = pd.DataFrame(sheet.worksheet_by_title("RW_Working_Hours").get_all_records())
    itr_cutoff = pd.DataFrame(sheet.worksheet_by_title("ITR_Cutoffs").get_all_records())
    orders_cutoff = pd.DataFrame(sheet.worksheet_by_title("Order_Cutoffs").get_all_records())
    rw_srm_rm = pd.DataFrame(sheet.worksheet_by_title("RW_SRM_RM").get_all_records())
    department_emails = pd.DataFrame(sheet.worksheet_by_title("Operations Department Emails").get_all_records())
    sheet_orderstodrop = service_key.open_by_key('1cnpNo85Hncf9cdWBfkQ1dn0TYnGfs-PjVGp1XMjk2Wo')
    OrdersWithIssues = pd.DataFrame(sheet_orderstodrop[0].get_all_records())    
    ITRWithIssues = pd.DataFrame(sheet_orderstodrop[2].get_all_records()) 
    orders_to_drop = service_key.open_by_key("1HQ_y1omRXpV_KxDuUK2kEbgIHmVeIxqM1LxxHH8up4o")
    orders_drop = pd.DataFrame(orders_to_drop.worksheet_by_title("Kenya").get_all_records())["Order Number"].to_list()
    rwanda_opening = pd.DataFrame(sheet.worksheet_by_title("Rwanda Opening Time").get_all_records())
    
    return {
        'staff': staff,
        'branch_data': branch_data,
        'working_hours': working_hours,
        'opening_time': opening_time,
        'uganda_opening': uganda_opening,
        'ug_srm_rm': ug_srm_rm,
        'ug_working_hours': ug_working_hours,
        'itr_cutoff': itr_cutoff,
        'orders_cutoff': orders_cutoff,
        'rw_srm_rm': rw_srm_rm,
        'orders_with_issues': OrdersWithIssues,
        'itrs_with_issues' : ITRWithIssues,
        'department_emails' : department_emails,
        'orders_drop': orders_drop,
        'rwanda_opening': rwanda_opening,
        'rw_working_hours': rw_working_hours
        }



def return_sunday_truth():
    today = datetime.date.today()
    if today.weekday() == 6:
        return True
    return False
    


def get_yesterday_date(truth = False):
    today = datetime.date.today()
    if truth and today.weekday() == 0:
        days_to_subtract = 2
    else:
        days_to_subtract = 1
    return today - datetime.timedelta(days=days_to_subtract)


def get_month_first_day():
    today = datetime.date.today()
    if today.day == 1:
        return datetime.date(today.year, today.month - 1, 1)
    else:
        return datetime.date(today.year, today.month, 1)


def get_day_name_days_ago(days_ago):
    today = datetime.datetime.today()
    delta = datetime.timedelta(days=days_ago)
    target_date = today - delta
    day_name = target_date.strftime("%A")
    return day_name


def get_previous_week_dates():
    today = datetime.datetime.now().date()
    start_date = today - timedelta(days=today.weekday() + 7)
    end_date = start_date + timedelta(days=6)
    return start_date, end_date

# Usage: Uganda Rwanda Daily Net Sales, Optica and York House Incentives


def get_todate(truth = False):
    today = datetime.date.today()
    if truth and today.weekday() == 0:
        days_substract = 2
    else:
        days_substract = 1

    return today - timedelta(days=days_substract)


def assert_date_modified(files):
    condition = True
    name = ""
    for file in files:
        win_file = pathlib.Path(file)
        filename = os.path.basename(file)
        name = os.path.splitext(filename)[0]
        if not win_file.exists():
            print(
                f"The file named {name} does not exists. Ensure you have the file you are trying to share")
            condition = False
            return condition
        else:
            last_modified = datetime.datetime.fromtimestamp(
                win_file.stat().st_mtime, tz=datetime.timezone.utc)
            date_modified = last_modified.date()
            today = datetime.date.today()
            if date_modified != today:
                print(
                    f"The file you are trying to send was last modified on {date_modified}. Ensure that the Notebook has run successfully so you don't send the same report twice. Check this file: {name}.xlsx")
                condition = False
                return condition
            else:
                condition = True

    return condition


def send_report(email_message, your_email, password, receiver_email, name):
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(your_email, password)
        server.sendmail(
            your_email, 
            receiver_email,
            email_message.as_string()
        )

        print(Fore.GREEN + f'{name.split(" ")[0]} sent✔')


def clean_folder(dir_name=r"/home/opticabi/Documents/optica_reports/"):
    files = os.listdir(dir_name)
    for file in files:
        if file.endswith(".xlsx"):
            os.remove(os.path.join(dir_name, file))


def clean_folder_uganda(dir_name=r"/home/opticabi/Documents/uganda_reports/"):
    files = os.listdir(dir_name)
    time.sleep(1)
    for file in files:
        if file.endswith(".xlsx"):
            os.remove(os.path.join(dir_name, file))


def clean_folder_rwanda(dir_name=r"/home/opticabi/Documents/rwanda_reports/"):
    files = os.listdir(dir_name)
    time.sleep(1)
    for file in files:
        if file.endswith(".xlsx"):
            os.remove(os.path.join(dir_name, file))


def create_unganda_engine():
    key = "@Akb@rp@$$w0rtf31n"
    encoded_password = urllib.parse.quote(key)
    conn_string = f'postgresql://postgres:{encoded_password}@10.40.16.19:5432/mawingu'
    engine = create_engine(conn_string)
    return engine

def create_rwanda_engine():
    key = "@Akb@rp@$$w0rtf31n"
    encoded_password = urllib.parse.quote(key)
    conn_string = f'postgresql://postgres:{encoded_password}@10.40.16.19:5432/voler'
    engine = create_engine(conn_string)
    return engine


def create_initial_file(filename):
    if not os.path.exists(filename):
        with open(filename, "w") as file:
            file.write(
                f"initialrecord@gmail.com {datetime.date.today()} Sent" + "\n")
            file.close()
    else:
        return


def record_sent_branch(branch_email, filename):
    with open(filename, 'a') as sent:
        sent.write(
            branch_email + " " +str(datetime.date.today()) + " " + "Sent" + '\n'
        )
        print(Fore.YELLOW + f"{branch_email} saved.")
        sent.close()


def return_sent_emails(filename):
    records = open(filename, "r")
    data = records.read()
    emails = []
    lists = data.split("\n")
    del lists[-1]
    if len(lists):
        for email in lists:
            if email.split(" ")[0] != "Test" and email.split(" ")[1] == str(datetime.date.today()):
                emails.append(email.split(" ")[0])
    else:
        emails = []
    records.close()
    return emails


def get_four_weeks_date_range():
    today = datetime.datetime.now().date()
    end_date = today - datetime.timedelta(days=28)
    start_date = end_date
    date_range = []
    for i in range(4):
        end_date = start_date + datetime.timedelta(days=6)
        date_range.append((start_date, end_date))
        start_date = end_date + datetime.timedelta(days=1)
    return date_range


def clear(): return os.system('cls')


def highlight_spaces(row):
    color = "#ffff4c"
    if " " in row.values:
        return ['background-color: {}'.format(color) + '; font-weight: bold' + '; font-size: 4px' for v in row]
    else:
        return ['' for v in row.values]


def attach_file(email_message, filename, name):
    with open(filename, "rb") as f:
        file_attachment = MIMEApplication(f.read())

    file_attachment.add_header(
        "Content-Disposition",
        f"attachment; filename= {name}"
    )

    email_message.attach(file_attachment)


def save_file(email_message, reports, branch_name, file_name, path):
    filename = f"{path}{branch_name} {file_name}"
    with pd.ExcelWriter(filename) as writer:
       for report_name, report in reports.items():
           report.to_excel(writer, sheet_name = report_name, index = False)
        
    writer.save()
    writer.close()
    attach_file(email_message, filename, name=f"{branch_name} {file_name}")


def get_comparison_months():
    today = datetime.datetime.now().date()
    prev_month = today.month - 1
    prev_year = today.year
    if prev_month < 1:
        prev_month += 12
        prev_year -= 1
    prev_month_name = calendar.month_name[prev_month]

    prev_prev_month = prev_month - 1
    prev_prev_year = prev_year
    if prev_prev_month < 1:
        prev_prev_month += 12
        prev_prev_year -= 1
    prev_prev_month_name = calendar.month_name[prev_prev_month]

    return prev_prev_month_name, prev_month_name


def createe_engine():
    key = "@Akb@rp@$$w0rtf31n"
    encoded_password = urllib.parse.quote(key)
    conn_string = f'postgresql://postgres:{encoded_password}@10.40.16.19:5432/mabawa'
    return create_engine(conn_string)


def format_payroll_number(payroll_number):
    if int(payroll_number) < 100:
        return f"00{payroll_number}"
    elif int(payroll_number) < 1000:
        return f"0{payroll_number}"
    else:
        return str(payroll_number)


def get_rm_srm_total(dataframe, x=None, y=None, z=None, has_perc=False, avg_cols=[]):
    df = dataframe.copy()
    df = df.sort_values(by=["SRM", "RM"])
    df = df.reset_index(drop=True)

    grouped_srm = df.groupby('SRM').sum(numeric_only=True)
    if has_perc:
        for i in range(len(x)):
            grouped_srm[x[i]] = round(
                (grouped_srm[y[i]] / grouped_srm[z[i]])* 100, 0
            ).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)

    if len(avg_cols):
        grouped_srm[avg_cols[0]] = round(
            (grouped_srm[avg_cols[1]] / grouped_srm[avg_cols[2]]),0
        ).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)

    cols = grouped_srm.columns
    srm_names = grouped_srm.index.tolist()
    grouped_rm = df.groupby('RM').sum(numeric_only=True)

    if has_perc:
        for i in range(len(x)):
            grouped_rm[x[i]] = round(
                (grouped_rm[y[i]] / grouped_rm[z[i]]) * 100, 0
            ).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)

    if len(avg_cols):
        grouped_rm[avg_cols[0]] = round(
            (grouped_rm[avg_cols[1]] / grouped_rm[avg_cols[2]]),0
        ).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)

    rm_names = grouped_rm.index.tolist()
    for srm in srm_names:
        new_row = {'SRM': f"Total ({srm})", 'RM': None, 'Outlet': None}
        for i in range(len(cols)):
            new_row[cols[i]] = grouped_srm.loc[srm, cols[i]]

        if not ((df['SRM'] == f"Total ({srm})")).any():
            last_srm_index = df[df['SRM'] == srm].index[-1]
            df = pd.concat([df.loc[:last_srm_index], pd.DataFrame(
                new_row, index=[last_srm_index + 1]), df.loc[last_srm_index + 1:]])

    for rm in rm_names:
        new_row = {'RM': f"Total ({rm})", 'SRM': None, 'Outlet': None}
        for i in range(len(cols)):
            new_row[cols[i]] = grouped_rm.loc[rm, cols[i]]

        if not ((df['RM'] == f"Total ({rm})")).any():
            last_rm_index = df[df['RM'] == rm].index[-1]
            df = pd.concat([df.loc[:last_rm_index], pd.DataFrame(
                new_row, index=[last_rm_index + 1]), df.loc[last_rm_index + 1:]])

    return df


def get_rm_srm_total_multiindex(dataframe, x=None, y=None, z=None, week_month=None, a=None, b=None, c=None, report=None):
    first_month, second_month = get_comparison_months()
    df = dataframe.copy()
    df = df.sort_values(by=["SRM", "RM"])
    df = df.reset_index()

    grouped_srm = df.groupby('SRM').sum(numeric_only=True)
    srm_names = grouped_srm.index.tolist()
    grouped_rm = df.groupby('RM').sum(numeric_only=True)

    if week_month == "Month":
        grouped_rm[(f'{first_month}',    '%Rejected')] = round(
            (grouped_rm[(f'{first_month}',     'Rejected')] / grouped_rm[(f'{first_month}',     'Total Orders')]) * 100, 0)
        grouped_rm[(f'{second_month}',    '%Rejected')] = round(
            (grouped_rm[(f'{second_month}',     'Rejected')] / grouped_rm[(f'{second_month}',     'Total Orders')]) * 100, 0)
        grouped_srm[(f'{first_month}',    '%Rejected')] = round(
            (grouped_srm[(f'{first_month}',     'Rejected')] / grouped_srm[(f'{first_month}',     'Total Orders')]) * 100, 0)
        grouped_srm[(f'{second_month}',    '%Rejected')] = round(
            (grouped_srm[(f'{second_month}',     'Rejected')] / grouped_srm[(f'{second_month}',     'Total Orders')]) * 100, 0)

    if week_month == "Week":
        columns = list(set(report.columns.get_level_values(0)))
        for i in range(len(columns)):
            grouped_rm[(f'{columns[i]}',    a)] = round(
                (grouped_rm[(f'{columns[i]}',     b)] / grouped_rm[(f'{columns[i]}',     c)]) * 100, 0)
        for i in range(len(columns)):
            grouped_srm[(f'{columns[i]}',    a)] = round(
                (grouped_srm[(f'{columns[i]}',     b)] / grouped_srm[(f'{columns[i]}',     c)]) * 100, 0)

    rm_names = grouped_rm.index.tolist()
    columns = grouped_srm.columns
    for srm in srm_names:
        new_row = {('Outlet', ''): None, ('RM', ''): None,
                   ('SRM', ''): str(f"Total ({srm})")}
        for i in range(len(columns)):
            new_row[columns[i]] = grouped_srm.loc[srm, columns[i]]

        if not ((df['SRM'] == f"Total ({srm})")).any():
            last_srm_index = df[df['SRM'] == srm].index[-1]
            df = pd.concat([df.loc[:last_srm_index], pd.DataFrame(new_row, index=[
                           last_srm_index + 1], columns=df.columns), df.loc[last_srm_index + 1:]])

    for rm in rm_names:
        new_row = {('Outlet', ''): None, ('RM', ''): str(
            f"Total ({rm})"), ('SRM', ''): None}
        for i in range(len(columns)):
            new_row[columns[i]] = grouped_rm.loc[rm, columns[i]]

        if not ((df['RM'] == f"Total ({rm})")).any():
            last_rm_index = df[df['RM'] == rm].index[-1]
            df = pd.concat([df.loc[:last_rm_index], pd.DataFrame(new_row, index=[
                           last_rm_index + 1], columns=df.columns), df.loc[last_rm_index + 1:]])

    return df


date_ranges = get_four_weeks_date_range()
first_week_start = (date_ranges[0][0]).strftime('%Y-%m-%d')
first_week_end = date_ranges[0][1].strftime('%Y-%m-%d')
second_week_start = date_ranges[1][0].strftime('%Y-%m-%d')
second_week_end = date_ranges[1][1].strftime('%Y-%m-%d')
third_week_start = date_ranges[2][0].strftime('%Y-%m-%d')
third_week_end = date_ranges[2][1].strftime('%Y-%m-%d')
fourth_week_start = date_ranges[3][0].strftime('%Y-%m-%d')
fourth_week_end = date_ranges[3][1].strftime('%Y-%m-%d')

start_date = (date_ranges[0][0])
end_date = (date_ranges[3][1])


def date_in_range(date, start_date, end_date):
    if start_date <= date <= end_date:
        return True
    return False


def check_date_range(row, x):
    date = row[x].strftime('%Y-%m-%d')
    if date_in_range(date, first_week_start, first_week_end):
        return str(pd.to_datetime(first_week_start).strftime('%Y-%b-%d')) + " " + "to" + " " + str(pd.to_datetime(first_week_end).strftime('%Y-%b-%d'))
    elif date_in_range(date, second_week_start, second_week_end):
        return str(pd.to_datetime(second_week_start).strftime('%Y-%b-%d')) + " " + "to" + " " + str(pd.to_datetime(second_week_end).strftime('%Y-%b-%d'))
    elif date_in_range(date, third_week_start, third_week_end):
        return str(pd.to_datetime(third_week_start).strftime('%Y-%b-%d')) + " " + "to" + " " + str(pd.to_datetime(third_week_end).strftime('%Y-%b-%d'))
    elif date_in_range(date, fourth_week_start, fourth_week_end):
        return str(pd.to_datetime(fourth_week_start).strftime('%Y-%b-%d')) + " " + "to" + " " + str(pd.to_datetime(fourth_week_end).strftime('%Y-%b-%d'))
    else:
        return "None"

def return_four_week_range():
    first = str(pd.to_datetime(first_week_start).strftime('%Y-%b-%d')) + " " + "to" + " " + str(pd.to_datetime(first_week_end).strftime('%Y-%b-%d'))
    second = str(pd.to_datetime(second_week_start).strftime('%Y-%b-%d')) + " " + "to" + " " + str(pd.to_datetime(second_week_end).strftime('%Y-%b-%d'))
    third = str(pd.to_datetime(third_week_start).strftime('%Y-%b-%d')) + " " + "to" + " " + str(pd.to_datetime(third_week_end).strftime('%Y-%b-%d'))
    fourth = str(pd.to_datetime(fourth_week_start).strftime('%Y-%b-%d')) + " " + "to" + " " + str(pd.to_datetime(fourth_week_end).strftime('%Y-%b-%d'))

    return [first, second, third, fourth]
    


def arrange_dateranges(dataframe):
    multi_columns = dataframe.columns
    dates = []
    for col in multi_columns:
        date_range = col[0]
        start_date = pd.to_datetime(date_range.split(" to ")[0])
        dates.append(start_date)

    unique_dates = list(set(dates))

    sorted_dates = sorted(unique_dates)

    sorted_columns = []

    for date in sorted_dates:
        date_range = f"{date.strftime('%Y-%b-%d')} to " + \
            f"{(date + pd.Timedelta(6, unit='d')).strftime('%Y-%b-%d')}"
        sorted_columns.append(date_range)
    return sorted_columns


def manipulate_multiindex(dataframe, name, col1, col2, rename):
    stacked_dataframe = dataframe.stack()
    stacked_dataframe[name] = round(
        (stacked_dataframe[col1] / stacked_dataframe[col2]) * 100, 1).fillna(0).astype(str) + "%"
    unstacked_dataframe = stacked_dataframe.unstack()
    swapped_dataframe = unstacked_dataframe.swaplevel(0, 1, 1).sort_index(
        level=1, axis=1).reindex([col2, name], axis=1, level=1)

    sorted_columns = arrange_dateranges(swapped_dataframe)
    final_dataframe = swapped_dataframe.reindex(
        sorted_columns, axis=1, level=0)
    final_dataframe = final_dataframe.reindex([col2, name], level=1, axis=1)
    final_dataframe = final_dataframe.rename(columns={col2: rename}, level=1)

    return final_dataframe


def return_incentives_daterange():
    start_date = ''
    end_date = ''
    today = datetime.date.today()
    if today.day == 1:
        start_date = (datetime.date(today.year, today.month - 1, 1))
    else:
        start_date = (datetime.date(today.year, today.month, 1))

    if today.weekday() == 0:
        days_substract = 1
    else:
        days_substract = 1

    end_date = (today - timedelta(days=days_substract))

    return start_date, end_date


def save_dataframes_to_excel(path, dataframes, sheets, multindex):
    writer = pd.ExcelWriter(path, engine='xlsxwriter')
    workbook = writer.book

    header_format = workbook.add_format(
        {'bg_color': 'yellow', 'border': 1, 'bold': True})
    cell_format = workbook.add_format({'border': 1})

    for df, sheet_name, dex in zip(dataframes, sheets, multindex):
        df = df.replace({pd.NaT: ''})
        df = df.replace({np.nan: '', -np.inf: '', np.inf: ''})
        df.to_excel(writer, sheet_name=sheet_name, index=dex)

        worksheet = writer.sheets[sheet_name]

        # Write column headers
        for col_num, value in enumerate(df.columns.values):
            if isinstance(value, tuple):
                value = ' '.join(value)
            worksheet.write(0, col_num, value, header_format)

        # Write data rows
        num_rows, num_cols = df.shape
        for row_num in range(num_rows):
            for col_num in range(num_cols):
                value = df.iloc[row_num, col_num]
                if isinstance(value, tuple):
                    value = ' '.join(value)
                if pd.isnull(value):
                    value = ''
                elif pd.api.types.is_datetime64_any_dtype(value):
                    value = str(value)
                    worksheet.write_datetime(
                        row_num + 1, col_num, value, cell_format)
                else:
                    value = str(value)
                    worksheet.write(row_num + 1, col_num, value, cell_format)

        # Adjust column widths
        for col_num, col_value in enumerate(df.columns):
            column_width = max(df[col_value].astype(
                str).map(len).max(), len(str(col_value)))
            worksheet.set_column(col_num, col_num, column_width)

    writer.save()
    writer.close()



def create_weeky_branch_conversion(conversions, index, week_range, values, cols):
    weekly_branches_conversion = pd.pivot_table(
        conversions,
        index=index,
        columns=week_range,
        values=[values[0], values[1]],
        aggfunc={values[0]: "count", values[1]: "sum"}
    )

    return manipulate_multiindex(
        weekly_branches_conversion,
        cols[0],
        cols[1],
        cols[2],
        cols[3]
    )


def create_country_conversion(conversions, week_range, values, country, cols):
    conversions["Country"] = country
    summary_weekly_conversion = pd.pivot_table(
        conversions,
        index="Country",
        columns=week_range,
        values=[values[0], values[1]],
        aggfunc={values[0]: "count", values[1]: "sum"}
    )

    return manipulate_multiindex(
        summary_weekly_conversion,
        cols[0],
        cols[1],
        cols[2],
        cols[3]
    )


def create_branch_conversion(weekly_data, index, values, rename, cols_order):
    branch_conversion = pd.pivot_table(
        weekly_data,
        index=index,
        values=[values[0], values[1]],
        aggfunc={values[0]: "count", values[1]: "sum"}
    ).reset_index()

    branch_conversion["%Conversion"] = round(
        branch_conversion[values[1]] /
        branch_conversion[values[0]] * 100, 0
    ).astype(int).astype(str) + "%"

    return branch_conversion.rename(columns=rename)[cols_order]


def create_staff_conversion(weekly_data, index, values, rename, cols_order):
    staff_conversion = pd.pivot_table(
        weekly_data,
        index=index,
        values=[values[0], values[1]],
        aggfunc={values[0]: "count", values[1]: "sum"}
    ).reset_index()

    staff_conversion["%Conversion"] = round(
        staff_conversion[values[1]] /
        staff_conversion[values[0]] * 100, 0
    ).astype(int).astype(str) + "%"

    return staff_conversion.rename(columns=rename)[cols_order]


"""
This Function is being used in Uganda Conversion Report smtp.
The purpose of this function is to emphasize on modularity and reduce the lenghth of code,
while maintaining simplicity.
"""


def apply_multiindex_format(dataframe, styles, properties, new, old):
    dataframe = dataframe.reset_index(level=0)
    dataframe = dataframe.rename(columns={f"{old}": " "}, level=0)
    dataframe = dataframe.rename(columns={"": f"{new}"}, level=1)
    # index_levels = [(' ', 'Outlet'), (' ','Country')]
    # existing_columns = list(set(dataframe.columns) & set(index_levels))
    # if existing_columns:
    #     dataframe = dataframe.set_index(existing_columns)
    dataframe_html = dataframe.to_html()
    return dataframe_html


def style_dataframe(dataframe, styles, properties):
    dataframe = dataframe.style.hide_index().set_table_styles(
        styles).set_properties(**properties)
    dataframe_html = dataframe.to_html(doctype_html=True)
    return dataframe_html
