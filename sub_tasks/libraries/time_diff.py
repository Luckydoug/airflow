from workalendar.africa import Kenya
from sub_tasks.libraries.utils import (service_file)
from airflow.models import Variable
import holidays as pyholidays
from sqlalchemy import create_engine
import datetime
import pygsheets
import businesstimedelta
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

service_key = pygsheets.authorize(
    service_file=service_file)
sheet = service_key.open_by_key('1yRlrhjTOv94fqNpZIiA9xToKbDi9GaWkdoGrG4FcdKs')
to_drop = pd.DataFrame(sheet[0].get_all_records())
date_objects = pd.to_datetime(to_drop['Date'], dayfirst=True).dt.date
holiday_dict = dict(zip(date_objects, to_drop['Holiday']))
FMT = '%H:%M'
cal = Kenya()
hl = cal.holidays(datetime.date.today().year)
hl_dict = dict(hl)
hl_dict = {key: val for key, val in hl_dict.items() if val != 'New Years Eve'}
hl_ls = list(hl_dict.keys())


def return_working_hours_dict(row, working_hours):
    branch_working_hours = {row["Outlet"]: {}}
    branch_work = working_hours[working_hours["Warehouse Code"]
                                == row["Outlet"]]
    working_days = branch_work["Days"].tolist()
    for single_day in working_days:
        day_work = branch_work[branch_work["Days"] == single_day]
        index1 = day_work.set_index("Days")
        branch_working_hours[row["Outlet"]].update({single_day: {
                                                   "Start Time": index1.loc[single_day, "Start Time"], "End Time": index1.loc[single_day, "End Time"]}})
    return branch_working_hours


def get_working_hours(row, working_hours):
    branch_working_hours = return_working_hours_dict(row, working_hours)
    for k1, v1 in branch_working_hours.copy().items():
        for k2, v2 in v1.copy().items():
            tdelta = datetime.datetime.strptime(branch_working_hours[row["Outlet"]][k2]["End Time"], FMT) - datetime.datetime.strptime(
                branch_working_hours[row["Outlet"]][k2]["Start Time"], FMT)
            seconds = tdelta.total_seconds()
            sec = int(seconds)
            if sec <= 60:
                del branch_working_hours[row["Outlet"]][k2]
    return branch_working_hours


def return_business_hours(workday):
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    vic_holidays = vic_holidays.append(holiday_dict)
    businesshrs = businesstimedelta.Rules([workday, holidays])
    return businesshrs


def calculate_time_difference(row, x, y, working_hours):
    branch = row["Outlet"]
    working_hours = get_working_hours(row, working_hours)
    normal_start = pd.to_datetime(
        working_hours[branch]["Monday"]["Start Time"]).time().strftime('%H:%M')
    normal_end = pd.to_datetime(
        working_hours[branch]["Monday"]["End Time"]).time().strftime('%H:%M')
    normal_day_rule = businesstimedelta.WorkDayRule(
        start_time=datetime.time.fromisoformat(normal_start),
        end_time=datetime.time.fromisoformat(normal_end),
        working_days=[0, 1, 2, 3, 4])

    normal_business_hours = return_business_hours(normal_day_rule)

    sat_start = pd.to_datetime(
        working_hours[branch]["Saturday"]["Start Time"]).time().strftime('%H:%M')
    sat_end = pd.to_datetime(
        working_hours[branch]["Saturday"]["End Time"]).time().strftime('%H:%M')
    saturday_rule = businesstimedelta.WorkDayRule(
        start_time=datetime.time.fromisoformat(sat_start),
        end_time=datetime.time.fromisoformat(sat_end),
        working_days=[5])

    saturday_business_hours = return_business_hours(saturday_rule)

    start = row[x]
    end = row[y]
    if end > start:
        if "Sunday" in working_hours[row["Outlet"]].keys():
            sun_start = pd.to_datetime(
                working_hours[branch]["Sunday"]["Start Time"]).time().strftime('%H:%M')
            sun_end = pd.to_datetime(
                working_hours[branch]["Sunday"]["End Time"]).time().strftime('%H:%M')
            Sunday = businesstimedelta.WorkDayRule(
                start_time=datetime.time.fromisoformat(sun_start),
                end_time=datetime.time.fromisoformat(sun_end),
                working_days=[6])

            sunday_business_hours = return_business_hours(Sunday)
            return ((normal_business_hours.difference(start, end).hours + float(normal_business_hours.difference(start, end).seconds) / 3600) + (sunday_business_hours.difference(start, end).hours + float(sunday_business_hours.difference(start, end).seconds) / 3600) + (saturday_business_hours.difference(start, end).hours + float(saturday_business_hours.difference(start, end).seconds) / 3600)) * 60

        else:
            return ((normal_business_hours.difference(start, end).hours + float(normal_business_hours.difference(start, end).seconds) / 3600) + (saturday_business_hours.difference(start, end).hours + float(saturday_business_hours.difference(start, end).seconds) / 3600)) * 60
    else:
        return 0
