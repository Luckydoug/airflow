from workalendar.africa import Kenya
from airflow.models import Variable
import holidays as pyholidays
import datetime
import pygsheets
import businesstimedelta
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
from sub_tasks.libraries.utils import createe_engine


def fetch_holidays():
    query = """
    select holiday_date as "Date",
    holiday_name as "Holiday"
    from mabawa_dw.dim_holidays;
    """

    to_drop =  pd.read_sql_query(query, con=createe_engine())

    date_objects = pd.to_datetime(to_drop['Date'], dayfirst=True).dt.date
    holiday_dict = dict(zip(date_objects, to_drop['Holiday']))

    return holiday_dict 



FMT = '%H:%M'
# cal = Kenya()
# hl = cal.holidays(datetime.date.today().year)
# hl_dict = dict(hl)
# hl_dict = {key: val for key, val in hl_dict.items() if val != 'New Years Eve'}
# hl_ls = list(hl_dict.keys())


def return_working_hours_dict(row, working_hours):
    branch_working_hours = {row["Outlet"]: {}}
    branch_work = working_hours[
        working_hours["Warehouse Code"]== row["Outlet"]
    ]
    working_days = branch_work["Days"].tolist()
    for single_day in working_days:
        day_work = branch_work[branch_work["Days"] == single_day]
        index1 = day_work.set_index("Days")
        branch_working_hours[row["Outlet"]].update(
            {
                single_day: {"Start Time": index1.loc[single_day, "Start Time"], 
                "End Time": index1.loc[single_day, "End Time"]}
            })
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


holidays_to_remove = [
    datetime.date(2023, 10, 10),
    datetime.date(2023, 6, 1),
    datetime.date(2023, 10, 20)
]

def return_business_hours(workday, country = None):
    holiday_dict = fetch_holidays()
    vic_holidays = pyholidays.KE()
    holidays = businesstimedelta.HolidayRule(vic_holidays)
    vic_holidays = vic_holidays.append(holiday_dict)
    if country == "Uganda":
        for tup in holidays_to_remove:
            if (tup) in holidays.holidays:
                holidays.holidays.pop(tup)
    businesshrs = businesstimedelta.Rules([workday, holidays])
    return businesshrs


def calculate_time_difference(row, x, y, working_hours, country = None):
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

    normal_business_hours = return_business_hours(normal_day_rule, country)

    sat_start = pd.to_datetime(
        working_hours[branch]["Saturday"]["Start Time"]).time().strftime('%H:%M')
    sat_end = pd.to_datetime(
        working_hours[branch]["Saturday"]["End Time"]).time().strftime('%H:%M')
    saturday_rule = businesstimedelta.WorkDayRule(
        start_time=datetime.time.fromisoformat(sat_start),
        end_time=datetime.time.fromisoformat(sat_end),
        working_days=[5])

    saturday_business_hours = return_business_hours(saturday_rule, country)

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

            sunday_business_hours = return_business_hours(Sunday, country)
            return ((normal_business_hours.difference(start, end).hours + float(normal_business_hours.difference(start, end).seconds) / 3600) + (sunday_business_hours.difference(start, end).hours + float(sunday_business_hours.difference(start, end).seconds) / 3600) + (saturday_business_hours.difference(start, end).hours + float(saturday_business_hours.difference(start, end).seconds) / 3600)) * 60

        else:
            return ((normal_business_hours.difference(start, end).hours + float(normal_business_hours.difference(start, end).seconds) / 3600) + (saturday_business_hours.difference(start, end).hours + float(saturday_business_hours.difference(start, end).seconds) / 3600)) * 60
    else:
        return 0
    
def return_working_hours_dict1(row, working_hours):
    branch_working_hours = {row["Outlet"]: {}}
    branch_work = working_hours[working_hours["Warehouse Code"]== row["Outlet"]]
    working_days = branch_work["Days"].tolist()
    for single_day in working_days:
        day_work = branch_work[branch_work["Days"] == single_day]
        index1 = day_work.set_index("Days")
        branch_working_hours[row["Outlet"]].update(
            {
                single_day: {"Start Time": index1.loc[single_day, "Start Time"], 
                "Auto Time": index1.loc[single_day, "Auto Time"]}
            })
    return branch_working_hours 

def get_working_hours1(row, working_hours):
    branch_working_hours = return_working_hours_dict1(row, working_hours)
    for k1, v1 in branch_working_hours.copy().items():
        for k2, v2 in v1.copy().items():
            tdelta = datetime.datetime.strptime(branch_working_hours[row["Outlet"]][k2]["Auto Time"], FMT) - datetime.datetime.strptime(
                branch_working_hours[row["Outlet"]][k2]["Start Time"], FMT)
            seconds = tdelta.total_seconds()
            sec = int(seconds)
            if sec <= 60:
                del branch_working_hours[row["Outlet"]][k2]
    return branch_working_hours


def calculate_time_difference1(row, x, y, working_hours, country = None):
    branch = row["Outlet"]
    working_hours = get_working_hours1(row, working_hours)
    normal_start = pd.to_datetime(
        working_hours[branch]["Monday"]["Start Time"]).time().strftime('%H:%M')
    normal_end = pd.to_datetime(
        working_hours[branch]["Monday"]["Auto Time"]).time().strftime('%H:%M')
    normal_day_rule = businesstimedelta.WorkDayRule(
        start_time=datetime.time.fromisoformat(normal_start),
        end_time=datetime.time.fromisoformat(normal_end),
        working_days=[0, 1, 2, 3, 4])

    normal_business_hours = return_business_hours(normal_day_rule,country)

    sat_start = pd.to_datetime(
        working_hours[branch]["Saturday"]["Start Time"]).time().strftime('%H:%M')
    sat_end = pd.to_datetime(
        working_hours[branch]["Saturday"]["Auto Time"]).time().strftime('%H:%M')
    saturday_rule = businesstimedelta.WorkDayRule(
        start_time=datetime.time.fromisoformat(sat_start),
        end_time=datetime.time.fromisoformat(sat_end),
        working_days=[5])

    saturday_business_hours = return_business_hours(saturday_rule,country)

    start = row[x]
    end = row[y]
    if end > start:
        if "Sunday" in working_hours[row["Outlet"]].keys():
            sun_start = pd.to_datetime(
                working_hours[branch]["Sunday"]["Start Time"]).time().strftime('%H:%M')
            sun_end = pd.to_datetime(
                working_hours[branch]["Sunday"]["Auto Time"]).time().strftime('%H:%M')
            Sunday = businesstimedelta.WorkDayRule(
                start_time=datetime.time.fromisoformat(sun_start),
                end_time=datetime.time.fromisoformat(sun_end),
                working_days=[6])

            sunday_business_hours = return_business_hours(Sunday,country)
            return ((normal_business_hours.difference(start, end).hours + float(normal_business_hours.difference(start, end).seconds) / 3600) + (sunday_business_hours.difference(start, end).hours + float(sunday_business_hours.difference(start, end).seconds) / 3600) + (saturday_business_hours.difference(start, end).hours + float(saturday_business_hours.difference(start, end).seconds) / 3600)) * 60

        else:
            return ((normal_business_hours.difference(start, end).hours + float(normal_business_hours.difference(start, end).seconds) / 3600) + (saturday_business_hours.difference(start, end).hours + float(saturday_business_hours.difference(start, end).seconds) / 3600)) * 60
    else:
        return 0