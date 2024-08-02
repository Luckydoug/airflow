import pandas as pd 
import datetime


def CallDate (startDate, holidays):
    workingDayCount = 0
    while workingDayCount < 2:
        startDate += datetime.timedelta(days=1)
        weekday = int(startDate.strftime('%w'))
        if weekday != 0:
            currentday = str(startDate.strftime('%m')) + '-' + str(startDate.strftime('%d'))
            if(not(any( currentday in day for day in holidays))):
                workingDayCount += 1
    
    return startDate