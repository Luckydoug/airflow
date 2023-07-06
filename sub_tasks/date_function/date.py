import pandas as pd
import datetime 
from datetime import date, timedelta

def From():
    return date.today()

# print(From())

def To(x):
    return date.today() - timedelta(days=x)

# print(To(x=2))