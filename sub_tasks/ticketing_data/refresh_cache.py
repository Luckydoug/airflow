import sys

from numpy import nan
sys.path.append(".")

#import libraries
import json
import requests
import subprocess
import pandas as pd

def refresh_metadata():
    headers={}
    payload = {}
    url = "http://10.40.16.19:8080/pentaho/api/system/refresh/metadata"
    requests.request("GET", url, headers=headers, data=payload, verify=False)
    return "something"

def refresh_cache():
    headers={}
    payload = {}
    url = "http://10.40.16.19:8080/pentaho/plugin/cda/api/clearCache"
    requests.request("GET", url, headers=headers, data=payload, verify=False)
    return "something"

def cda_cache():

    rc = subprocess.call("/home/opticabi/airflow/dags/sub_tasks/ticketing_data/cda.sh")
    print(rc)
    return "CDA Cleared"

def cda_cache1():

    rc = subprocess.call("/home/opticabi/airflow/dags/sub_tasks/daily_salereport/cda.sh")
    print(rc)
    return "CDA Cleared"