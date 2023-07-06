import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pandas as pd
from pandas.io.json._normalize import nested_to_record 
from sqlalchemy import create_engine
from airflow.models import Variable

# api login
login_url = "https://10.40.16.9:4300/OpticaBI/XSJS/login_demo.xsjs"

def login():

    payload = json.dumps({
    "CompanyDB": "OPTICA_LIVE",
    "Password": "chris83$",
    "UserName": "inconia"
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", login_url, headers=headers, data=payload, verify=False)
    data = response.json()
    SessionId = data['SessionId']
    return SessionId

def login_uganda():

    payload = json.dumps({
    "CompanyDB": "UGANDA",
    "Password": "happy83",
    "UserName": "data11"
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", login_url, headers=headers, data=payload, verify=False)
    data = response.json()
    SessionId = data['SessionId']
    return SessionId

def login_rwanda():

    payload = json.dumps({
    "CompanyDB": "RWANDA",
    "Password": "1234",
    "UserName": "data2"
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", login_url, headers=headers, data=payload, verify=False)
    data = response.json()
    SessionId = data['SessionId']
    return SessionId



# print(login_rwanda())
    
