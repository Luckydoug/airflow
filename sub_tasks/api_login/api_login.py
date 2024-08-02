import sys

sys.path.append(".")

# import libraries
import json
import psycopg2
import requests
import pandas as pd
from pandas.io.json._normalize import nested_to_record
from sqlalchemy import create_engine
from airflow.models import Variable
from pangres import upsert
from sub_tasks.libraries.utils import createe_engine

# from sub_tasks.libraries.utils import create_unganda_engine
# from sub_tasks.libraries.utils import create_rwanda_engine

# api login
login_url = "https://10.40.16.9:4300/OpticaBI/XSJS/login_demo.xsjs"
hrms_login_url = "http://52.71.65.50:8094/API/Login/AuthenticateLogin"


def login():
    engine = createe_engine()

    payload = json.dumps(
        {"CompanyDB": "OPTICA_LIVE", "Password": "44game", "UserName": "data5"}
    )
    headers = {"Content-Type": "application/json"}

    response = requests.request(
        "POST", login_url, headers=headers, data=payload, verify=False
    )
    data = response.json()
    SessionId = data["SessionId"]
    # return SessionId

    df = pd.DataFrame({"country": "Kenya", "session_id": SessionId}, index=[0])
    df = df.set_index("country")

    upsert(
        engine=engine,
        df=df,
        schema="mabawa_staging",
        table_name="api_login",
        if_row_exists="update",
        create_table=False,
    )


def login_uganda():
    engine = createe_engine()

    payload = json.dumps(
        {"CompanyDB": "UGANDA", "Password": "happy83", "UserName": "data11"}
    )
    headers = {"Content-Type": "application/json"}

    response = requests.request(
        "POST", login_url, headers=headers, data=payload, verify=False
    )
    data = response.json()
    SessionId = data["SessionId"]
    # return SessionId

    df = pd.DataFrame({"country": "Uganda", "session_id": SessionId}, index=[0])
    df = df.set_index("country")

    upsert(
        engine=engine,
        df=df,
        schema="mabawa_staging",
        table_name="api_login",
        if_row_exists="update",
        create_table=False,
    )


def login_rwanda():
    engine = createe_engine()

    payload = json.dumps(
        {"CompanyDB": "RWANDA", "Password": "1234", "UserName": "data2"}
    )
    headers = {"Content-Type": "application/json"}

    response = requests.request(
        "POST", login_url, headers=headers, data=payload, verify=False
    )
    data = response.json()
    SessionId = data["SessionId"]
    # return SessionId

    df = pd.DataFrame({"country": "Rwanda", "session_id": SessionId}, index=[0])
    df = df.set_index("country")

    upsert(
        engine=engine,
        df=df,
        schema="mabawa_staging",
        table_name="api_login",
        if_row_exists="update",
        create_table=False,
    )


def login_hrms():
    engine = createe_engine()

    payload = json.dumps({"cmpCode": "OL01", "userName": "api", "password": "Happy@12"})
    headers = {"Content-Type": "application/json"}

    response = requests.request(
        "POST", hrms_login_url, headers=headers, data=payload, verify=False
    )
    data = response.json()
    print(data)
    access_token = data["result"]["access_token"]

    df = pd.DataFrame({"country": "Kenya:HRMS", "session_id": access_token}, index=[0])
    df = df.set_index("country")

    upsert(
        engine=engine,
        df=df,
        schema="mabawa_staging",
        table_name="api_login",
        if_row_exists="update",
        create_table=False,
    )

def login_hrms_uganda():
    engine = createe_engine()

    payload = json.dumps({"cmpCode": "OLUG", "userName": "api", "password": "Happy@34"})
    headers = {"Content-Type": "application/json"}

    response = requests.request(
        "POST", hrms_login_url, headers=headers, data=payload, verify=False
    )
    data = response.json()
    print(data)
    access_token = data["result"]["access_token"]

    df = pd.DataFrame({"country": "Uganda:HRMS", "session_id": access_token}, index=[0])
    df = df.set_index("country")

    upsert(
        engine=engine,
        df=df,
        schema="mabawa_staging",
        table_name="api_login",
        if_row_exists="update",
        create_table=False,
    )

def login_hrms_rwanda():
    engine = createe_engine()

    payload = json.dumps({"cmpCode": "OLRWD", "userName": "api", "password": "Happy@80"})
    headers = {"Content-Type": "application/json"}

    response = requests.request(
        "POST", hrms_login_url, headers=headers, data=payload, verify=False
    )
    data = response.json()
    print(data)
    access_token = data["result"]["access_token"]

    df = pd.DataFrame({"country": "Rwanda:HRMS", "session_id": access_token}, index=[0])
    df = df.set_index("country")

    upsert(
        engine=engine,
        df=df,
        schema="mabawa_staging",
        table_name="api_login",
        if_row_exists="update",
        create_table=False,
    )

# login_rwanda()
# login_uganda()
# login_hrms()    
# login_hrms_rwanda()
# login_hrms_uganda
