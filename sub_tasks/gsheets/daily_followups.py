from airflow.models import Variable
from sub_tasks.data.connect import (pg_execute, engine)
from pangres import upsert
import pandas as pd
import pygsheets
import sys
sys.path.append(".")

def high_rx_nonconversion():

    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1-litSZaRZ9L4vEwjelKtCfaccG1PtwMudcBwKHaPYPQ')
    sh = pd.DataFrame(sh.worksheet_by_title("High Rx Non conversion").get_all_records()).astype(str).fillna("NAN")
    sh = sh.drop_duplicates()

    sh.rename(columns=lambda x: x.lower().replace(' ', '_'),inplace=True)
    # sh.set_index(['customer_code','et_date_and_time','branch'],inplace=True)

    query = """truncate mabawa_staging.fllwup_high_rx_nonconversion;"""
    query = pg_execute(query)

    sh.to_sql('fllwup_high_rx_nonconversion', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
  
def long_queue_times():

    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1-litSZaRZ9L4vEwjelKtCfaccG1PtwMudcBwKHaPYPQ')
    sh = pd.DataFrame(sh.worksheet_by_title("Long queue wait times").get_all_records()).astype(str).fillna("NAN")
    sh = sh.drop_duplicates()

    sh.rename(columns=lambda x: x.lower().replace(' ', '_'),inplace=True)
    # sh.set_index(['customer_code','et_date_and_time','branch'],inplace=True)

    query = """truncate mabawa_staging.fllwup_long_queue_times;"""
    query = pg_execute(query)

    sh.to_sql('fllwup_long_queue_times', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)


def insurance_not_submitted():

    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1-litSZaRZ9L4vEwjelKtCfaccG1PtwMudcBwKHaPYPQ')
    sh = pd.DataFrame(sh.worksheet_by_title("Non Submitted Insurance Clients").get_all_records()).astype(str).fillna("NAN")
    sh = sh.drop_duplicates()

    sh.rename(columns=lambda x: x.lower().replace(' ', '_'),inplace=True)
    # sh.set_index(['customer_code','et_date_and_time','branch'],inplace=True)

    query = """truncate mabawa_staging.fllwup_insurance_not_submitted;"""
    query = pg_execute(query)

    sh.to_sql('fllwup_insurance_not_submitted', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)


def insurance_errors():

    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1-litSZaRZ9L4vEwjelKtCfaccG1PtwMudcBwKHaPYPQ')
    sh = pd.DataFrame(sh.worksheet_by_title("Insurance errors").get_all_records()).astype(str).fillna("NAN")
    sh = sh.drop_duplicates()

    sh.rename(columns=lambda x: x.lower().replace(' ', '_'),inplace=True)
    # sh.set_index(['customer_code','et_date_and_time','branch'],inplace=True)

    query = """truncate mabawa_staging.fllwup_insurance_errors;"""
    query = pg_execute(query)

    sh.to_sql('fllwup_insurance_errors', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)


def upload_snt_preauth():

    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1-litSZaRZ9L4vEwjelKtCfaccG1PtwMudcBwKHaPYPQ')
    sh = pd.DataFrame(sh.worksheet_by_title("Delayed Orders from Upload Attachment to Sent - Preauth").get_all_records()).astype(str).fillna("NAN")
    sh = sh.drop_duplicates()

    sh.rename(columns=lambda x: x.lower().replace(' ', '_'),inplace=True)
    # sh.set_index(['customer_code','et_date_and_time','branch'],inplace=True)

    # query = """truncate mabawa_staging.fllwup_upload_snt_preauth;"""
    # query = pg_execute(query)

    sh.to_sql('fllwup_upload_snt_preauth', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)


# long_queue_times()

