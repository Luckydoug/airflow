import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pygsheets
import pandas as pd
from datetime import date
from airflow.models import Variable
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 


from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine) 

def fetch_perc_det():
    gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
    sh = gc.open_by_key('1ZO1V5srBGYx6Tu7LXgDHhvmSrz-odHdkY5c9MreK56E')
    sh = sh[0]
    sh = pd.DataFrame(sh.get_all_records())

    # drop_table = """truncate table mabawa_dw.detractors_incentive_slab;"""
    # drop_table = pg_execute(drop_table)

    sh.to_sql('detractors_incentive_slab', con = engine, schema='mabawa_dw', if_exists = 'replace', index=False)


def fetch_perc_ins_rej():
    gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
    sh = gc.open_by_key('1ZO1V5srBGYx6Tu7LXgDHhvmSrz-odHdkY5c9MreK56E')
    sh = sh[1]
    sh = pd.DataFrame(sh.get_all_records())

    # drop_table = """truncate table mabawa_dw.rejections_incentive_slab;"""
    # drop_table = pg_execute(drop_table)

    sh.to_sql('rejections_incentive_slab', con = engine, schema='mabawa_dw', if_exists = 'replace', index=False)


def fetch_perc_sop():
    gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
    sh = gc.open_by_key('1ZO1V5srBGYx6Tu7LXgDHhvmSrz-odHdkY5c9MreK56E')
    sh = sh[2]
    sh = pd.DataFrame(sh.get_all_records())

    # drop_table = """truncate table mabawa_dw.sop_incentive_slab;"""
    # drop_table = pg_execute(drop_table)

    sh.to_sql('sop_incentive_slab', con = engine, schema='mabawa_dw', if_exists = 'replace', index=False)


# fetch_perc_det()
# fetch_perc_ins_rej()
# fetch_perc_sop()
# fetch_perc_nps()



