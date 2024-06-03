import pandas as pd
import pygsheets
from airflow.models import variable
from pangres import upsert
from sub_tasks.data.connect import engine

def fetch_staff_emails():
    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8')
    staff_emails = pd.DataFrame(sh.worksheet_by_title("Emails").get_all_records())#.fillna("NAN")
    staff_emails.rename(columns={
       "Payroll No": "payroll_no",
       "Name": 'staff_name',
       "Email": "email"
    }, inplace=True)

    staff_emails = staff_emails.set_index(['payroll_no'])[['staff_name','email']]

    upsert(engine=engine,
           df=staff_emails,
           schema='mabawa_dw',
           table_name='staff_emails',
           if_row_exists='update',
           create_table=False
    )

def TAT():
    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8')
    tat = pd.DataFrame(sh.worksheet_by_title("Insurance TAT").get_all_records())#.fillna("NAN")
    tat.rename(columns={
       "Insurance Company": "insurance_company",
       "Turnaround Time": 'tat',
    }, inplace=True)

    # staff_emails = staff_emails.set_index(['payroll_no'])[['staff_name','email']]

    tat[['insurance_company','tat']].to_sql('insurance_tat', con = engine, schema='mabawa_dw', if_exists = 'append', index=False)



