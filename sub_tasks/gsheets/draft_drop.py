from airflow.models import Variable
from sub_tasks.data.connect import engine
from pangres import upsert
import pandas as pd
import pygsheets
import sys
sys.path.append(".")


def fetch_draft_drop():
    gc = pygsheets.authorize(
        service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json'
    )
    sh = gc.open_by_key('1HQ_y1omRXpV_KxDuUK2kEbgIHmVeIxqM1LxxHH8up4o')
    sh = pd.DataFrame(sh.worksheet_by_title("Kenya").get_all_records()).fillna("NAN")
    sh = sh.drop_duplicates(subset=["Order Number"])
    sh.rename(columns={
        'Date': 'date',
        'Branch Code': 'branch',
        'Order Number': 'order_number',
        'Order Creator': 'order_creator',
        'Reason for the Delay': 'delay_reason',
        'Was the Issue Resolved?': 'issue_resolved',
        'Additional Remarks': 'additional_remarks',
        'Glazing':'glazing'
    }, inplace=True)

    sh = sh.set_index(['order_number'])

    upsert(engine=engine,
           df=sh,
           schema='mabawa_dw',
           table_name='dim_draft_drop',
           if_row_exists='update',
           create_table=False)

# fetch_draft_drop()
