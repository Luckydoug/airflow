import sys
sys.path.append(".")

import pandas as pd
import pygsheets
from sub_tasks.data.connect import (pg_execute,engine)

def refresh_old_clients_called():
    
    query = """
    refresh materialized view mabawa_mviews.old_clients_called;
    """
    query = pg_execute(query)

    q = """ 
    select 
        last_et, et_date, customer_code, branch, optom, last_on, on_date, on_branch, date_of_call, call_status, appointment_date, remarks, draft_orderno, creation_date, code, create_date, rtrn_date, rtrn, rtrn_cll
    from mabawa_mviews.old_clients_called;
    """

    df = pd.read_sql(q,con=engine).fillna('')

    gc = pygsheets.authorize(service_file=r"/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json")
    sh = gc.open_by_key('17O1okXKSsWvFecqmoBaur1EMwn4SiEL_R_7nGTpBaZU')
    worksheet = sh.worksheet_by_title("OUTPUT")

    worksheet.set_dataframe(df,start='A1')
    
