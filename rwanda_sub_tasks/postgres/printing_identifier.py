import sys
sys.path.append(".")
import numpy as np
import pandas as pd
import holidays as pyholidays
from datetime import date, timedelta, datetime, time
from pangres import upsert
from sub_tasks.data.connect_voler import (pg_execute,engine)
from sub_tasks.libraries.utils import (calculate_time_taken,calculate_time_taken_for_row,today,pastdate)


def update_printing_identifier():

    df_q = """  
    select doc_entry, doc_no, identifier, ods_outlet, ods_creator, user_name, sales_order_created, printed_identifier, ods_insurance_order
    from voler_mviews.v_printing_identifier_efficiency
    where printed_identifier >= current_date - interval '7 days'
    and printed_identifier <= current_date
    """

    df = pd.read_sql(df_q,con=engine)

    df = df[df['ods_outlet'].isna()!=True]
    
    wrkng_hrs_q = """    
    SELECT warehouse_code, warehouse_name, docnum, days, start_time, end_time, auto_time
    FROM reports_tables.working_hours
    """

    wrkng_hrs = pd.read_sql(wrkng_hrs_q,engine)

    branch_data = {}

    for _, row in wrkng_hrs.iterrows():
        branch = row['warehouse_code']
        day = row['days']
        start_time = row['start_time']
        end_time = row['end_time']

        start_time = datetime.strptime(start_time,'%H:%M').time()
        end_time = datetime.strptime(end_time,'%H:%M').time()

        if branch not in branch_data:

            branch_data[branch] = {
                'working_hours': {},
                }
    
        branch_data[branch]['working_hours'][day] = (start_time, end_time)
    
    ke_holidays = pyholidays.KE()

    df['time_taken'] = df.apply(lambda row: calculate_time_taken_for_row(row, 'ods_outlet', 'sales_order_created', 'printed_identifier', branch_data, ke_holidays), axis=1)
    df['time_taken'] = np.where(df['time_taken']<0,0,df['time_taken'])
    df.set_index('doc_entry',inplace=True)

    upsert(engine=engine,
    df=df,
    schema='voler_mviews',
    table_name='printing_identifier_efficiency',
    if_row_exists='update',
    create_table=False)

# update_printing_identifier()