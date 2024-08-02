import sys
sys.path.append(".")

import pandas as pd
from pangres import upsert
from sub_tasks.data.connect import (engine) 


def fetch_old_clients_followup():
        
        sheet_id = '17O1okXKSsWvFecqmoBaur1EMwn4SiEL_R_7nGTpBaZU'
        sheet_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv'

        df = pd.read_csv(sheet_url)

        df.rename({
                'Last ET':'last_et',
                'ET Date':'et_date',
                'Customer Code':'customer_code',
                'Branch':'branch', 
                'Optom':'optom',
                'Last ON':'last_on',
                'ON Date':'on_date',
                'ON Branch':'on_branch',
                'Date Of Call':'date_of_call',
                'Call Status':'call_status',
                'Appointment Date':'appointment_date',
                'Remarks (Give Details Of The Conversation)':'remarks'
                },axis=1,inplace=True)
        
        for col in ['last_et','customer_code','last_on']:
                df[col] = df[col].map(lambda x: str(x).rstrip('0').rstrip('.') if '.' in str(x) else str(x))
        
        df = df[df['last_et'].duplicated(keep=False)==False]

        df.set_index('last_et',inplace=True)

        upsert(engine=engine,
                df=df,
                schema='mabawa_staging',
                table_name='source_old_clients',
                if_row_exists='update',
                create_table=True)

# fetch_old_clients_followup()
