import pandas as pd

from pangres import upsert
from sub_tasks.data.connect import engine
from sub_tasks.libraries.utils import FromDate, ToDate
from sub_tasks.emails.preauth_creds import credentials
from googleapiclient.discovery import build

FromDate = '2024/06/01'
ToDate = '2024/07/15'

def fetch_preauth_requests():

    creds = credentials()

    query = f'after:{FromDate} before:{ToDate}'

    service = build("gmail", "v1", credentials=creds)

    results = service.users().messages().list(userId="me",q=query).execute()

    messages = results['messages']

    df = pd.DataFrame()

    for mssg in messages:
        
        temp_dict = { }

        msg = service.users().messages().get(userId='me', id=mssg['id']).execute()

        temp_dict['id'] = msg['id']
        temp_dict['threadId'] = msg['threadId']
        temp_dict['snippet'] = msg['snippet']

        msg_headers = msg['payload']['headers']

        for headr in msg_headers:

            if headr['name'] == 'From':
                temp_dict['from'] = headr['value']
            
            elif headr['name'] == 'To':
                temp_dict['to'] = headr['value']
            
            elif headr['name'] == 'CC':
                temp_dict['cc'] = headr['value']

            elif headr['name'] == 'Date':
                temp_dict['date'] = headr['value']
            
            elif headr['name'] == 'Subject':
                temp_dict['subject'] = headr['value']
            
            else:
                pass

        temp_df = pd.DataFrame.from_dict(temp_dict,orient='index').T
        
        df = pd.concat([df,temp_df], ignore_index=True)

        nextPageToken = results.get('nextPageToken', None)
        prevPageToken = None

        while nextPageToken and nextPageToken != prevPageToken:

            results = service.users().messages().list(userId="me",q=query,pageToken=nextPageToken).execute()

            prevPageToken = nextPageToken

            messages = results['messages']
            
            for mssg in messages:
            
                temp_dict = { }

                msg = service.users().messages().get(userId='me', id=mssg['id']).execute()

                temp_dict['id'] = msg['id']
                temp_dict['threadId'] = msg['threadId']
                temp_dict['snippet'] = msg['snippet']

                msg_headers = msg['payload']['headers']

                for headr in msg_headers:

                    if headr['name'] == 'From':
                        temp_dict['from'] = headr['value']
                    
                    elif headr['name'] == 'To':
                        temp_dict['to'] = headr['value']
                    
                    elif headr['name'] == 'CC':
                        temp_dict['cc'] = headr['value']

                    elif headr['name'] == 'Date':
                        temp_dict['date'] = headr['value']
                    
                    elif headr['name'] == 'Subject':
                        temp_dict['subject'] = headr['value']
                    
                    else:
                        pass

                temp_df = pd.DataFrame.from_dict(temp_dict,orient='index').T
                
                df = pd.concat([df,temp_df], ignore_index=True)
                
            nextPageToken = results.get('nextPageToken', None)  

    df.set_index(['id'],inplace=True)

    upsert(engine=engine,
    df=df,
    schema='mabawa_staging',
    table_name='source_preauth_requests',
    if_row_exists='update',
    create_table=True)

# fetch_preauth_requests()