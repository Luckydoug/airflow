import sys
sys.path.append(".")
import psycopg2
import requests
import pandas as pd
from pangres import upsert
from sub_tasks.data.connect import engine
from datetime import date, timedelta, time, datetime
from sub_tasks.libraries.utils import flatten_dict

today = date.today()
pastdate = today - timedelta(days=2)
datefrom = pastdate.strftime('%Y-%m-%d')
dateto = date.today().strftime('%Y-%m-%d')
# datefrom = '2024-01-01'
# dateto = '2024-05-07'

def fetch_token():

    refresh_token_url = "https://accounts.zoho.com/oauth/v2/token"
    params = {
        "refresh_token": "1000.0a5036d9ddf7e4e243c3651161b830e6.3b809495570a1d9a53398a0179381a73",
        "client_id": "1000.QRT7VH36RSOKKPZOCDHJTTYB169WBF",
        "client_secret": "fad2d176b51219feb9a0eb2d144fca2e6348c07a13",
        "redirect_uri": "https://support.optica.africa/agent/opticalimited/optica-kenya",
        "grant_type": "refresh_token"
    }

    response = requests.post(refresh_token_url, params=params)
    token_content = response.json()
    token = token_content['access_token']

    return token

def fetch_survey_tickets():

    token = fetch_token()

    headers = {
    "Authorization": f"Bearer {token}"
    }

    url_tckt_cnt = "https://desk.zoho.com/api/v1/ticketsCountByFieldValues"

    params_tckt_cnt = {
            "field": "channel",
            "modifiedTimeRange": f"{datefrom}T00:00:00.000Z,{dateto}T23:59:00.000Z"
        }

    response_tckt_cnt = requests.get(url_tckt_cnt, params=params_tckt_cnt, headers=headers)
    result_tckt_cnt = response_tckt_cnt.json()

    chnnl_tckt_cnt = {entry['value']: int(entry['count']) for entry in result_tckt_cnt['channel']}

    tckt_cnt = chnnl_tckt_cnt.get("rms") + chnnl_tckt_cnt.get("ajua")

    tickets = []

    for number in range(0, tckt_cnt, 100):

        limit = min(99, tckt_cnt - number)

        url = "https://desk.zoho.com/api/v1/tickets/search"

        params = {
                "from":number,
                "limit":limit,
                "channel": ["rms","ajua"],
                "modifiedTimeRange": f"{datefrom}T00:00:00.000Z,{dateto}T23:59:00.000Z"
            }
        
        try:
            response = requests.get(url, params=params, headers=headers)
            result = response.json()
            tickets += result['data']

        except:
            pass
    
    data = [flatten_dict(ticket) for ticket in tickets]

    keys_to_include = ['ticketNumber','createdTime','modifiedTime','statusType','status','classification','firstName','lastName',
                    'NPS Score','RMS Customer Feedback','Branch','Branch Comments','Order Number','Loyalty Code',
                    'Procedure']

    ls = [{key: entry[key] for key in keys_to_include} for entry in data]

    df = pd.DataFrame(ls)

    df.rename(columns=lambda x: x.lower().replace(' ', '_'),inplace=True)

    df.set_index('ticketnumber',inplace=True)

    upsert(engine=engine,
    df=df,
    schema='mabawa_staging',
    table_name='source_survey_tickets',
    if_row_exists='update',
    create_table=True)


def fetch_insurance_tracking():

    token = fetch_token()

    headers = {
    "Authorization": f"Bearer {token}"
    }

    url = "https://desk.zoho.com/api/v1/tickets/search"

    params = {
            "modifiedTimeRange": f"{datefrom}T00:00:00.000Z,{dateto}T23:59:00.000Z",
            "subject":"Insurance Tracking*"
        }
    
    response = requests.get(url, params=params, headers=headers)
    result = response.json()

    tckt_cnt = result['count']

    tickets = []

    for number in range(0, tckt_cnt, 100):

        limit = min(99, tckt_cnt - number)

        url = "https://desk.zoho.com/api/v1/tickets/search"

        params = {
                "from":number,
                "limit":limit,
                "modifiedTimeRange": f"{datefrom}T00:00:00.000Z,{dateto}T23:59:00.000Z",
                "subject":"Insurance Tracking*"
            }
        
        try:
            response = requests.get(url, params=params, headers=headers)
            result = response.json()
            tickets += result['data']

        except:
            pass
    
    data = [flatten_dict(ticket) for ticket in tickets]
    keys_to_include = ['ticketNumber','subject','Order Number','status','Client Feedback','Our Offer','classification']
    ls = [{key: entry[key] for key in keys_to_include} for entry in data]
    df = pd.DataFrame(ls)

    df.rename(columns=lambda x: x.lower().replace(' ', '_'),inplace=True)

    df.drop_duplicates('ticketnumber',inplace=True)

    df.set_index('ticketnumber',inplace=True)

    upsert(engine=engine,
    df=df,
    schema='mabawa_staging',
    table_name='source_insurance_tracking_tickets',
    if_row_exists='update',
    create_table=True)