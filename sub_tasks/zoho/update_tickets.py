from airflow.models import variable
import pandas as pd
import datetime
import requests
import re 
from sqlalchemy import create_engine
import urllib
from sub_tasks.libraries.utils import createe_engine
from pangres import upsert
from sub_tasks.libraries.utils import return_session_id


def get_token():
    engine = createe_engine()
    refresh_token_url = "https://accounts.zoho.com/oauth/v2/token"
    params = {
        "refresh_token": "1000.8edfc26f1b4520b12f59020e70ab0e38.bb0876fa1d26651f4283d281f406016d",
        "client_id": "1000.QRT7VH36RSOKKPZOCDHJTTYB169WBF",
        "client_secret": "fad2d176b51219feb9a0eb2d144fca2e6348c07a13",
        "redirect_uri": "https://support.optica.africa/agent/opticalimited/optica-kenya",
        "grant_type": "refresh_token"
    }

    response = requests.post(refresh_token_url, params=params)
    token_content = response.json()
    token = token_content['access_token']

    df = pd.DataFrame({"country": 'Zoho', "session_id": token}, index=[0])
    df = df.set_index("country")

    upsert(engine=engine,
            df=df,
            schema='mabawa_staging',
            table_name='api_login',
            if_row_exists='update',
            create_table=False)
    

def get_ticket_count():
    url = "https://desk.zoho.com/api/v1/ticketsCountByFieldValues"
    yesterday_date = "2024-03-01"
    today = "2024-03-15"
    token = return_session_id(country="Zoho")
    params = {
        "field": "status",
        "modifiedTimeRange": f"{yesterday_date}T00:00:00.000Z,{today}T23:59:00.000Z"
    }

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    result = response.json()

    status_counts = {}

    for status_entry in result.get("status", []):
        status_value = status_entry.get('value')
        status_count = status_entry.get('count', 0)
        status_counts[status_value] = status_count

    tracking = status_counts.get('followup tracking', 0)
    noreply = status_counts.get("no reply required", 0)

    return int(tracking), int(noreply)

def fetch_tickets(status, total_count, token):
    tickets = []
    yesterday_date = "2024-03-01"
    today = "2024-03-15"
    lists = list(range(0, total_count, 100))
    print(lists)

    for number in lists:
        limit = min(99, total_count - number)
        url = "https://desk.zoho.com/api/v1/tickets/search"
        params = {
            "from": number,
            "limit": limit,
            "status": status,
            "modifiedTimeRange": f"{yesterday_date}T00:00:00.000Z,{today}T23:59:00.000Z"
        }
        headers = {
            'Authorization': f"Bearer {token}"
        }

        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            print(response)
            result = response.json()
            tickets.extend(result.get("data", []))
        except requests.exceptions.RequestException as e:
            pass
    
    all_tickets = pd.DataFrame(tickets)
    all_tickets = all_tickets[all_tickets["classification"] == "Insurance Tracking"]
    return all_tickets[[
        "id",
        "modifiedTime",
        "statusType",
        "subject",
        "classification",
        "status"
    ]]

def extract_numeric(text):
    numeric_value = re.search(r'\d+', text)
    if numeric_value:
        return numeric_value.group()
    else:
        return None

def update_tickets():
    engine = createe_engine()
    token = return_session_id("Zoho")
    tracking_count, noreply_count = get_ticket_count()
    
    noreply_tickets = fetch_tickets(
        status="no reply required",
        total_count=noreply_count,
        token=token
    )

    tracking_tickets = fetch_tickets(
        status="followup tracking",
        total_count=tracking_count,
        token=token
    )

    insurance_tracking_tickets = pd.concat([noreply_tickets, tracking_tickets], ignore_index=True)
    insurance_tracking_tickets["order number"] = insurance_tracking_tickets['subject'].apply(extract_numeric)

    order_numbers = ','.join(map(lambda x: f"'{x}'", set(insurance_tracking_tickets["order number"])))

    query = f"""
    select 
    doc_no as "order number",
    case when cnvrtd = 1 and sm_cnvrtd = 1 then doc_no
    when cnvrtd = 1 and sm_cnvrtd = 0 then dff_draft_orderno
    else null end as "converted_order",
    cnvrtd as "converted"
    from mabawa_mviews.insurance_feedback_conversion ifc 
    where doc_no::text in ({order_numbers})
    """

    orders = pd.read_sql_query(query, con = engine)

    insurance_tracking = pd.merge(insurance_tracking_tickets, orders, on = "order number", how = "left")
    converted_tickets = insurance_tracking[insurance_tracking["converted"] == 1]

    return converted_tickets






    

