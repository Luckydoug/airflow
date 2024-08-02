from airflow.models import variable
import pandas as pd
import requests
import re 
import datetime
from sub_tasks.libraries.utils import createe_engine
import os
from pangres import upsert
from sub_tasks.libraries.utils import return_session_id
from reports.draft_to_upload.utils.utils import today
from sub_tasks.libraries.utils import first_week_start


"""
Non-Converted Insurance Approved orders, i.e.,
i) Insurance Fully Approved,
ii) Insurance Partially Approved, and
iii) Use Available Amount on SMART,
are uploaded manually to Zoho Desk as tickets. 
Of course, if it's a ticket, then it must be closed. 
Customer Service agents have to manually look on the web app to see if the order has converted before they follow up. 
This process takes much time.

To solve this problem, 
Douglas created this script that will help update tickets automatically only if they have converted using Zoho Desk API.
The script begins by fetching the token from Zoho Desk API. After obtaining the token, we fetch all tickets from 4 weeks ago. 
We extract the order number from the subject using regex. After getting the order number, 
we fetch data from the data warehouse and return only those orders that have converted. 
After this, we update the tickets and make a record of the updated tickets in the data warehouse for future reference.


DOCUMENTATION V 0.01

Written and Curated by D.O.U.G.L.A.S
"""




def get_token():
    engine = createe_engine()
    refresh_token_url = "https://accounts.zoho.com/oauth/v2/token"
    params = {
        "refresh_token": os.getenv("zoho_refresh_token"),
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

    upsert(
        engine=engine, 
        df=df,
        schema='mabawa_staging',
        table_name='api_login',
        if_row_exists='update',
        create_table=False
    )
    

def get_ticket_count():
    url = "https://desk.zoho.com/api/v1/ticketsCountByFieldValues"
    token = return_session_id(country="Zoho")
    params = {
        "field": "status",
        "modifiedTimeRange": f"{first_week_start}T00:00:00.000Z,{today}T23:59:00.000Z"
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
    lists = list(range(0, total_count, 100))

    for number in lists:
        limit = min(99, total_count - number)
        url = "https://desk.zoho.com/api/v1/tickets/search"
        params = {
            "from": number,
            "limit": limit,
            "status": status,
            "modifiedTimeRange": f"{first_week_start}T00:00:00.000Z,{today}T23:59:00.000Z"
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
    all_tickets = all_tickets[all_tickets["layoutId"] == '563504000000074011']
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

def get_tickets_to_update():
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

    converted_tickets = converted_tickets.rename(
        columns={
            "modifiedTime": "modified_time",
            "statusType": "status_type",
            "order number": "order_number",
        }
    )

    converted_tickets = converted_tickets.drop(columns=["converted"])
    converted_tickets = converted_tickets.set_index("id")

    upsert(
        engine=engine, 
        df=converted_tickets,
        schema='reports_tables',
        table_name='zoho_tickets_updates',
        if_row_exists='update',
        create_table=False
    )

def update_zoho_tickets():
    engine = createe_engine()

    query = """
    select id, modified_time as "modifiedTime",
    status_type as "statusType", subject,
    classification, status, order_number,
    converted_order,
    updated_timestamp
    from reports_tables.zoho_tickets_updates
    where ticket_url is null;
    """

    zoho_updates = pd.read_sql_query(query, con=engine)
    new_updates = pd.DataFrame()
    

    if zoho_updates.empty:
        return

    for index, row in zoho_updates.iterrows():
        activities_count = 0
        update_url = f"https://desk.zoho.com/api/v1/tickets/{row['id']}"
        activities_url = f"https://desk.zoho.com/api/v1/tickets/{row['id']}/activities"

        token = return_session_id("Zoho")

        headers = {
            'Authorization': f"Bearer {token}"
        }

        try:
            activities_ids = []
            activities_response = requests.get(activities_url, headers=headers)
            if activities_response.status_code == 204:
                reason = "Converted before follow up"
            else:
                activities_response.raise_for_status()
                activities_data = activities_response.json()
                if activities_data.get("data"): 
                    reason = "Converted After Follow up"
                    for activity in activities_data.get("data"):
                        activities_ids.append(activity["id"])

                else:
                    reason = "Converted before follow up"

        except requests.exceptions.RequestException as e:
            print(f"Encounted error when trying to fetch activities for ticket {row['id']}")
            return

        updates = {
            "status": "Closed",
            "customFields": {
                "Reason For Not Converting": reason,
                "Converted Order Number": row['converted_order'],
                "Insurance Converted": "Yes"
            }
        }

        try:
            response = requests.patch(update_url, headers=headers, json=updates)
            response.raise_for_status()
            result = response.json()

            if len(activities_ids):
                activities_count = len(activities_ids)
                update_tasks_url = "https://desk.zoho.com/api/v1/tasks/updateMany"

                tasks_updates = {
                    "ids": activities_ids,
                    "fieldName": "status",
                    "fieldValue": "Completed",
                    "isCustomField": False,
                }

                task_update = requests.post(update_tasks_url, headers=headers, json=tasks_updates)
                task_update.raise_for_status()
                reslt = task_update.json()
          
            new_url = f"https://support.optica.africa/agent/opticalimited/optica-kenya/tickets/details/{row['id']}"
            
            updated_row = row.copy()
            updated_row['ticket_url'] = new_url
            updated_row['activities_count'] = activities_count
            updated_row = updated_row.rename(
                {
                    "modifiedTime": "modified_time",
                    "statusType": "status_type"
                },
                axis='columns'
            )
        
            current_time = datetime.datetime.now()
            updated_timestamp = current_time + datetime.timedelta(hours=3)
            updated_row['updated_timestamp'] = updated_timestamp
            new_updates = new_updates.append(updated_row)

            print("Updated")
        
        except requests.exceptions.RequestException as e:
            print("Error updating ticket: ", e)

    
    upsert(
        engine=engine, 
        df=new_updates.set_index("id"),
        schema='reports_tables',
        table_name='zoho_tickets_updates',
        if_row_exists='update',
        create_table=False
    )


      
            
def truncate_zoho_updates():
    pass

# get_token()
# update_zoho_tickets()

































      
        
















































    

