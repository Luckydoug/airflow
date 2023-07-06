import pandas as pd
import requests
import datetime
from datetime import timedelta
from sub_tasks.data.connect import (engine)
from pangres import upsert

refresh_token_url = "https://accounts.zoho.com/oauth/v2/token"
params = {
    "refresh_token": "1000.d53b23b7a82fc1fb3f557a5102cad4f9.2f807bfefbc8b9a0a2577be2e339ce02",
    "client_id": "1000.QRT7VH36RSOKKPZOCDHJTTYB169WBF",
    "client_secret": "fad2d176b51219feb9a0eb2d144fca2e6348c07a13",
    "scope": "Desk.tickets.READ,Desk.search.READ,Desk.activities.calls.READ,Desk.activities.events.READ,Desk.activities.READ,Desk.activities.tasks.READ",
    "redirect_uri": "https://support.optica.africa/agent/opticalimited/optica-kenya",
    "grant_type": "refresh_token"
}

try:
    response = requests.post(refresh_token_url, params=params)
    response.raise_for_status()
    token_content = response.json()
    token = token_content["access_token"]

except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")


today = datetime.date.today()
yesterday_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

def extract_agent_details(row):
    data_object = row["assignee"]
    if pd.isna(data_object):
        return "Unassigned", "Unassigned"
    else:
        return data_object["firstName"] + " " + data_object["lastName"], data_object["emailId"]
    
def extract_department(row):
    data_object = row["department"]
    if pd.isna(data_object):
        return None
    else:
        return data_object["name"]


def get_ticket_count():
    url = "https://desk.zoho.com/api/v1/ticketsCountByFieldValues"
    params = {
        "field": "statusType",
        "modifiedTimeRange": f"{yesterday_date}T00:00:00.000Z,{yesterday_date}T23:59:00.000Z"
    }
    headers = {
        'Authorization': f"Bearer {token}"
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        result = response.json()
        total = 0
        for i in range(len(result["statusType"])):
            total += int(result["statusType"][i]["count"])
        return total
    except requests.exceptions.RequestException as e:
        print("Error: ", e)
        return None


def fetch_zoho_tickets():
    total = get_ticket_count()
    lists = list(range(0, total, 100))
    tickets = []

    for number in lists:
        limit = 99
        if lists[-1] == number:
            limit = total - number
        url = "https://desk.zoho.com/api/v1/tickets/search"
        params = {
            "from": number,
            "limit": limit,
            "modifiedTimeRange": f"{yesterday_date}T00:00:00.000Z,{yesterday_date}T23:59:00.000Z"
        }
        headers = {
            'Authorization': f"Bearer {token}"
        }

        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            result = response.json()
            tickets.extend(result["data"])
        except requests.exceptions.RequestException as e:
            print("Error: ", e)

    return pd.DataFrame(tickets)


columns = [
    "ticketNumber",
    "createdTime",
    "status",
    "modifiedTime",
    "closedTime",
    "customerResponseTime",
    "assigned_agent",
    "agent_email",
    "id",
    "contactId",
    "statusType",
    "subject",
    "dueDate",
    "departmentId",
    "channel",
    "department",
    "onholdTime",
    "language",
    "threadCount",
    "priority",
    "classification",
    "phone",
    "isSpam",
    "createdBy",
    "isEscalated"
]


def fetch_closed_tickets():
    data = fetch_zoho_tickets()
    closed_tickets = data[
        (data["status"] == "Closed") |
        (data["status"] == "Locked")
    ].copy()

    closed_tickets["createdTime"] = pd.to_datetime(
        (closed_tickets["createdTime"].str.split("T").str[0]) + " " + (closed_tickets["createdTime"].str.split("T").str[1][:-1]))
    closed_tickets["modifiedTime"] = pd.to_datetime(
        (closed_tickets["modifiedTime"].str.split("T").str[0]) + " " + (closed_tickets["modifiedTime"].str.split("T").str[1][:-1]))
    closed_tickets["customerResponseTime"] = pd.to_datetime(
        (closed_tickets["customerResponseTime"].str.split("T").str[0]) + " " + (closed_tickets["customerResponseTime"].str.split("T").str[1][:-1]))
    closed_tickets["closedTime"] = pd.to_datetime(
        (closed_tickets["closedTime"].str.split("T").str[0]) + " " + (closed_tickets["closedTime"].str.split("T").str[1][:-1]))
    closed_tickets["dueDate"] = pd.to_datetime(
    (closed_tickets["dueDate"].str.split("T").str[0]) + " " + (closed_tickets["dueDate"].str.split("T").str[1][:-1]))
    closed_tickets = closed_tickets[closed_tickets["closedTime"].dt.date == closed_tickets["modifiedTime"].dt.date]

    if len(closed_tickets):
        closed_tickets[["assigned_agent", "agent_email"]] = closed_tickets.apply(
            lambda row: extract_agent_details(row), result_type="expand", axis=1)
        
        closed_tickets["department"] = closed_tickets.apply(lambda row: extract_department(row), axis=1)

        filtered_closed_tickets = closed_tickets[columns]
        closed_tickets_rename = filtered_closed_tickets.rename(columns={
            "ticketNumber": "ticket_number",
            "createdTime": "created_time",
            "modifiedTime": "modified_time",
            "closedTime": "closed_time",
            "customerResponseTime": "customer_response_time",
            "contactId": "contact_id",
            "statusType": "status_type",
            "dueDate": "due_date",
            "departmentId": "department_id",
            "onholdTime": "on_hold_time",
            "contactId": "contact_id",
            "threadCount": "thread_count",
            "isSpam": "is_spam",
            "createdBy": "created_by",
            "isEscalated": "is_escalated"
        })

        closed_tickets_rename["ticket_number"] = closed_tickets_rename["ticket_number"].astype(int)
        closed_tickets_rename["is_spam"] = closed_tickets_rename["is_spam"].astype(str).str.capitalize()
        closed_tickets_rename["is_escalated"] = closed_tickets_rename["is_escalated"].astype(str).str.capitalize()
        closed_tickets_rename = closed_tickets_rename.set_index("ticket_number")

        upsert(engine=engine,
            df=closed_tickets_rename,
            schema='mabawa_staging',
            table_name='source_closed_tickets',
            if_row_exists='update',
            create_table=True)
    else:
        return


