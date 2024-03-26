import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow



def credentials():

    SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

    creds = None

    if os.path.exists(r"/home/opticabi/airflow/dags/sub_tasks/emails/token.json"):
        creds = Credentials.from_authorized_user_file(r"/home/opticabi/airflow/dags/sub_tasks/emails/token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                r"/home/opticabi/airflow/dags/sub_tasks/emails/client_secret_716541083472-hbsho22f0htsf760lglsgotq47ecccks.apps.googleusercontent.com.json", 
                SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    
    return creds

