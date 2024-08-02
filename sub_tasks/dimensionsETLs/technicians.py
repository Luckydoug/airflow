import sys

sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from sub_tasks.data.connect import pg_execute, engine
# from sub_tasks.api_login.api_login import login
from sub_tasks.libraries.utils import return_session_id


def fetch_sap_technicians():
    SessionId = return_session_id(country="Kenya")
    # SessionId = login()

    technician_url = f"https://10.40.16.9:4300/OpticaBI/XSJS/BI_API.xsjs?pageType=GetTechniciansList&Type=All&SessionId={SessionId}"

    response = requests.get(technician_url, verify=False)
    technicians = response.json()
    stripped_technicians = technicians["result"]["body"]["recs"]["Results"]
    technicians_df = pd.DataFrame.from_dict(stripped_technicians)

    technicians_df2 = technicians_df.T

    technicians_df2.rename(
        columns={
            "Code": "technician_code",
            "Name": "technician_name",
            "TYPE": "technician_type",
            "Active": "technician_active_status",
        },
        inplace=True,
    )

    query = """truncate mabawa_staging.source_technicians;"""
    query = pg_execute(query)

    technicians_df2.to_sql(
        "source_technicians",
        con=engine,
        schema="mabawa_staging",
        if_exists="append",
        index=False,
    )
