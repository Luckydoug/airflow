import pandas as pd
from airflow.models import variable
from reports.draft_to_upload.utils.utils import today

class FecthData:
    def __init__(self, engine) -> None:
        self.engine = engine

    def fetch_data(self, query) -> pd.DataFrame:
        with self.engine.connect() as connection:
            try:
                data = pd.read_sql_query(query, con=connection)
                return data
            except Exception as e:
                raise e

    def fetch_queue_data(self, start_date, mview) -> pd.DataFrame:
        query = f"""
        select
        qt.visit_id as "Visit ID",
        qt.et_start::date as "CreateDate",
        trim(to_char(qt.et_start::date, 'Month')) as "Month",
        qt.outlet_id as "Branch",
        qt.cust_id as "Customer Code",
        qt.optom_id as "Optom",
        qt.optom_name as "Optom Name",
        qt.added_to_queue2 as "AddedQueueTime",
        qt.eyetest_complete as "Completed Time",
        qt.queue_time2 as "Queue Time",
        qt.eye_testtime as "Eye Test Time",
        case when ets.days is not null then 'Yes' else 'No' end as "Converted",
        ets.mode_of_pay as "Customer Type"
        from {mview}.eyetest_queue_time qt 
        left join {mview}.et_conv as ets
        on qt.visit_id::text = ets.code::text
        where qt.et_start::date between '{start_date}' and '{today}'
        and qt.outlet_id <> 'nan'
        and qt.outlet_id is not null
        and qt.outlet_id <> 'null'
        and qt.outlet_id not in ('HOM')
        """

        queue_data = self.fetch_data(query)
        queue_data["CreateDate"] = pd.to_datetime(queue_data["CreateDate"], format="%Y-%m-%d").dt.date

        return queue_data
    
    def fetch_branch_data(self, schema):
        query = f"""
        select branch_code as "Outlet",
        branch_name as "Branch",
        email as "Email",
        rm as "RM",
        rm_email as "RM Email",
        rm_group as "RM Group",
        srm as "SRM",
        srm_email as "SRM Email",
        branch_manager as "Branch Manager",
        front_desk as "Front Desk",
        zone as "Zone"
        from {schema}.branch_data bd 
        """

        branch_data = self.fetch_data(query)
        return branch_data
