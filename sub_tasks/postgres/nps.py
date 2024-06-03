import sys
sys.path.append(".")
from sub_tasks.data.connect import pg_execute

def refresh_nps():

    query = """
    refresh materialized view mabawa_mviews.nps_surveys;
    refresh materialized view mabawa_mviews.nps_summary; 
    """

    query = pg_execute(query)