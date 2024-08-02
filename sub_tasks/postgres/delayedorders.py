import sys
sys.path.append(".")
from sub_tasks.data.connect import pg_execute

def refresh_delayedorders():
    
    query = """
    refresh materialized view mabawa_mviews.delayedorders;
    """
    query = pg_execute(query)

