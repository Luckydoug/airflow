import sys
sys.path.append(".")
from sub_tasks.data.connect import pg_execute

def refresh_optom_queue_time():
    
    query = """
    refresh materialized view mabawa_mviews.eyetest_queue_time;
    """
    query = pg_execute(query)

def refresh_optom_queue_no_et():
    
    query = """
    refresh materialized view mabawa_mviews.optom_queue_no_et;
    """
    query = pg_execute(query)