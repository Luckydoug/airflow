import sys
sys.path.append(".")
from sub_tasks.data.connect import pg_execute

def refresh_order_contents():
    
    query = """
    refresh materialized view mabawa_mviews.order_contents;
    """
    query = pg_execute(query)

def refresh_fronly_orders():
    
    query = """
    refresh materialized view mabawa_mviews.fronly_orders;
    """
    query = pg_execute(query)