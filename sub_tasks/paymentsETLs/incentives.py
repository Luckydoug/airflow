import sys
sys.path.append(".")
import psycopg2
from airflow.models import Variable
from sub_tasks.data.connect import pg_execute
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def create_incentive_cash():

    query = """
    refresh materialized view mabawa_mviews.m_cash_payment_details;
    refresh materialized view mabawa_mviews.m_cash_or_insurance_order;
    refresh materialized view mabawa_mviews.m_ojdt_details;
    refresh materialized view mabawa_mviews.m_discount_details;
    refresh materialized view mabawa_mviews.incentive_cash; 
    """

    query = pg_execute(query)
    print('cash incentive done')

def create_incentive_insurance():
    
    query = """
    update mabawa_staging.source_orders_header
    set creation_date = '2024-02-29'
    where draft_orderno = '245502222';
    refresh materialized view mabawa_mviews.incentive_insurance2;
    insert into mabawa_dw.update_log(table_name, update_time) values('incentives', default);
    """

    query = pg_execute(query)
    print('insurance incentive done')
    
   