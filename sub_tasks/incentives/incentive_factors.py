import sys
sys.path.append(".")
from airflow.models import Variable
from sub_tasks.data.connect import pg_execute


def refresh_order_contents():
    
    query = """
    refresh materialized view mabawa_mviews.order_contents;
    """
    query = pg_execute(query)

def refresh_all_activity():
    
    query = """
    refresh materialized view mabawa_mviews.all_activity;
    """
    query = pg_execute(query)

def refresh_lens_silh():

    query = """
    refresh materialized view mabawa_mviews.lens_incentive; 
    refresh materialized view mabawa_mviews.silh_incentive;
    """

    query = pg_execute(query)
    print('lens_silh done')

def refresh_insurance_rejections():
    query = """
    refresh materialized view mabawa_mviews.insurance_rejections; 
    refresh materialized view mabawa_mviews.insurance_rejections_summary;
    """

    query = pg_execute(query)
    print('refresh_insurance_rejections done')


def refresh_sunglass_sales_summary():
    query = """
    refresh materialized view mabawa_mviews.sunglass_sales_summary; 
    """

    query = pg_execute(query)
    print('refresh_sunglass_sales_summary done')

def refresh_google_reviews_summary():
    query = """
    refresh materialized view mabawa_mviews.google_reviews_summary; 
    """

    query = pg_execute(query)
    print('refresh_google_reviews_summary done')

def refresh_insurance_feedback_conversion():
    query = """
    refresh materialized view mabawa_mviews.insurance_feedback_conversion; 
    """

    query = pg_execute(query)
    print('refresh_insurance_feedback_conversion done')

def refresh_sop():
    query = """
    refresh materialized view mabawa_mviews.sop; 
    refresh materialized view mabawa_mviews.sop_summary; 
    """

    query = pg_execute(query)
    print('refresh_sop done')

def refresh_nps_summary():
    query = """
    refresh materialized view mabawa_mviews.nps_surveys;
    refresh materialized view mabawa_mviews.nps_summary; 
    """

    query = pg_execute(query)
    print('refresh_nps_summary done')




