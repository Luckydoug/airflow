import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pandas as pd
from pandas.io.json._normalize import nested_to_record 
from sqlalchemy import create_engine
from airflow.models import Variable
from pandas.io.json._normalize import nested_to_record 
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from datetime import date
import datetime
import pytz
import businesstimedelta
import pandas as pd
import holidays as pyholidays
from workalendar.africa import Kenya
import pygsheets

from sub_tasks.data.connect_mawingu import (pg_execute, pg_fetch_all, engine) 
# from sub_tasks.api_login.api_login import(login)

def fetch_npsreviews_with_issues():
     gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
     sh = gc.open_by_key('1Wn7O54ohdn9z1YineGomEqIGCVw3GrzUSafOpYuIv_k')
     wk1 = sh[1]
     wk1 = pd.DataFrame(wk1.get_all_records())

     wk1.rename (columns = {'User ID':'user_id', 
                        'Date':'date',
                        'Name':'name',
                       'Order Number':'order_no', 
                       'Branch':'branch', 
                       'Rating':'rating',
                       'Comment why not to drop':'comment_why_not_to_drop',
                       'Rating After CS Call':'rating_after_cscall',
                       'Verification whether the entry should actually be dropped:yes/no':'dropped:yes/no',
                       'Source':'source'}
            ,inplace=True)

     print('renamed successfully')

     truncate_table = """truncate mawingu_staging.source_npsreviews_with_issues;"""
     truncate_table = pg_execute(truncate_table)

     wk1[['user_id','date','name','order_no','branch','rating','comment_why_not_to_drop','rating_after_cscall','dropped:yes/no','source']].to_sql('source_npsreviews_with_issues', con = engine, schema='mawingu_staging', if_exists = 'append', index=False)   
     
     print('orders with issues pulled')

def npsreviews_with_issues_live():

        orders = pd.read_sql("""
        SELECT 
        line_id,doc_entry,type,sap_internal_number,branch,survey_id,ajua_triggered,ajua_response,nps_rating,nps_rating_2,
        feedback,feedback2,triggered_time,trigger_date,long_feedback
        FROM mawingu_staging.source_ajua_info  
        """, con=engine)

        ordertodrop = pd.read_sql("""
        SELECT 
        order_no 
        FROM mawingu_staging.source_npsreviews_with_issues 
        """,con=engine)

        todroplist = ordertodrop["order_no"].to_list() 

        print('list of nps to drop')

        neworders = orders[~orders["sap_internal_number"].isin(todroplist)]

        # truncate_live = """truncate view if exists optica_training.ajua_detailed_tbl;"""
        # truncate_live = pg_execute(truncate_live)
        # print('ajua_detailed_tbl')

        # truncate_live = """truncate view if exists optica_training.ajua_info_rating_rmsrm_usercodes;"""
        # truncate_live = pg_execute(truncate_live)
        # print('ajua_info_rating_rmsrm_usercodes') 

        # truncate_live = """drop view if exists optica_training.ajua_info_rating_rmsrm;"""
        # truncate_live = pg_execute(truncate_live)
        # print('dropped ajua_info_rating_rmsrm')       


        truncate_live = """truncate table mawingu_dw.dim_ajua_info;"""
        truncate_live = pg_execute(truncate_live)


        neworders.to_sql('dim_ajua_info', con = engine, schema='mawingu_dw', if_exists = 'append', index=False)
    
        print('finished')

        return 'something'

# fetch_npsreviews_with_issues()
# npsreviews_with_issues_live()