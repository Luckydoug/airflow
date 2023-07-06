import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pygsheets
import pandas as pd
from airflow.models import Variable
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 
from datetime import date


from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine) 


def fetch_branchstock_cutoffs():
    
     gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
     sh = gc.open_by_key('1Cu1ArrK0vZLtMUuZvXjSpkfF6gT2MONjVsnpXQydRCI')
     wk1 = sh[0]
     wk1 = pd.DataFrame(wk1.get_all_records())
     wk1.rename (columns = {
                              'Branch':'branch_code', 
                              'Warehouse Name':'whse_name', 
                              'Address':'address', 
                              'Type':'cutoff_type',
                              'BRS Cut':'brs_cut',
                              'Lens Store Cut':'lens_store_cut',
                              'Designer Store Cut':'designer_store_cut',
                              'Main Store Cut':'main_store_cut',
                              'Control Cut':'control_cut',
                              'Packaging Cut':'packaging_cut'
                              }
            ,inplace=True)
     wk1.loc[wk1["whse_name"] == "Nanyuki", "branch_code"] = 'NAN'
     #rslt_df = wk1[wk1['whse_name'] == 'Nanyuki']
     #print(rslt_df)

     #wk1 = wk1.set_index(['branch_code','cutoff_type'])

     query = """truncate mabawa_staging.source_branchstock_cutoffs;"""
     query = pg_execute(query)
     
     wk1.to_sql('source_branchstock_cutoffs', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)

     return 'something' 

def create_branchstock_cutoffs():

  query="""
  truncate mabawa_dw.dim_branchstock_cutoffs;
  insert into mabawa_dw.dim_branchstock_cutoffs
  SELECT 
      branch_code, whse_name, 
      brs_first_cut, brs_second_cut, 
      lens_store_first_cut, lens_store_second_cut, 
      designer_store_first_cut, designer_store_second_cut,
      main_store_first_cut, main_store_second_cut, 
      control_first_cut, control_second_cut,
      packaging_first_cut, packaging_second_cut,
      (case when brs_first_cut = brs_second_cut
      then 'ONE CUTOFF'
      else 'TWO CUTOFF' end) as brs_cutoffs,
      (case when lens_store_first_cut = lens_store_second_cut
      then 'ONE CUTOFF'
      else 'TWO CUTOFF' end) as lens_store_cutoffs,
      (case when designer_store_first_cut = designer_store_second_cut
      then 'ONE CUTOFF'
      else 'TWO CUTOFF' end) as designer_store_cutoffs,
      (case when main_store_first_cut = main_store_first_cut
      then 'ONE CUTOFF'
      else 'TWO CUTOFF' end) as main_store_cutoffs,
      (case when control_first_cut = control_first_cut
      then 'ONE CUTOFF'
      else 'TWO CUTOFF' end) as control_store_cutoffs,
      (case when packaging_first_cut = packaging_first_cut
      then 'ONE CUTOFF'
      else 'TWO CUTOFF' end) as packaging_store_cutoffs
  FROM mabawa_mviews.v_dim_branchstock_cutoffs
  """
  query=pg_execute(query)
  
  return "Created Dim Table"