import sys
import numpy as np
sys.path.append(".")

# Import Libraries
import json
import psycopg2
import requests
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import Variable
from airflow.exceptions import AirflowException
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
import mysql.connector as database
import urllib.parse
import time

# PG Execute(Query)
from sub_tasks.data.connect_voler import (pg_execute, pg_fetch_all, engine)  
# from sub_tasks.api_login.api_login import(login_rwanda)

conn = psycopg2.connect(host="10.40.16.19",database="voler", user="postgres", password="@Akb@rp@$$w0rtf31n")
# SessionId = login_rwanda()

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
start_month = datetime.date(today.year,today.month,1)
print(start_month,yesterday)

# today = datetime.date.today()
# yesterday = today - datetime.timedelta(days=1)
# start_month = datetime.date(yesterday.year,yesterday.month,1)
# print(start_month,yesterday)

def daily_net_payments_rwanda():
    payments = """
    SELECT doc_entry, doc_no, user_sign, createdon, createdat,case
        when length(createdat::text) in (1,2) then null
        else
            (left(createdat::text,(length(createdat::text)-2))||':'||right(createdat::text, 2))::time 
        end 
            as "Create_Time",
    createdby, cust_code, base_doc, order_no, full_doc_type, 
    mode_of_pay, total_advance_amt, advance_amt, remaining_amt, total_amt, status, branch_code, credit_card_name, 
    payment_method, mpesa_code, mpesa_date, mpesa_amt, insurance_company_name, insurance_scheme, insurance_membership_no, 
    principal_member_no, principal_member_name, web_payment_no, insurance_type, insurance_totl_apprv_amt, 
    insurance_frame_apprv_amt, insurance_lens_apprv_amt, insurance_act_apprv_amt, draft_orderno
    FROM voler_staging.source_payment
    where createdon::date = '{yesterday}'
    and status <> 'Cancel' ;

    """.format(yesterday = yesterday)
    payments = pd.read_sql_query(payments, con=conn)
    
    ###Select the required columns
    payments = payments[['branch_code','draft_orderno','createdon', 'Create_Time','full_doc_type','mode_of_pay','total_advance_amt',
    'advance_amt', 'remaining_amt']] 
    ###Rename columns
    payments = payments.rename(columns = {'branch_code':'Branch','draft_orderno':'Order No.','createdon':'Create_Date'})
    ##Create a new column and rename 
    payments["mode_of_pay1"] = payments.apply(lambda row: 'Insurance' if row['mode_of_pay'] == 'Insurance' else 'Cash/Mpesa/CC',axis =1)
    print(payments)

    payments['advance_amt'] = pd.to_numeric(payments['advance_amt'])    
    payment_summary = payments.pivot_table(index = 'Branch',columns = ['mode_of_pay1'],aggfunc = {'advance_amt':np.sum})
    payment_summary = payment_summary.droplevel([0],axis = 1)
    payment_summary = payment_summary.reset_index()
    payment_summary = payment_summary.fillna(0)
    
    ##Add the insurance column if not such payment was processed
    if 'Insurance' not in payment_summary.columns:
        payment_summary['Insurance'] = 0
    else:  
       payment_summary['Insurance']
    payment_summary['Total Cash'] = payment_summary['Cash/Mpesa/CC']
    payment_summary['Total Gross Amount'] = payment_summary['Cash/Mpesa/CC'] + payment_summary['Insurance']
    payment_summary['Total Cash Net Amount'] = payment_summary['Total Gross Amount'] * (100/118)
    payment_summary['Total Insurance Net Amount'] = payment_summary['Insurance'] * (100/118)

   #-------------------------------END OF DAILY------------------------------------

    paymentsmtd = """
    SELECT doc_entry, doc_no, user_sign, createdon, createdat,case
        when length(createdat::text) in (1,2) then null
        else
            (left(createdat::text,(length(createdat::text)-2))||':'||right(createdat::text, 2))::time 
        end 
            as "Create_Time",
    createdby, cust_code, base_doc, order_no, full_doc_type, 
    mode_of_pay, total_advance_amt, advance_amt, remaining_amt, total_amt, status, branch_code, credit_card_name, 
    payment_method, mpesa_code, mpesa_date, mpesa_amt, insurance_company_name, insurance_scheme, insurance_membership_no, 
    principal_member_no, principal_member_name, web_payment_no, insurance_type, insurance_totl_apprv_amt, 
    insurance_frame_apprv_amt, insurance_lens_apprv_amt, insurance_act_apprv_amt, draft_orderno
    FROM voler_staging.source_payment
    where createdon::date between '{start_month}' and '{yesterday}'
    and status <> 'Cancel';

    """.format(start_month = start_month,yesterday = yesterday)

    paymentsmtd = pd.read_sql_query(paymentsmtd, con=conn)

    print('get size')
    print(paymentsmtd.shape)
    ###Select the required columns
    paymentsmtd = paymentsmtd[['branch_code','draft_orderno','createdon', 'Create_Time','full_doc_type','mode_of_pay','total_advance_amt','advance_amt', 'remaining_amt']] 
    ###Rename columns
    paymentsmtd = paymentsmtd.rename(columns = {'branch_code':'Branch','draft_orderno':'Order No.','createdon':'Create_Date'})
    print(paymentsmtd)
    ###Create a new column and rename 
    paymentsmtd["mode_of_pay1"] = paymentsmtd.apply(lambda row: 'Insurance' if row['mode_of_pay'] == 'Insurance' else 'Cash/Mpesa/CC',axis =1)
    paymentsmtd['advance_amt'] = pd.to_numeric(paymentsmtd['advance_amt'])
  
    payment_summarymtd = paymentsmtd.pivot_table(index = 'Branch',columns = ['mode_of_pay1'],aggfunc = {'advance_amt':np.sum})
    payment_summarymtd = payment_summarymtd.droplevel([0],axis = 1)
    payment_summarymtd = payment_summarymtd.reset_index()
    payment_summarymtd = payment_summarymtd.fillna(0)

    ##Add Insurance if no transaction was made.
    if 'Insurance' not in payment_summarymtd.columns:
        payment_summarymtd['Insurance'] = 0
    else:  
        payment_summarymtd['Insurance']

    payment_summarymtd['Total Cash'] = payment_summarymtd['Cash/Mpesa/CC']
    payment_summarymtd['Total Gross Amount'] = payment_summarymtd['Cash/Mpesa/CC'] + payment_summarymtd['Insurance']
    payment_summarymtd['Total Cash Net Amount'] = payment_summarymtd['Total Cash'] * (100/118)
    payment_summarymtd['Total Insurance Net Amount'] = payment_summarymtd['Insurance'] * (100/118)
   
   #--------------------------------------END OF MTD DATA---------------------------------------------------------
   #Save in an excel file
    import xlsxwriter
    print(xlsxwriter.__version__)
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    with pd.ExcelWriter(r"/home/opticabi/Documents/rwanda_reports/Daily Net Sales Report.xlsx", engine='xlsxwriter') as writer:    
        payment_summary.to_excel(writer,sheet_name='daily', index=False)    
        payment_summarymtd.to_excel(writer,sheet_name='monthly', index=False)
     


    #------------------------------------MERGE THE MTD AND THE DAILY DATA---------------------------------------------------------------------------------
def daily_mtd_payments_rwanda():

    mtd = pd.read_excel(r"/home/opticabi/Documents/rwanda_reports/Daily Net Sales Report.xlsx",sheet_name = 'monthly')    

    mtd = mtd.rename(columns = {"Cash/Mpesa/CC":"MTD_Cash/Mpesa/CC","Insurance":"MTD_Insurance",
                                "Total Cash":"MTD_Total Cash","Total Gross Amount":"MTD_Total Gross Amount",
                                "Total Cash Net Amount":"MTD_Total Cash Net Amount",
                                "Total Insurance Net Amount":"MTD_Total Insurance Net Amount"})
    
    daily = pd.read_excel(r"/home/opticabi/Documents/rwanda_reports/Daily Net Sales Report.xlsx",sheet_name = 'daily')    

    ###merge
    daily_mtd = pd.merge(mtd,daily,on="Branch",how = 'outer')
    
    daily_mtd = daily_mtd.drop_duplicates(subset=['Branch'],keep='first')
    print(daily_mtd)
    print('its been printed')

    #Save in an excel file
    import xlsxwriter
    print(xlsxwriter.__version__)
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    with pd.ExcelWriter(r"/home/opticabi/Documents/rwanda_reports/Final Summary Net Sales.xlsx", engine='xlsxwriter') as writer:    
        daily_mtd.to_excel(writer,sheet_name='daily_monthly', index=False)    
 
def mtd_daily_net_payments_rwanda():
    paymentsmtd_daily = """
    SELECT doc_entry, doc_no, user_sign, createdon, createdat,case
    when length(createdat::text) in (1,2) then null
    else
        (left(createdat::text,(length(createdat::text)-2))||':'||right(createdat::text, 2))::time 
    end 
        as "Create_Time",
    createdby, cust_code, base_doc, order_no, full_doc_type, 
    mode_of_pay, total_advance_amt, advance_amt, remaining_amt, total_amt, status, branch_code, credit_card_name, 
    payment_method, mpesa_code, mpesa_date, mpesa_amt, insurance_company_name, insurance_scheme, insurance_membership_no, 
    principal_member_no, principal_member_name, web_payment_no, insurance_type, insurance_totl_apprv_amt, 
    insurance_frame_apprv_amt, insurance_lens_apprv_amt, insurance_act_apprv_amt, draft_orderno
    FROM voler_staging.source_payment
    where createdon::date between '{start_month}' and '{yesterday}'
    and status <> 'Cancel';

    """.format(start_month = start_month,yesterday = yesterday)

    paymentsmtd_daily = pd.read_sql_query(paymentsmtd_daily, con=conn)
    print(paymentsmtd_daily)

    ###Select the required columns
    paymentsmtd_daily = paymentsmtd_daily[['branch_code','draft_orderno','createdon', 'Create_Time','full_doc_type','mode_of_pay','total_advance_amt','advance_amt', 'remaining_amt']] 
    ###Rename columns
    paymentsmtd_daily = paymentsmtd_daily.rename(columns = {'branch_code':'Branch','draft_orderno':'Order No.','createdon':'Create_Date'})
    ###Create a new column and rename 
    paymentsmtd_daily["mode_of_pay1"] = paymentsmtd_daily.apply(lambda row: 'Insurance' if row['mode_of_pay'] == 'Insurance' else 'Cash/Mpesa/CC',axis =1)
    paymentsmtd_daily['advance_amt'] = pd.to_numeric(paymentsmtd_daily['advance_amt'])
    
    paymentsmtd_daily['Create_Date'] = pd.to_datetime(paymentsmtd_daily['Create_Date'])
    paymentsmtd_daily = paymentsmtd_daily.pivot_table(index = ['Create_Date','Branch'],columns = ['mode_of_pay1'],aggfunc = {'advance_amt':np.sum})
    paymentsmtd_daily = paymentsmtd_daily.droplevel([0],axis=1).reset_index().fillna(0) 

    ##Add Insurance if no transaction was made.
    if 'Insurance' not in paymentsmtd_daily.columns:
        paymentsmtd_daily['Insurance'] = 0
    else:  
        paymentsmtd_daily['Insurance']  

    paymentsmtd_daily['Total Cash'] = paymentsmtd_daily['Cash/Mpesa/CC']
    paymentsmtd_daily['Total Gross Amount'] = paymentsmtd_daily['Cash/Mpesa/CC'] + paymentsmtd_daily['Insurance']
    paymentsmtd_daily['Total Cash Net Amount'] = paymentsmtd_daily['Total Cash'] * (100/118)
    paymentsmtd_daily['Total Insurance Net Amount'] = paymentsmtd_daily['Insurance'] * (100/118)     
    paymentsmtd_daily = paymentsmtd_daily.round() 
    paymentsmtd_daily = paymentsmtd_daily.rename(columns={'Total Cash Net Amount':'Cash','Insurance':'Gross Insurance','Total Insurance Net Amount':'Insurance'})
    paymentsmtd_daily['Total NET Sales'] = paymentsmtd_daily['Cash'] + paymentsmtd_daily['Insurance']
    paymentsmtd_daily = paymentsmtd_daily[['Create_Date','Branch','Cash','Insurance','Total NET Sales']]
    print('lets check')    
    
    paymentsmtd_daily['Create_Date'] = paymentsmtd_daily['Create_Date'].astype(str)
    paymentsmtd_daily['Create_Date'] = paymentsmtd_daily['Create_Date'].str.replace('-','.')

    #---------------------------------------------------------------------------------------------------------------------------------------------

    netsales = """
        SELECT "Branch", "MTD_Cash/Mpesa/CC", "MTD_Insurance", "MTD_Total Cash", "MTD_Total Gross Amount", 
        "MTD_Total Cash Net Amount", "MTD_Total Insurance Net Amount", "Cash/Mpesa/CC", "Insurance", "Total Cash", 
        "Total Gross Amount", "Total Cash Net Amount", "Total Insurance Net Amount"
        FROM voler_dw.mtd_daily_net_payments;
        """ 
    netsales = pd.read_sql_query(netsales, con=conn)
    
    ##Select Columns
    netsales[" "] = range(1, len(netsales) + 1)
    netsales = netsales[[" ","Branch","MTD_Total Cash","MTD_Insurance","MTD_Total Cash Net Amount","MTD_Total Insurance Net Amount",
                         "Total Cash","Insurance","Total Cash Net Amount","Total Insurance Net Amount"]]    
    netsales = netsales.fillna(0)
    newnetsales = netsales.copy()
    print(netsales.columns)
    netsales[["MTD_Total Cash","MTD_Insurance","MTD_Total Cash Net Amount","MTD_Total Insurance Net Amount",
                         "Total Cash","Insurance","Total Cash Net Amount","Total Insurance Net Amount"]] = netsales[["MTD_Total Cash","MTD_Insurance","MTD_Total Cash Net Amount","MTD_Total Insurance Net Amount",
                         "Total Cash","Insurance","Total Cash Net Amount","Total Insurance Net Amount"]].astype(int)
    netsales = netsales.rename(columns = {"MTD_Total Cash Net Amount":"MTD Cash","MTD_Total Insurance Net Amount":"MTD Insurance",
                                    "Total Cash Net Amount":"Day Cash","Insurance":"Insurance1","Total Insurance Net Amount":"Day Insurance"})
    print('Columns renamed')
    print(netsales.columns)
    netsales['MTD Net Sales'] = netsales['MTD Cash'] + netsales['MTD Insurance']
    netsales['Day Net Sales'] = netsales['Day Cash'] + netsales['Day Insurance']
    print('Summation completed')
    netsales = netsales[["Branch","MTD Cash","MTD Insurance","MTD Net Sales","Day Cash","Day Insurance","Day Net Sales"]]
    netsales[["MTD Cash","MTD Insurance","MTD Net Sales","Day Cash","Day Insurance","Day Net Sales"]] = netsales[["MTD Cash","MTD Insurance","MTD Net Sales","Day Cash","Day Insurance","Day Net Sales"]].astype(int)
    print(netsales)  
    netsalesnew = netsales[["Branch","MTD Cash","MTD Insurance","MTD Net Sales"]]  

    #Save the data in an excel file
    import xlsxwriter
    print(xlsxwriter.__version__)

    #Create a Pandas Excel writer using XlsxWriter as the engine.
    with pd.ExcelWriter(r"/home/opticabi/Documents/rwanda_reports/Rwanda BranchWise Net Sales Report.xlsx", engine='xlsxwriter') as writer:    
        for group, dataframe in paymentsmtd_daily.groupby('Create_Date'):
            print(group)
            # save the dataframe for each group to a csv
            dataframe = dataframe.drop(['Create_Date'],axis=1)
            dataframe = dataframe.sort_values(by = 'Branch',ascending = True)            
            name = f'{group}'
            dataframe.to_excel(writer,sheet_name=name,index=False)
        netsalesnew.to_excel(writer,sheet_name='MTD', index=False)   



