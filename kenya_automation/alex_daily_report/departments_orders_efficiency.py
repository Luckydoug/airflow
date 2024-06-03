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

##Others
import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from sub_tasks.libraries.styles import styles, properties
from sub_tasks.libraries.utils import get_todate,send_report,assert_date_modified, create_initial_file, return_sent_emails, fetch_gsheet_data, record_sent_branch, fourth_week_start, fourth_week_end


from reports.draft_to_upload.utils.utils import return_report_daterange
from reports.draft_to_upload.utils.utils import get_report_frequency

# PG Execute(Query)
from sub_tasks.data.connect import (pg_execute, engine) 
# from sub_tasks.api_login.api_login import(login)
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")



#Define the days that is yesterday
today = datetime.date.today()
# today = datetime.date(2024,1,25)
yesterday = today - datetime.timedelta(days=1)
print(yesterday)
formatted_date = yesterday.strftime('%Y-%m-%d')


def update_calculated_field():
    OrdersWithIssues = fetch_gsheet_data()["orders_with_issues"]
    orders_cutoff = fetch_gsheet_data()["orders_cutoff"]

    departments = f"""
	with dept as 
    (SELECT dept, status, "Order Criteria", "Doc Entry", "Doc No", "start", finish, "Time Min",CAST(finish AS DATE) as "Finish_Date"
    FROM mabawa_mviews.v_orderefficiencydata) 
    select * from dept 
    where "Finish_Date"::date = '{yesterday}'
    """.format(yesterday=yesterday)
    departments = pd.read_sql_query(departments,con=conn)  

    mainstore_designer_steps = f""" 
    select *, 
    case 
        when sale_order_to_order_printed >= 0 and sale_order_to_order_printed <= 5 then '0 - 5 Mins'
        when sale_order_to_order_printed >= 6 and sale_order_to_order_printed <= 10 then '6 - 10 Mins'
        when sale_order_to_order_printed >= 11 and sale_order_to_order_printed <= 15 then '11 - 15 Mins'
        else '+15 Mins'
    end as tt,
    case 
        when order_printed_to_frame_to_lenstore >= 0 and order_printed_to_frame_to_lenstore <= 5 then '0 - 5 Mins'
        when order_printed_to_frame_to_lenstore >= 6 and order_printed_to_frame_to_lenstore <= 10 then '6 - 10 Mins'
        when order_printed_to_frame_to_lenstore >= 11 and order_printed_to_frame_to_lenstore <= 15 then '11 - 15 Mins'
        else '+15 Mins'
    end as tt1,
    TO_CHAR(DATE_TRUNC('hour', frame_to_lenstore), 'HH24:00') || ' - ' || TO_CHAR(DATE_TRUNC('hour', frame_to_lenstore) + INTERVAL '1 hour', 'HH24:00') AS Hour
    from mabawa_mviews.salesorder_to_senttolensstore
    where doc_no in (select "Doc No" from mabawa_mviews.v_orderefficiencydata
    where dept in ('Main Store','Designer'))
    and frame_to_lenstore::date = '{yesterday}'
    """
    mainstoredesigner =pd.read_sql_query(mainstore_designer_steps,con=conn)    

    mainstore = mainstoredesigner[mainstoredesigner['ods_outlet'] == '0MA']
    designer = mainstoredesigner[mainstoredesigner['ods_outlet'] == '0DS']

    saleorder_to_orderprinted_main = mainstore[mainstore['sale_order_to_order_printed'].notna()]
    orderprinted_to_lenstore_main = mainstore[mainstore['order_printed_to_frame_to_lenstore'].notna()]
    required_columns = ['Hour','Orders','0 - 5 Mins', '6 - 10 Mins', '11 - 15 Mins', '+15 Mins']

    saleorder_to_orderprinted_main = saleorder_to_orderprinted_main.pivot_table(index = 'hour',columns = 'tt',values = 'doc_entry',aggfunc = {'doc_entry':np.count_nonzero},margins = True,margins_name = 'Orders').reset_index().rename(columns = {'hour':'Hour'}).fillna(' ')
    orderprinted_to_lenstore_main = orderprinted_to_lenstore_main.pivot_table(index = 'hour',columns = 'tt1',values = 'doc_entry',aggfunc = {'doc_entry':np.count_nonzero},margins = True,margins_name = 'Orders').reset_index().rename(columns = {'hour':'Hour'}).fillna(' ')

    saleorder_to_orderprinted_designer = designer[designer['sale_order_to_order_printed'].notna()]
    orderprinted_to_lenstore_designer = designer[designer['order_printed_to_frame_to_lenstore'].notna()]

    saleorder_to_orderprinted_designer = saleorder_to_orderprinted_designer.pivot_table(index = 'hour',columns = 'tt',values = 'doc_entry',aggfunc = {'doc_entry':np.count_nonzero},margins = True,margins_name = 'Orders').reset_index().rename(columns = {'hour':'Hour'}).fillna(' ')
    orderprinted_to_lenstore_designer = orderprinted_to_lenstore_designer.pivot_table(index = 'hour',columns = 'tt1',values = 'doc_entry',aggfunc = {'doc_entry':np.count_nonzero},margins = True,margins_name = 'Orders').reset_index().rename(columns = {'hour':'Hour'}).fillna(' ')

    dataframes = [saleorder_to_orderprinted_main, orderprinted_to_lenstore_main, saleorder_to_orderprinted_designer, orderprinted_to_lenstore_designer]
    for df in dataframes:
        required_columns = ['Hour', 'Orders', '0 - 5 Mins', '6 - 10 Mins', '11 - 15 Mins', '+15 Mins']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            for col in missing_columns:
                df[col] = np.nan

    required_columns = ['Hour','Orders','0 - 5 Mins', '6 - 10 Mins', '11 - 15 Mins', '+15 Mins']
    saleorder_to_orderprinted_main = saleorder_to_orderprinted_main[required_columns]
    orderprinted_to_lenstore_main = orderprinted_to_lenstore_main[required_columns]
    saleorder_to_orderprinted_designer = saleorder_to_orderprinted_designer[required_columns]
    orderprinted_to_lenstore_designer = orderprinted_to_lenstore_designer[required_columns]

    OrdersWithIssues["DATE"] = pd.to_datetime(OrdersWithIssues.DATE, dayfirst=True, errors="coerce")
    print(OrdersWithIssues.dtypes)
    OrdersWithIssues = OrdersWithIssues[(OrdersWithIssues.DATE >= formatted_date) & (OrdersWithIssues.DATE <= formatted_date)]   
    
    dept_orders = departments[departments['dept'].isin(['Control', 'Designer', 'Main Store', 'Packaging', 'Lens Store'])]
    print(dept_orders)
    dept_orders['finish'] = dept_orders['finish'].fillna('')
    dept_orders[['Day', 'Time']] = dept_orders['finish'].str.split(' ', expand=True)    
    dept_orders['Time'] = pd.to_datetime(dept_orders['Time'], format='%H:%M:%S')
    dept_orders['Hour'] = dept_orders['Time'].dt.strftime('%H')  
    orders = pd.merge(dept_orders, orders_cutoff, on='Order Criteria', how='left')
    orders['cut off'] = np.where(orders['dept'] == 'Control', orders['Control Room'],
                                              (np.where(orders['dept'] == 'Designer',orders['Designer Store'], 
                                                        (np.where(orders['dept'] == 'Main Store', orders['Main Store'],
                                                                   (np.where(orders['dept'] == 'Lens Store', orders['Lens Store'],
                                                                              (np.where(orders['dept'] == 'Packaging', orders['Packaging'], 10)))))))))
    
    orders['Delay'] = np.where(orders['Time Min'] > orders['cut off'], 1, 0)


    """   CONTROL ORDER EFFICIENCY   """
    branchrejected = f"""
    select sd.doc_entry,so.doc_no,sd.odsc_date,sd.odsc_status,so.ods_status  from mabawa_staging.source_orderscreenc1 sd
    left join mabawa_staging.source_orderscreen so on so.doc_entry = sd.doc_entry  
    where odsc_date::date between '2024-04-01' and '{yesterday}'
    and odsc_status = 'Branch Reject Received at Receiver'
    """
    branchrejected_df = pd.read_sql_query(branchrejected, con=engine)
    branchrejected_df_list = branchrejected_df['doc_no'].to_list()

    controlIssues = OrdersWithIssues[OrdersWithIssues["DEPARTMENT"] == "CONTROL ROOM"]
    controlIssuesOrders = controlIssues["ORDER NUMBER"].tolist()

    control = orders[orders['dept'] == 'Control']
    control = control[~control['Doc No'].isin(branchrejected_df_list)]
    control = control.drop_duplicates(subset = ['Doc No','start'],keep = 'first')
    control = control[~control["Doc No"].isin(controlIssuesOrders)]


    controlpivot1 = pd.pivot_table(control, index=['Hour'], values=[
                                'Doc No'], aggfunc='count', fill_value=0, margins=True, margins_name='Total')
    controlpivot2 = pd.pivot_table(control, index=['Hour'], values=[
                                'Time Min'], aggfunc='mean', fill_value=0, margins=True, margins_name='Total')
    controlpivot3 = pd.pivot_table(control, index=['Hour'], values=[
                                'Delay'], aggfunc='sum', fill_value=0, margins=True, margins_name='Total')
    
    controlpivot4 = pd.merge(controlpivot1, controlpivot2, on=['Hour'], how='left')
    controlpivot = pd.merge(controlpivot4, controlpivot3, on=['Hour'], how='left')
    print(controlpivot)
    controlpivot['% of Efficiency'] = (controlpivot['Doc No'] - controlpivot['Delay'])/controlpivot['Doc No']

    controlpivot['Time Min'] = controlpivot['Time Min'].round(2)
    controlpivot['% of Efficiency'] = controlpivot['% of Efficiency'].map('{:.2%}'.format)
    controlpivot = np.transpose(controlpivot)
    print(controlpivot)

    """ Delayed Orders"""
    control_delay = control[control['Delay'] == 1]
    if not control_delay.empty:
        controldelay = pd.pivot_table(control_delay, index=['Hour', 'Doc No'], values='Time Min', aggfunc='mean', fill_value=0)
    else:
        columns = ['Hour', 'Doc No', 'Time Min']
        controldelay = pd.DataFrame(columns=columns)     
    print('Let us print controldelay')


    """Cut Off"""
    control['Time Taken'] = control.apply 
    control['15 min'] = control['Time Min'].apply(lambda x: 1 if x > 15 else 0)
    control['12 min'] = control['Time Min'].apply(lambda x: 1 if x > 12 else 0)
    control['10 min'] = control['Time Min'].apply(lambda x: 1 if x > 10 else 0)
    control['7 min'] = control['Time Min'].apply(lambda x: 1 if x > 7 else 0)

    cuttoff1 = pd.pivot_table(control, index='dept', values=[
                          '15 min'], aggfunc='sum', fill_value=0)
    cuttoff2 = pd.pivot_table(control, index='dept', values=[
                            '12 min'], aggfunc='sum', fill_value=0)
    cuttoff3 = pd.pivot_table(control, index='dept', values=[
                            '10 min'], aggfunc='sum', fill_value=0)
    cuttoff4 = pd.pivot_table(control, index='dept', values=[
                            '7 min'], aggfunc='sum', fill_value=0)
    cutcontrol = pd.merge(cuttoff1, cuttoff2, on='dept')
    cutcontrol = pd.merge(cutcontrol, cuttoff3, on='dept')
    cutcontrol = pd.merge(cutcontrol, cuttoff4, on='dept')

    print('Control Room Calculated')

    """   DESIGNER ORDER EFFICIENCY   """        
    designerIssues = OrdersWithIssues[OrdersWithIssues["DEPARTMENT"] == "DESIGNER STORE"]
    designerIssuesOrders = designerIssues["ORDER NUMBER"].tolist()

    designer = orders[orders['dept'] == 'Designer']
    designer = designer[~designer["Doc No"].isin(designerIssuesOrders)]
    designerpivot1 = pd.pivot_table(designer, index=['Hour'], values=[
                                'Doc No'], aggfunc='count', fill_value=0, margins=True, margins_name='Total')
    designerpivot2 = pd.pivot_table(designer, index=['Hour'], values=[
                                'Time Min'], aggfunc='mean', fill_value=0, margins=True, margins_name='Total')
    designerpivot3 = pd.pivot_table(designer, index=['Hour'], values=[
                                'Delay'], aggfunc='sum', fill_value=0, margins=True, margins_name='Total')
    
    designerpivot4 = pd.merge(designerpivot1, designerpivot2, on=['Hour'], how='left')
    designerpivot = pd.merge(designerpivot4, designerpivot3, on=['Hour'], how='left')
    print(designerpivot)
    designerpivot['% of Efficiency'] = (designerpivot['Doc No'] - designerpivot['Delay'])/designerpivot['Doc No']

    designerpivot['Time Min'] = designerpivot['Time Min'].round(2)
    designerpivot['% of Efficiency'] = designerpivot['% of Efficiency'].map('{:.2%}'.format)
    designerpivot = np.transpose(designerpivot)
    print(designerpivot)

    """ Delayed Orders"""
    designer_delay = designer[designer['Delay'] == 1]
    if not designer_delay.empty:
        designerdelay = pd.pivot_table(designer_delay, index=['Hour', 'Doc No'], values='Time Min', aggfunc='mean', fill_value=0)
    else:
        columns = ['Hour', 'Doc No', 'Time Min']
        designerdelay = pd.DataFrame(columns=columns) 

    """Cut Off"""
    designer['Time Taken'] = designer.apply 
    designer['15 min'] = designer['Time Min'].apply(lambda x: 1 if x > 15 else 0)
    designer['12 min'] = designer['Time Min'].apply(lambda x: 1 if x > 12 else 0)
    designer['8 min'] = designer['Time Min'].apply(lambda x: 1 if x > 8 else 0)
    designer['6 min'] = designer['Time Min'].apply(lambda x: 1 if x > 6 else 0)

    cuttoff1 = pd.pivot_table(designer, index='dept', values=[
                          '15 min'], aggfunc='sum', fill_value=0)
    cuttoff2 = pd.pivot_table(designer, index='dept', values=[
                            '12 min'], aggfunc='sum', fill_value=0)
    cuttoff3 = pd.pivot_table(designer, index='dept', values=[
                            '8 min'], aggfunc='sum', fill_value=0)
    cuttoff4 = pd.pivot_table(designer, index='dept', values=[
                            '6 min'], aggfunc='sum', fill_value=0)
    cutdesigner = pd.merge(cuttoff1, cuttoff2, on='dept')
    cutdesigner = pd.merge(cutdesigner, cuttoff3, on='dept')
    cutdesigner = pd.merge(cutdesigner, cuttoff4, on='dept')
    print(cutdesigner)
    print('Designer Calculated')

    """ MAIN STORE ORDER EFFICIENCY   """        
    mainstoreIssues = OrdersWithIssues[OrdersWithIssues["DEPARTMENT"] == "MAIN STORE"]
    mainstoreIssuesOrders = mainstoreIssues["ORDER NUMBER"].tolist()

    mainstore = orders[orders['dept'] == 'Main Store']
    mainstore = mainstore[~mainstore["Doc No"].isin(mainstoreIssuesOrders)]
    mainstorepivot1 = pd.pivot_table(mainstore, index=['Hour'], values=[
                                'Doc No'], aggfunc='count', fill_value=0, margins=True, margins_name='Total')
    mainstorepivot2 = pd.pivot_table(mainstore, index=['Hour'], values=[
                                'Time Min'], aggfunc='mean', fill_value=0, margins=True, margins_name='Total')
    mainstorepivot3 = pd.pivot_table(mainstore, index=['Hour'], values=[
                                'Delay'], aggfunc='sum', fill_value=0, margins=True, margins_name='Total')
    
    mainstorepivot4 = pd.merge(mainstorepivot1, mainstorepivot2, on=['Hour'], how='left')
    mainstorepivot = pd.merge(mainstorepivot4, mainstorepivot3, on=['Hour'], how='left')
    print(mainstorepivot)
    mainstorepivot['% of Efficiency'] = (mainstorepivot['Doc No'] - mainstorepivot['Delay'])/mainstorepivot['Doc No']

    mainstorepivot['Time Min'] = mainstorepivot['Time Min'].round(2)
    mainstorepivot['% of Efficiency'] = mainstorepivot['% of Efficiency'].map('{:.2%}'.format)
    mainstorepivot = np.transpose(mainstorepivot)
    print(mainstorepivot)

    """ Delayed Orders"""
    mainstore_delay = mainstore[mainstore['Delay'] == 1]
    if not mainstore_delay.empty:
        mainstoredelay = pd.pivot_table(mainstore_delay, index=['Hour', 'Doc No'], values='Time Min', aggfunc='mean', fill_value=0)
    else:
        columns = ['Hour', 'Doc No', 'Time Min']
        mainstoredelay = pd.DataFrame(columns=columns) 

    """Cut Off"""
    mainstore['Time Taken'] = mainstore.apply 
    mainstore['15 min'] = mainstore['Time Min'].apply(lambda x: 1 if x > 15 else 0)
    mainstore['12 min'] = mainstore['Time Min'].apply(lambda x: 1 if x > 12 else 0)
    mainstore['8 min'] = mainstore['Time Min'].apply(lambda x: 1 if x > 8 else 0)
    mainstore['6 min'] = mainstore['Time Min'].apply(lambda x: 1 if x > 6 else 0)

    cuttoff1 = pd.pivot_table(mainstore, index='dept', values=[
                            '15 min'], aggfunc='sum', fill_value=0)
    cuttoff2 = pd.pivot_table(mainstore, index='dept', values=[
                            '12 min'], aggfunc='sum', fill_value=0)
    cuttoff3 = pd.pivot_table(mainstore, index='dept', values=[
                            '8 min'], aggfunc='sum', fill_value=0)
    cuttoff4 = pd.pivot_table(mainstore, index='dept', values=[
                            '6 min'], aggfunc='sum', fill_value=0)

    cutmainstore = pd.merge(cuttoff1, cuttoff2, on='dept')
    cutmainstore = pd.merge(cutmainstore, cuttoff3, on='dept')
    cutmainstore = pd.merge(cutmainstore, cuttoff4, on='dept')
    print(cutmainstore)
    print('Main Store Calculated')

    """ PACKAGING ORDER EFFICIENCY   """ 
    ibc_query = f"""
        select doc_no::int,odsc_date::date, odsc_time,odsc_status
        from mabawa_staging.source_orderscreenc1 os
        left join mabawa_staging.source_orderscreen so 
        on os.doc_entry = so.doc_entry 
        where odsc_status in ('IBC Received at Packaging')
        and odsc_date::date between '2024-01-01' and '{yesterday}'
        """
    ibc = pd.read_sql_query(ibc_query, con=engine)     
    ibc_to_drop = ibc["doc_no"].tolist()

    packagingIssues = OrdersWithIssues[OrdersWithIssues["DEPARTMENT"] == "PACKAGING"]
    packagingIssuesOrders = packagingIssues["ORDER NUMBER"].tolist()

    packaging = orders[orders['dept'] == 'Packaging']
    packaging = packaging[~packaging["Doc No"].isin(packagingIssuesOrders)]
    packaging = packaging[~packaging["Doc No"].isin(ibc_to_drop)]
    
    packagingpivot1 = pd.pivot_table(packaging, index=['Hour'], values=[
                                'Doc No'], aggfunc='count', fill_value=0, margins=True, margins_name='Total')
    packagingpivot2 = pd.pivot_table(packaging, index=['Hour'], values=[
                                'Time Min'], aggfunc='mean', fill_value=0, margins=True, margins_name='Total')
    packagingpivot3 = pd.pivot_table(packaging, index=['Hour'], values=[
                                'Delay'], aggfunc='sum', fill_value=0, margins=True, margins_name='Total')
    
    packagingpivot4 = pd.merge(packagingpivot1, packagingpivot2, on=['Hour'], how='left')
    packagingpivot = pd.merge(packagingpivot4, packagingpivot3, on=['Hour'], how='left')
    print(packagingpivot)
    packagingpivot['% of Efficiency'] = (packagingpivot['Doc No'] - packagingpivot['Delay'])/packagingpivot['Doc No']

    packagingpivot['Time Min'] = packagingpivot['Time Min'].round(2)
    packagingpivot['% of Efficiency'] = packagingpivot['% of Efficiency'].map('{:.2%}'.format)
    packagingpivot = np.transpose(packagingpivot)
    print(packagingpivot)

    """ Delayed Orders"""
    packaging_delay = packaging[packaging['Delay'] == 1]
    if not packaging_delay.empty:
        packagingdelay = pd.pivot_table(packaging_delay, index=['Hour', 'Doc No'], values='Time Min', aggfunc='mean', fill_value=0)
    else:    
        columns = ['Hour', 'Doc No', 'Time Min']
        packagingdelay = pd.DataFrame(columns=columns)     

    """Cut Off"""
    packaging['Time Taken'] = packaging.apply 
    packaging['15 min'] = packaging['Time Min'].apply(lambda x: 1 if x > 15 else 0)
    packaging['12 min'] = packaging['Time Min'].apply(lambda x: 1 if x > 12 else 0)
    packaging['8 min'] = packaging['Time Min'].apply(lambda x: 1 if x > 8 else 0)
    packaging['6 min'] = packaging['Time Min'].apply(lambda x: 1 if x > 6 else 0)

    cuttoff1 = pd.pivot_table(packaging, index='dept', values=[
                            '15 min'], aggfunc='sum', fill_value=0)
    cuttoff2 = pd.pivot_table(packaging, index='dept', values=[
                            '12 min'], aggfunc='sum', fill_value=0)
    cuttoff3 = pd.pivot_table(packaging, index='dept', values=[
                            '8 min'], aggfunc='sum', fill_value=0)
    cuttoff4 = pd.pivot_table(packaging, index='dept', values=[
                            '6 min'], aggfunc='sum', fill_value=0)

    cutpackaging = pd.merge(cuttoff1, cuttoff2, on='dept')
    cutpackaging = pd.merge(cutpackaging, cuttoff3, on='dept')
    cutpackaging = pd.merge(cutpackaging, cuttoff4, on='dept')
    print(cutpackaging)
    print('Packaging Calculated')

    """ LENS STORE ORDER EFFICIENCY   """        
    lensstoreIssues = OrdersWithIssues[OrdersWithIssues["DEPARTMENT"] == "LENS STORE"]
    lensstoreIssuesOrders = lensstoreIssues["ORDER NUMBER"].tolist()

    lensstore = orders[orders['dept'] == 'Lens Store']
    lensstore = lensstore[~lensstore["Doc No"].isin(lensstoreIssuesOrders)]
    lensstorepivot1 = pd.pivot_table(lensstore, index=['Hour'], values=[
                                'Doc No'], aggfunc='count', fill_value=0, margins=True, margins_name='Total')
    lensstorepivot2 = pd.pivot_table(lensstore, index=['Hour'], values=[
                                'Time Min'], aggfunc='mean', fill_value=0, margins=True, margins_name='Total')
    lensstorepivot3 = pd.pivot_table(lensstore, index=['Hour'], values=[
                                'Delay'], aggfunc='sum', fill_value=0, margins=True, margins_name='Total')
    
    lensstorepivot4 = pd.merge(lensstorepivot1, lensstorepivot2, on=['Hour'], how='left')
    lensstorepivot = pd.merge(lensstorepivot4, lensstorepivot3, on=['Hour'], how='left')
    print(lensstorepivot)
    lensstorepivot['% of Efficiency'] = (lensstorepivot['Doc No'] - lensstorepivot['Delay'])/lensstorepivot['Doc No']

    lensstorepivot['Time Min'] = lensstorepivot['Time Min'].round(2)
    lensstorepivot['% of Efficiency'] = lensstorepivot['% of Efficiency'].map('{:.2%}'.format)
    lensstorepivot = np.transpose(lensstorepivot)
    print(lensstorepivot)

    """ Delayed Orders"""
    lensstore_delay = lensstore[lensstore['Delay'] == 1]
    if not lensstore_delay.empty:
        lensstoredelay = pd.pivot_table(lensstore_delay, index=['Hour', 'Doc No'], values='Time Min', aggfunc='mean', fill_value=0)
        print(lensstoredelay)
    else:
        columns = ['Hour', 'Doc No', 'Time Min']
        lensstoredelay = pd.DataFrame(columns=columns) 

    """Cut Off"""
    lensstore['Time Taken'] = lensstore.apply 
    lensstore['15 min'] = lensstore['Time Min'].apply(lambda x: 1 if x > 15 else 0)
    lensstore['12 min'] = lensstore['Time Min'].apply(lambda x: 1 if x > 12 else 0)
    lensstore['8 min'] = lensstore['Time Min'].apply(lambda x: 1 if x > 8 else 0)
    lensstore['6 min'] = lensstore['Time Min'].apply(lambda x: 1 if x > 6 else 0)

    cuttoff1 = pd.pivot_table(lensstore, index='dept', values=[
                            '15 min'], aggfunc='sum', fill_value=0)
    cuttoff2 = pd.pivot_table(lensstore, index='dept', values=[
                            '12 min'], aggfunc='sum', fill_value=0)
    cuttoff3 = pd.pivot_table(lensstore, index='dept', values=[
                            '8 min'], aggfunc='sum', fill_value=0)
    cuttoff4 = pd.pivot_table(lensstore, index='dept', values=[
                            '6 min'], aggfunc='sum', fill_value=0)

    cutt = pd.merge(cuttoff1, cuttoff2, on='dept')
    cutt = pd.merge(cutt, cuttoff3, on='dept')
    cutt = pd.merge(cutt, cuttoff4, on='dept')
    # cuttlens2 = cutt.copy()
    print('lens Store Calculated')

    """Save In Excel """
    with pd.ExcelWriter(r"/home/opticabi/Documents/optica_reports/order_efficiency/order efficiency results.xlsx", engine='xlsxwriter') as writer:
        controlpivot.to_excel(writer, sheet_name='Control', index=True)
        controldelay.to_excel(writer, sheet_name='Control',index=True, startrow=15)
        cutcontrol.to_excel(writer, sheet_name='Control', index=True, startrow=9)
        designerpivot.to_excel(writer, sheet_name='Designer', index=True)
        designerdelay.to_excel(writer, sheet_name='Designer',index=True, startrow=15)
        cutdesigner.to_excel(writer, sheet_name='Designer',index=True, startrow=9)
        mainstorepivot.to_excel(writer, sheet_name='Main store', index=True)
        mainstoredelay.to_excel(writer, sheet_name='Main store', index=True, startrow=15)
        cutmainstore.to_excel(writer, sheet_name='Main store', index=True, startrow=9)
        packagingpivot.to_excel(writer, sheet_name='Packaging', index=True)
        packagingdelay.to_excel(writer, sheet_name='Packaging', index=True, startrow=15)
        cutpackaging.to_excel(writer, sheet_name='Packaging', index=True, startrow=9)
        lensstorepivot.to_excel(writer, sheet_name='Lens store', index=True)
        lensstoredelay.to_excel(writer, sheet_name='Lens store', index=True, startrow=15)
        cutt.to_excel(writer, sheet_name='Lens store', index=True, startrow=9)
        controlpivot.to_excel(writer, sheet_name='Control', index=True)

    with pd.ExcelWriter("/home/opticabi/Documents/optica_reports/order_efficiency/maindesignerbreakdown.xlsx", engine='xlsxwriter') as writer:    
        saleorder_to_orderprinted_main.to_excel(writer, sheet_name='so_to_op_main', index=False)
        orderprinted_to_lenstore_main.to_excel(writer, sheet_name='op_to_lenstore_main', index=False)   
        saleorder_to_orderprinted_designer.to_excel(writer, sheet_name='so_to_op_designer', index=False)
        orderprinted_to_lenstore_designer.to_excel(writer, sheet_name='op_to_lenstore_designer', index=False) 
    writer.save()

    def save_xls(list_dfs, xls_path):
        with ExcelWriter(xls_path) as writer:
            for n, df in enumerate(list_dfs):
                df.to_excel(writer, 'sheet%s' % n)
            writer.save()

# update_calculated_field()   