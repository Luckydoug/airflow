# import sys
# import numpy as np
# sys.path.append(".")

# # Import Libraries
# import json
# import psycopg2
# import requests
# import pandas as pd
# from pandas.io.json._normalize import nested_to_record 
# from sqlalchemy import create_engine
# from airflow.models import Variable
# from airflow.exceptions import AirflowException
# from pandas.io.json._normalize import nested_to_record 
# from pangres import upsert, DocsExampleTable
# from sqlalchemy import create_engine, text, VARCHAR
# from datetime import date
# import datetime
# import pytz
# import businesstimedelta
# import pandas as pd
# import holidays as pyholidays
# from workalendar.africa import Kenya
# import pygsheets
# import mysql.connector as database
# import urllib.parse

# # PG Execute(Query)
# from sub_tasks.data.connect import (pg_execute, engine) 
# from sub_tasks.api_login.api_login import(login)
# conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")

# SessionId = login()

# def receiving():
#     # Receiving Data
#     receiving = pd.read_sql("""
#     select * 
#     from mabawa_staging.source_orderscreenc1 a
#     left join
#     (select doc_entry, doc_no, cust_code, ods_outlet
#     from mabawa_staging.source_orderscreen) as b
#     using (doc_entry)
#     where a.odsc_createdby in ('receiving1','receiving2')
#     and a.odsc_date::date >= '2022-12-01'
#     """,con=engine)

#     print(receiving)

#     # Editing the receiving dataframe
#     receiving['odsc_date'] = pd.to_datetime(receiving['odsc_date'], dayfirst=True)
#     receiving['odsc_time']= pd.to_datetime(receiving['odsc_time'], format ='%H%M').dt.time
#     receiving['DateTime'] = pd.to_datetime(receiving['odsc_date'].astype(str) + ' ' + receiving['odsc_time'].astype(str))
    
#     # Riders Information
#     riders = pd.read_sql("""
#     SELECT trip_date, trip, westlands_time_from_hq, westlands_time_back_at_hq, karen_time_from_hq, karen_time_back_at_hq, eastlands_time_from_hq, eastlands_time_back_at_hq, thika_time_from_hq, thika_time_back_at_hq, mombasa_time_from_hq, "mombasa_time_back_at_hq", "rongai_time_from_hq", rongai_time_back_at_hq, upcountry, "CBD  time from Hq", cbd_time_back_at_hq, "CBD2  time from Hq", cbd2_time_back_at_hq
#     FROM mabawa_staging.source_riders
#     """,con=engine)

#     # Order Screen
#     AllOrders = pd.read_sql("""
#     select  doc_entry,
#             doc_no,
#             ods_createdon,
#             cust_code,
#             ods_normal_repair_order,
#             ods_status,
#             ods_ordercriteriastatus,
#             ods_total_amt,
#             ods_outlet
#     from mabawa_staging.source_orderscreen
#     where ods_createdon::date >= '2022-12-01'
#     """,con=engine)  

#     AllOrders.rename(columns={'doc_entry':'DocEntry','doc_no':'Order No.','ods_createdon':'CreateDate','cust_code':'Customer Code','ods_normal_repair_order':'Normal(Or)Repair Order','ods_status':'Status','ods_ordercriteriastatus':'Order Criteria Status','ods_total_amt':'Order Total Amount','ods_outlet':'Outlet'}, inplace=True)

#     AllOrders = AllOrders[['Order No.','Order Criteria Status']].rename(columns={'Order No.':'doc_no','Order Criteria Status':'order_criteria_status'})

#     receiving = pd.merge(receiving, AllOrders, on='doc_no', how='left')
#     print(receiving)
#     print('receivig printed')
#     gc = pygsheets.authorize(service_file='/home/opticabi/airflow/dags/sub_tasks/gsheets/keys2.json')
#     sh = gc.open_by_key('1U32LqEJ8sy9YUmSP4nsK4HSHPJTEEmtaZJ9to-joZms')
#     routes = sh[0]
#     routes = pd.DataFrame(routes.get_all_records())

#     print('The routes information has been fetched')

#     # Convert the date column to datetime
#     routes['Date'] = pd.to_datetime(routes['Date'], dayfirst=True)

#     # Thika Route
#     thika = routes[['Date','Trip','Thika Road']]
#     thika['Thika Branches'] = thika['Thika Road'].str.split(',')
#     thika = thika.explode('Thika Branches')
#     print('thika exploded')

#     # Thika Time back at Hq based on the full/partial trips
#     thikaroad = riders[['trip_date','trip','thika_time_back_at_hq']]
#     thika = thika[['Date','Trip','Thika Branches']].rename(columns={'Date':'trip_date','Trip':'trip'})
#     thi = pd.merge(thikaroad, thika, on=['trip_date','trip'], how='left')
#     print(thi)
#     # Merge based on the date and branch
#     thikas = thi['Thika Branches'].to_list()
#     thika = receiving[receiving['ods_outlet'].isin(thikas)]
#     thika.sort_values(by=['odsc_date'], ascending=True, inplace=True)
#     print(thika['doc_no'].nunique())


#     # Thika Receiving
#     thika = pd.merge(thika, thi, left_on=['odsc_date','ods_outlet'], right_on=['trip_date','Thika Branches'], how='left')
#     print(thika['doc_no'].nunique())
#     print(thika)
#     # Selecting the trip that came with the orders
#     thika['thika_time_back_at_hq2'] = thika.groupby('doc_no', as_index=False)['thika_time_back_at_hq'].shift(-1)

#     # Fill the nulls with the closing time for OHO 
#     thika['thika_time_back_at_hq2'] = thika['thika_time_back_at_hq2'].fillna('18:00:00')

#     # thika['odsc_time']= pd.to_datetime(thika['odsc_time']).dt.time
#     thika['thika_time_back_at_hq']= pd.to_datetime(thika['thika_time_back_at_hq']).dt.time
#     thika['thika_time_back_at_hq2']= pd.to_datetime(thika['thika_time_back_at_hq2']).dt.time

#     # Pick the correct trip the orders came with 
#     thika['CorrectTrip'] = np.where((thika['odsc_time'] >= thika['thika_time_back_at_hq']) & (thika['odsc_time'] <= thika['thika_time_back_at_hq2']) | (thika['odsc_time'] >= thika['thika_time_back_at_hq']) & (thika['thika_time_back_at_hq2'].isnull()),1,0)

#     # Number of trips with a correct trip
#     correct = thika[thika['CorrectTrip']==1]
#     print(correct['doc_no'].nunique())
#     cor = correct['doc_no'].unique()

#     nocorrect = thika.loc[(thika['CorrectTrip']==0) & (~thika['doc_no'].isin(cor))]

#     thika['region'] = 'Thika'

#     # Upcountry Route
#     upcountry = routes[['Date','Trip','Upcountry']]
#     upcountry['Upcountry Branches'] = upcountry['Upcountry'].str.split(',')
#     upcountry = upcountry.explode('Upcountry Branches')

#     # Thika Time back at Hq based on the full/partial trips
#     uproad = riders[['trip_date','trip','upcountry']]
#     upcountry = upcountry[['Date','Trip','Upcountry Branches']].rename(columns={'Date':'trip_date','Trip':'trip'})
#     up = pd.merge(uproad, upcountry, on=['trip_date','trip'], how='left')

#     # Merge based on the date and branch
#     ups = up['Upcountry Branches'].to_list()
#     upcountry = receiving[receiving['ods_outlet'].isin(ups)]
#     upcountry.sort_values(by=['odsc_date'], ascending=True, inplace=True)
#     print(upcountry['doc_no'].nunique())

#     # Upcountry Receiving
#     upcountry = pd.merge(upcountry, up, left_on=['odsc_date','ods_outlet'], right_on=['trip_date','Upcountry Branches'], how='left')
#     print(upcountry['doc_no'].nunique())

#     # Upcountry Time
#     upcountry['upcountry_time'] = pd.to_datetime(upcountry['trip_date'].astype(str) + ' ' + upcountry['upcountry'].astype(str),errors = 'coerce')

#     upcountry['region'] = 'Upcountry'

#     # Karen Route
#     karen = routes[['Date','Trip','Karen']]
#     karen['Karen Branches'] = karen['Karen'].str.split(',')
#     karen = karen.explode('Karen Branches')

#     # Karen Time back at Hq based on the full/partial trips
#     karenroad = riders[['trip_date','trip','karen_time_back_at_hq']]
#     karen = karen[['Date','Trip','Karen Branches']].rename(columns={'Date':'trip_date','Trip':'trip'})
#     kar = pd.merge(karenroad, karen, on=['trip_date','trip'], how='left')

#     # Merge based on the date and branch
#     karens = kar['Karen Branches'].to_list()
#     karen = receiving[receiving['ods_outlet'].isin(karens)]
#     karen.sort_values(by=['odsc_date'], ascending=True, inplace=True)
#     print(karen['doc_no'].nunique())

#     # Thika Receiving
#     karen = pd.merge(karen, kar, left_on=['odsc_date','ods_outlet'], right_on=['trip_date','Karen Branches'], how='left')
#     print(karen['doc_no'].nunique())

#     # Selecting the trip that came with the orders
#     karen['karen_time_back_at_hq2'] = karen.groupby('doc_no', as_index=False)['karen_time_back_at_hq'].shift(-1)

#     # Fill the nulls with the closing time for OHO 
#     karen['karen_time_back_at_hq2'] = karen['karen_time_back_at_hq2'].fillna('18:00:00')

#     # thika['odsc_time']= pd.to_datetime(thika['odsc_time']).dt.time
#     karen['karen_time_back_at_hq']= pd.to_datetime(karen['karen_time_back_at_hq'], errors='coerce').dt.time
#     karen['karen_time_back_at_hq2']= pd.to_datetime(karen['karen_time_back_at_hq2'], errors='coerce').dt.time

#     # Pick the correct trip the orders came with 
#     karen['CorrectTrip'] = np.where((karen['odsc_time'] >= karen['karen_time_back_at_hq']) & (karen['odsc_time'] <= karen['karen_time_back_at_hq2']) | (karen['odsc_time'] >= karen['karen_time_back_at_hq']) & (karen['karen_time_back_at_hq2'].isnull()),1,0)

#     karen['region'] = 'Karen'

#     # Thika Route
#     eastlands = routes[['Date','Trip','Eastlands']]
#     eastlands['Eastlands Branches'] = eastlands['Eastlands'].str.split(',')
#     eastlands = eastlands.explode('Eastlands Branches')

#     # Thika Time back at Hq based on the full/partial trips
#     eastroad = riders[['trip_date','trip','eastlands_time_back_at_hq']]
#     eastlands = eastlands[['Date','Trip','Eastlands Branches']].rename(columns={'Date':'trip_date','Trip':'trip'})
#     east = pd.merge(eastroad, eastlands, on=['trip_date','trip'], how='left')

#     # Merge based on the date and branch
#     easts = east['Eastlands Branches'].to_list()
#     eastlands = receiving[receiving['ods_outlet'].isin(easts)]
#     eastlands.sort_values(by=['odsc_date'], ascending=True, inplace=True)
#     print(eastlands['doc_no'].nunique())

#     # Thika Receiving
#     eastlands = pd.merge(eastlands, east, left_on=['odsc_date','ods_outlet'], right_on=['trip_date','Eastlands Branches'], how='left')
#     print(eastlands['doc_no'].nunique())

#     # Selecting the trip that came with the orders
#     eastlands['eastlands_time_back_at_hq2'] = eastlands.groupby('doc_no', as_index=False)['eastlands_time_back_at_hq'].shift(-1)

#     # Fill the nulls with the closing time for OHO 
#     eastlands['eastlands_time_back_at_hq2'] = eastlands['eastlands_time_back_at_hq2'].fillna('18:00:00')

#     # thika['odsc_time']= pd.to_datetime(thika['odsc_time']).dt.time
#     eastlands['eastlands_time_back_at_hq']= pd.to_datetime(eastlands['eastlands_time_back_at_hq']).dt.time
#     eastlands['eastlands_time_back_at_hq2']= pd.to_datetime(eastlands['eastlands_time_back_at_hq2']).dt.time

#     # Pick the correct trip the orders came with 
#     eastlands['CorrectTrip'] = np.where((eastlands['odsc_time'] >= eastlands['eastlands_time_back_at_hq']) & (eastlands['odsc_time'] <= eastlands['eastlands_time_back_at_hq2']) | (eastlands['odsc_time'] >= eastlands['eastlands_time_back_at_hq']) & (eastlands['eastlands_time_back_at_hq2'].isnull()),1,0)

#     eastlands['region'] = 'Eastlands'

#     # Thika Route
#     mombasa = routes[['Date','Trip','Mombasa Road']]
#     mombasa['Mombasa Road Branches'] = mombasa['Mombasa Road'].str.split(',')
#     mombasa = mombasa.explode('Mombasa Road Branches')

#     # Thika Time back at Hq based on the full/partial trips
#     mombasaroad = riders[['trip_date','trip','mombasa_time_back_at_hq']]
#     mombasa = mombasa[['Date','Trip','Mombasa Road Branches']].rename(columns={'Date':'trip_date','Trip':'trip'})
#     msa = pd.merge(mombasaroad, mombasa, on=['trip_date','trip'], how='left')

#     # Merge based on the date and branch
#     msas = msa['Mombasa Road Branches'].to_list()
#     mombasa = receiving[receiving['ods_outlet'].isin(msas)]
#     mombasa.sort_values(by=['odsc_date'], ascending=True, inplace=True)
#     print(mombasa['doc_no'].nunique())


#     # Thika Receiving
#     mombasa = pd.merge(mombasa, msa, left_on=['odsc_date','ods_outlet'], right_on=['trip_date','Mombasa Road Branches'], how='left')
#     print(mombasa['doc_no'].nunique())

#     # Selecting the trip that came with the orders
#     mombasa['mombasa_time_back_at_hq2'] = mombasa.groupby('doc_no', as_index=False)['mombasa_time_back_at_hq'].shift(-1)

#     # Fill the nulls with the closing time for OHO 
#     mombasa['mombasa_time_back_at_hq2'] = mombasa['mombasa_time_back_at_hq2'].fillna('18:00:00')

#     # thika['odsc_time']= pd.to_datetime(thika['odsc_time']).dt.time
#     mombasa['mombasa_time_back_at_hq']= pd.to_datetime(mombasa['mombasa_time_back_at_hq']).dt.time
#     mombasa['mombasa_time_back_at_hq2']= pd.to_datetime(mombasa['mombasa_time_back_at_hq2']).dt.time

#     # Pick the correct trip the orders came with 
#     mombasa['CorrectTrip'] = np.where((mombasa['odsc_time'] >= mombasa['mombasa_time_back_at_hq']) & (mombasa['odsc_time'] <= mombasa['mombasa_time_back_at_hq2']) | (mombasa['odsc_time'] >= mombasa['mombasa_time_back_at_hq']) & (mombasa['mombasa_time_back_at_hq2'].isnull()),1,0)

#     mombasa['region'] = 'Mombasa Road'

#     # Thika Route
#     westlands = routes[['Date','Trip','Westlands']]
#     westlands['Westlands Branches'] = westlands['Westlands'].str.split(',')
#     westlands = westlands.explode('Westlands Branches')

#     # Thika Time back at Hq based on the full/partial trips
#     westlandsroad = riders[['trip_date','trip','westlands_time_back_at_hq']]
#     west = westlands[['Date','Trip','Westlands Branches']].rename(columns={'Date':'trip_date','Trip':'trip'})
#     we = pd.merge(westlandsroad, west, on=['trip_date','trip'], how='left')


#     # Merge based on the date and branch
#     wes = we['Westlands Branches'].to_list()
#     westlands = receiving[receiving['ods_outlet'].isin(wes)]
#     westlands.sort_values(by=['odsc_date'], ascending=True, inplace=True)
#     print(westlands['doc_no'].nunique())

#     # Thika Receiving
#     westlands = pd.merge(westlands, we, left_on=['odsc_date','ods_outlet'], right_on=['trip_date','Westlands Branches'], how='left')
#     print(westlands['doc_no'].nunique())

#     # Selecting the trip that came with the orders
#     westlands['westlands_time_back_at_hq2'] = westlands.groupby('doc_no', as_index=False)['westlands_time_back_at_hq'].shift(-1)

#     # Fill the nulls with the closing time for OHO 
#     westlands['westlands_time_back_at_hq2'] = westlands['westlands_time_back_at_hq2'].fillna('18:00:00')

#     # thika['odsc_time']= pd.to_datetime(thika['odsc_time']).dt.time
#     westlands['westlands_time_back_at_hq']= pd.to_datetime(westlands['westlands_time_back_at_hq']).dt.time
#     westlands['westlands_time_back_at_hq2']= pd.to_datetime(westlands['westlands_time_back_at_hq2']).dt.time

#     # Pick the correct trip the orders came with 
#     westlands['CorrectTrip'] = np.where((westlands['odsc_time'] >= westlands['westlands_time_back_at_hq']) & (westlands['odsc_time'] <= westlands['westlands_time_back_at_hq2']) | (westlands['odsc_time'] >= westlands['westlands_time_back_at_hq']) & (westlands['westlands_time_back_at_hq2'].isnull()),1,0)

#     westlands['region'] = 'Westlands'

#     # Thika Route
#     rongai = routes[['Date','Trip','Rongai']]
#     rongai['Rongai Branches'] = rongai['Rongai'].str.split(',')
#     rongai = rongai.explode('Rongai Branches')

#     # Thika Time back at Hq based on the full/partial trips
#     rongairoad = riders[['trip_date','trip','rongai_time_back_at_hq']]
#     rong = rongai[['Date','Trip','Rongai Branches']].rename(columns={'Date':'trip_date','Trip':'trip'})
#     ron = pd.merge(rongairoad, rong, on=['trip_date','trip'], how='left')

#     # Merge based on the date and branch
#     rons = ron['Rongai Branches'].to_list()
#     rongai = receiving[receiving['ods_outlet'].isin(rons)]
#     rongai.sort_values(by=['odsc_date'], ascending=True, inplace=True)
#     print(rongai['doc_no'].nunique())

#     # Thika Receiving
#     rongai = pd.merge(rongai, ron, left_on=['odsc_date','ods_outlet'], right_on=['trip_date','Rongai Branches'], how='left')
#     print(rongai['doc_no'].nunique())

#     # Selecting the trip that came with the orders
#     rongai['rongai_time_back_at_hq2'] = rongai.groupby('doc_no', as_index=False)['rongai_time_back_at_hq'].shift(-1)

#     # Fill the nulls with the closing time for OHO 
#     rongai['rongai_time_back_at_hq2'] = rongai['rongai_time_back_at_hq2'].fillna('18:00:00')

#     # thika['odsc_time']= pd.to_datetime(thika['odsc_time']).dt.time
#     rongai['rongai_time_back_at_hq']= pd.to_datetime(rongai['rongai_time_back_at_hq']).dt.time
#     rongai['rongai_time_back_at_hq2']= pd.to_datetime(rongai['rongai_time_back_at_hq2']).dt.time

#     # Pick the correct trip the orders came with 
#     rongai['CorrectTrip'] = np.where((rongai['odsc_time'] >= rongai['rongai_time_back_at_hq']) & (rongai['odsc_time'] <= rongai['rongai_time_back_at_hq2']) | (rongai['odsc_time'] >= rongai['rongai_time_back_at_hq']) & (rongai['rongai_time_back_at_hq2'].isnull()),1,0)

#     rongai['region'] = 'Rongai'

#     # Picking only the correct trips for now
#     rongai = rongai[rongai['CorrectTrip']==1]
#     westlands = westlands[westlands['CorrectTrip']==1]
#     mombasa = mombasa[mombasa['CorrectTrip']==1]
#     eastlands = eastlands[eastlands['CorrectTrip']==1]
#     karen = karen[karen['CorrectTrip']==1]
#     thika = thika[thika['CorrectTrip']==1]

#     # Converting the times to datetimes
#     rongai['rongai_time'] = pd.to_datetime(rongai['trip_date'].astype(str) + ' ' + rongai['rongai_time_back_at_hq'].astype(str))
#     westlands['westlands_time'] = pd.to_datetime(westlands['trip_date'].astype(str) + ' ' + westlands['westlands_time_back_at_hq'].astype(str))
#     mombasa['mombasa_time'] = pd.to_datetime(mombasa['trip_date'].astype(str) + ' ' + mombasa['mombasa_time_back_at_hq'].astype(str))
#     eastlands['eastlands_time'] = pd.to_datetime(eastlands['trip_date'].astype(str) + ' ' + eastlands['eastlands_time_back_at_hq'].astype(str))
#     karen['karen_time'] = pd.to_datetime(karen['trip_date'].astype(str) + ' ' + karen['karen_time_back_at_hq'].astype(str))
#     thika['thika_time'] = pd.to_datetime(thika['trip_date'].astype(str) + ' ' + thika['thika_time_back_at_hq'].astype(str))

#     # Renaming the columns
#     rongai.rename(columns={'rongai_time':'receiving_time'}, inplace=True)
#     westlands.rename(columns={'westlands_time':'receiving_time'}, inplace=True)
#     mombasa.rename(columns={'mombasa_time':'receiving_time'}, inplace=True)
#     eastlands.rename(columns={'eastlands_time':'receiving_time'}, inplace=True)
#     karen.rename(columns={'karen_time':'receiving_time'}, inplace=True)
#     upcountry.rename(columns={'upcountry_time':'receiving_time'}, inplace=True)
#     thika.rename(columns={'thika_time':'receiving_time'}, inplace=True)

#     rongai = rongai[['doc_entry','odsc_date','odsc_time','odsc_status','odsc_createdby','doc_no','cust_code','ods_outlet','DateTime','trip_date','receiving_time','order_criteria_status','region']]
#     westlands = westlands[['doc_entry','odsc_date','odsc_time','odsc_status','odsc_createdby','doc_no','cust_code','ods_outlet','DateTime','trip_date','receiving_time','order_criteria_status','region']]
#     mombasa = mombasa[['doc_entry','odsc_date','odsc_time','odsc_status','odsc_createdby','doc_no','cust_code','ods_outlet','DateTime','trip_date','receiving_time','order_criteria_status','region']]
#     eastlands = eastlands[['doc_entry','odsc_date','odsc_time','odsc_status','odsc_createdby','doc_no','cust_code','ods_outlet','DateTime','trip_date','receiving_time','order_criteria_status','region']]
#     karen = karen[['doc_entry','odsc_date','odsc_time','odsc_status','odsc_createdby','doc_no','cust_code','ods_outlet','DateTime','trip_date','receiving_time','order_criteria_status','region']]
#     upcountry = upcountry[['doc_entry','odsc_date','odsc_time','odsc_status','odsc_createdby','doc_no','cust_code','ods_outlet','DateTime','trip_date','receiving_time','order_criteria_status','region']]
#     thika = thika[['doc_entry','odsc_date','odsc_time','odsc_status','odsc_createdby','doc_no','cust_code','ods_outlet','DateTime','trip_date','receiving_time','order_criteria_status','region']]

#     receiving = pd.concat([thika,westlands,mombasa,eastlands,karen,upcountry,rongai], axis=0)
#     print(receiving)
   



#     ##Fetch Holidays
#     data = pd.read_sql("""
#     SELECT holiday_date, holiday_name
#     FROM mabawa_dw.dim_holidays;
#     """, con=engine)
    
#     print("Fetched Holidays")

#     workday = businesstimedelta.WorkDayRule(
#         start_time=datetime.time(9),
#         end_time=datetime.time(18),
#         working_days=[0,1, 2, 3, 4])

#     saturday = businesstimedelta.WorkDayRule(
#         start_time=datetime.time(9),
#         end_time=datetime.time(16),
#         working_days=[5])
    
#     vic_holidays = pyholidays.KE()
#     holidays = businesstimedelta.HolidayRule(vic_holidays)
#     from workalendar.africa import Kenya
#     cal = Kenya()
#     # hl = cal.holidays()
#     hl = data.values.tolist()
#     my_dict=dict(hl)
#     vic_holidays=vic_holidays.append(my_dict)
#     # businesshrs = businesstimedelta.Rules([workday, holidays])
#     print(vic_holidays)
    
#     businesshrs = businesstimedelta.Rules([workday, saturday, holidays])

#     def BusHrs(start, end):
#         if end>=start:
#             return float(businesshrs.difference(start,end).hours)*float(60)+float(businesshrs.difference(start,end).seconds)/float(60)
#         else:
#             return 0
 
#     # Add a cut-off column (15 mins)
#     receiving['cutoff'] = np.where(receiving['region']=='Upcountry',30,15)
#     # receiving['user'] = 'Receiving'
#     receiving['duration'] = receiving.apply(lambda row: BusHrs(row['receiving_time'], row['DateTime']), axis=1)
    
#     # Hourly Receiving Data
#     receiving['hour'] = receiving['receiving_time'].dt.hour


#     # Truncate the existing table before appending the new table
#     truncate_table = """drop table mabawa_staging.source_receiving_data;"""
#     truncate_table = pg_execute(truncate_table)

#     receiving.to_sql('source_receiving_data', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)   
#     print()
#     print('Receiving information has been successfully appended')

# receiving()
