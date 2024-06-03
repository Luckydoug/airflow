from airflow.models import Variable
from sub_tasks.data.connect import engine
from sub_tasks.data.connect_voler import engine as rwanda_engine
from sub_tasks.data.connect_mawingu import engine as uganda_engine
from pangres import upsert
from sub_tasks.libraries.utils import service_file
import pandas as pd
import pygsheets
import sys

sys.path.append(".")


def dateq(row):
    if str(row['Create Date']).__contains__('/'):
        try:
            if len(row['Create Date'].split('/')[2]>3):
                if int(row['Create Date']).split('/')[1]>12:
                    return pd.to_datetime(row['Create Date'],format='%m/%d/%Y')
                return pd.to_datetime(row['Create Date'],dayfirst=True)
            elif len(row['Create Date'].split('/')[2]<3):
                if int(row['Create Date']).split('/')[1]>12:
                    return pd.to_datetime(row['Create Date'],format='%m/%d/%y')
                return pd.to_datetime(row['Create Date'],dayfirst=True)
        except:
            return pd.to_datetime(row['Create Date'],errors='coerce')

    if str(row['Create Date']).__contains__('.'):
        try:
            if len(row['Create Date'].split('.')[2]>3):
                if int(row['Create Date']).split('.')[1]>12:
                    return pd.to_datetime(row['Create Date'],format='%m.%d.%Y')
                return pd.to_datetime(row['Create Date'],dayfirst=True)
            elif len(row['Create Date'].split('.')[2]<3):
                if int(row['Create Date']).split('.')[1]>12:
                    return pd.to_datetime(row['Create Date'],format='%m.%d.%y')
                return pd.to_datetime(row['Create Date'],dayfirst=True)
        except:
            return pd.to_datetime(row['Create Date'],errors='coerce')
    if str(row['Create Date']).__contains__('-'):
        try:
            if len(row['Create Date'].split('-')[2]>3):
                if int(row['Create Date']).split('-')[1]>12:
                    return pd.to_datetime(row['Create Date'],format='%m-%d-%Y')
                return pd.to_datetime(row['Create Date'],dayfirst=True)
            elif len(row['Create Date'].split('-')[2]<3):
                if int(row['Create Date']).split('-')[1]>12:
                    return pd.to_datetime(row['Create Date'],format='%m-%d-%y')
                return pd.to_datetime(row['Create Date'],dayfirst=True)
        except:
            return pd.to_datetime(row['Create Date'],dayfirst=True)
    else:
        return pd.to_datetime(row['Create Date'],dayfirst=True)
    
    

def callque(row):
    if str(row['Date of call']).__contains__('/'):
        try:
            if len(row['Date of call'].split('/')[2])>3:
                if int(row['Date of call'].split('/')[1])>12:
                    return pd.to_datetime(row['Date of call'],format='%m/%d/%Y')
                return pd.to_datetime(row['Date of call'],dayfirst=True)
            elif len(row['Date of call'].split('/')[2])<3:
                if int(row['Date of call'].split('/')[1])>12:
                    return pd.to_datetime(row['Date of call'],format='%m/%d/%y')
                return pd.to_datetime(row['Date of call'],dayfirst=True)
        except:
            return pd.to_datetime(row['Date of call'],dayfirst=True)
    if str(row['Date of call']).__contains__('.'):
                
        try:
            if len(row['Date of call'].split('.')[2])>3:
                if int(row['Date of call'].split('.')[1])>12:
                    return pd.to_datetime(row['Date of call'],format='%m.%d.%Y')
                return pd.to_datetime(row['Date of call'],dayfirst=True)
            elif len(row['Date of call'].split('.')[2])<3:
                if int(row['Date of call'].split('.')[1])>12:
                    return pd.to_datetime(row['Date of call'],format='%m.%d.%y')
                return pd.to_datetime(row['Date of call'],dayfirst=True)
        except:
            return pd.to_datetime(row['Date of call'],dayfirst=True)
    
    if str(row['Date of call']).__contains__('-'):
                
        try:
            if len(row['Date of call'].split('.')[2])>3:
                if int(row['Date of call'].split('-')[1])>12:
                    return pd.to_datetime(row['Date of call'],format='%m-%d-%Y')
                return pd.to_datetime(row['Date of call'],dayfirst=True)
            elif len(row['Date of call'].split('-')[2])<3:
                if int(row['Date of call'].split('-')[1])>12:
                    return pd.to_datetime(row['Date of call'],format='%m-%d-%y')
                return pd.to_datetime(row['Date of call'],dayfirst=True)
        except:
            return pd.to_datetime(row['Date of call'],dayfirst=True)
    
    else:
        return pd.to_datetime(row['Date of call'],errors='coerce')

def fetch_optom_queue_data():
   gc = pygsheets.authorize(service_file=service_file)
   sq = gc.open_by_key('1nGVDxrOmcmkKeBI58-sqteA-FomxXMM7xFw84o8wwIU')
   sq = pd.DataFrame(sq.worksheet_by_title("Non-Converted Optom Queue Data").get_all_records())
   sq['Create Date'] = sq.apply(lambda row: dateq(row),axis=1)
   sq['Date of call'] = sq['Date of call'].str.replace('04/13/0202','04/13/2022')
   sq['Date of call'] = sq.apply(lambda row: callque(row),axis=1)
   print(sq)
  
   sq.rename(
      columns={
         "Loyalty Code": "loyalty_code",
         "Create Date": "create_date",
         "OutLet ID": "outlet_id",
         "Optom Name": "optom_name",
         "Status": "status",
         "Mobile Number": "mobile_number",
         "Date of call": "date_of_call",
         "Call By?": "call_by",
         "Time of Call": "time_of_call",
         "Customer Remarks": "customer_remarks",
         "Additional Comments": "additional_comments",
         "To come back with Commitment": "to_come_back_with_commitment",
         "Action Points": "action_points",
         "Coming Back Commitment": "coming_back_commitment",
         "Optom Explanation": "optom_explanation"
      },
      inplace=True,
      )
   
   sq['loyalty_codeandcreate_date'] = sq['loyalty_code'].astype(str) + ' ' + sq['create_date'].astype(str) + ' ' + sq['outlet_id'].astype(str)
   sq = sq.drop_duplicates(subset='loyalty_codeandcreate_date',keep = 'first')
   sq = sq.set_index('loyalty_codeandcreate_date')   

   upsert(
      engine=engine,
      df=sq,
      schema="reports_tables",
      table_name="internalaudit_optomqueue",
      if_row_exists="update",
      create_table=False,
   )

def datehold(row):
    if str(row['Eye Test Date']).__contains__('/'):
        try:
            if len(row['Eye Test Date'].split('/')[2]>3):
                if int(row['Eye Test Date']).split('/')[1]>12:
                    return pd.to_datetime(row['Eye Test Date'],format='%m/%d/%Y')
                return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
            elif len(row['Eye Test Date'].split('/')[2]<3):
                if int(row['Eye Test Date']).split('/')[1]>12:
                    return pd.to_datetime(row['Eye Test Date'],format='%m/%d/%y')
                return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
        except:
            return pd.to_datetime(row['Eye Test Date'],errors='coerce')

    if str(row['Eye Test Date']).__contains__('.'):
        try:
            if len(row['Eye Test Date'].split('.')[2]>3):
                if int(row['Eye Test Date']).split('.')[1]>12:
                    return pd.to_datetime(row['Eye Test Date'],format='%m.%d.%Y')
                return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
            elif len(row['Eye Test Date'].split('.')[2]<3):
                if int(row['Eye Test Date']).split('.')[1]>12:
                    return pd.to_datetime(row['Eye Test Date'],format='%m.%d.%y')
                return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
        except:
            return pd.to_datetime(row['Eye Test Date'],errors='coerce')
    if str(row['Eye Test Date']).__contains__('-'):
        try:
            if len(row['Eye Test Date'].split('-')[2]>3):
                if int(row['Eye Test Date']).split('-')[1]>12:
                    return pd.to_datetime(row['Eye Test Date'],format='%m-%d-%Y')
                return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
            elif len(row['Eye Test Date'].split('-')[2]<3):
                if int(row['Eye Test Date']).split('-')[1]>12:
                    return pd.to_datetime(row['Eye Test Date'],format='%m-%d-%y')
                return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
        except:
            return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
    else:
        return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
       
def callhold(row):
    if str(row['Date of Call']).__contains__('/'):
        try:
            if len(row['Date of Call'].split('/')[2])>3:
                if int(row['Date of Call'].split('/')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m/%d/%Y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
            elif len(row['Date of Call'].split('/')[2])<3:
                if int(row['Date of Call'].split('/')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m/%d/%y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
        except:
            return pd.to_datetime(row['Date of Call'],dayfirst=True)
    if str(row['Date of Call']).__contains__('.'):
                
        try:
            if len(row['Date of Call'].split('.')[2])>3:
                if int(row['Date of Call'].split('.')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m.%d.%Y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
            elif len(row['Date of Call'].split('.')[2])<3:
                if int(row['Date of Call'].split('.')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m.%d.%y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
        except:
            return pd.to_datetime(row['Date of Call'],dayfirst=True)
    
    if str(row['Date of Call']).__contains__('-'):
                
        try:
            if len(row['Date of Call'].split('.')[2])>3:
                if int(row['Date of Call'].split('-')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m-%d-%Y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
            elif len(row['Date of Call'].split('-')[2])<3:
                if int(row['Date of Call'].split('-')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m-%d-%y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
        except:
            return pd.to_datetime(row['Date of Call'],dayfirst=True)
    
    else:
        return pd.to_datetime(row['Date of Call'],errors='coerce')


def fetch_eyetests_on_hold():
   gc = pygsheets.authorize(service_file=service_file)
   sh = gc.open_by_key('1nGVDxrOmcmkKeBI58-sqteA-FomxXMM7xFw84o8wwIU')
   sh = pd.DataFrame(sh.worksheet_by_title("Eye Tests Put on Hold").get_all_records())
   sh['Eye Test Date'] = sh.apply(lambda row: datehold(row),axis=1)
   sh['Date of Call'] = sh['Date of Call'].replace('11-10-0222','11-10-2022').replace('18-03-203','18-03-2023')
   sh['Date of Call'] = sh.apply(lambda row: callhold(row),axis=1)

   print(sh)

   sh.rename(
      columns={
         "Loyalty Code": "loyalty_code",
         "Eye Test Date": "create_date",
         "Branch": "branch",
         "Optom Name": "optom_name",
         "Customer Name": "customer_name",
         "Mobile Number": "mobile_number",
         "Date of Call": "date_of_call",
         "Call By": "call_by",
         "Time of Call": "time_of_call",
         "Book Reasons": "book_reasons",
         "Customer Remarks": "customer_remarks",
         "Additional Comments": "additional_comments",
         "Department to Escalate To": "department_to_escalate_to",
         "Action Points": "action_points",
        
        
      },
      inplace=True,
      )
   sh['loyalty_codeandcreate_date'] = sh['loyalty_code'].astype(str) + ' ' + sh['create_date'].astype(str) + ' ' + sh['branch'].astype(str)
  
   sh= sh.drop_duplicates(subset='loyalty_codeandcreate_date',keep = 'first')
   sh = sh.set_index('loyalty_codeandcreate_date')

   upsert(
      engine=engine,
      df=sh,
      schema="reports_tables",
      table_name="internalaudit_eyetestonhold",
      if_row_exists="update",
      create_table=False,
   )  


# def datecall(row):
#     if str(row['Date of Call']).__contains__('/'):
#         try:
#             if len(row['Date of Call'].split('/')[2])>3:
#                 if int(row['Date of Call'].split('/')[1])>12:
#                     return pd.to_datetime(row['Date of Call'],format='%m/%d/%Y')
#                 return pd.to_datetime(row['Date of Call'],dayfirst=True)
#             elif len(row['Date of Call'].split('/')[2])<3:
#                 if int(row['Date of Call'].split('/')[1])>12:
#                     return pd.to_datetime(row['Date of Call'],format='%m/%d/%y')
#                 return pd.to_datetime(row['Date of Call'],dayfirst=True)
#         except:
#             return pd.to_datetime(row['Date of Call'],dayfirst=True)
#     if str(row['Date of Call']).__contains__('.'):
                
#         try:
#             if len(row['Date of Call'].split('.')[2])>3:
#                 if int(row['Date of Call'].split('.')[1])>12:
#                     return pd.to_datetime(row['Date of Call'],format='%m.%d.%Y')
#                 return pd.to_datetime(row['Date of Call'],dayfirst=True)
#             elif len(row['Date of Call'].split('.')[2])<3:
#                 if int(row['Date of Call'].split('.')[1])>12:
#                     return pd.to_datetime(row['Date of Call'],format='%m.%d.%y')
#                 return pd.to_datetime(row['Date of Call'],dayfirst=True)
#         except:
#             return pd.to_datetime(row['Date of Call'],dayfirst=True)
    
#     if str(row['Date of Call']).__contains__('-'):
                
#         try:
#             if len(row['Date of Call'].split('.')[2])>3:
#                 if int(row['Date of Call'].split('-')[1])>12:
#                     return pd.to_datetime(row['Date of Call'],format='%m-%d-%Y')
#                 return pd.to_datetime(row['Date of Call'],dayfirst=True)
#             elif len(row['Date of Call'].split('-')[2])<3:
#                 if int(row['Date of Call'].split('-')[1])>12:
#                     return pd.to_datetime(row['Date of Call'],format='%m-%d-%y')
#                 return pd.to_datetime(row['Date of Call'],dayfirst=True)
#         except:
#             return pd.to_datetime(row['Date of Call'],dayfirst=True)
    
#     else:
#         return pd.to_datetime(row['Date of Call'],errors='coerce')

# def datess(row):
#     if str(row['Order Date']).__contains__('/'):
#         return pd.to_datetime(row['Order Date'],format='%d/%m/%Y')
#     elif str(row['Order Date']).__contains__('.'):
#         return pd.to_datetime(row['Order Date'],format='%d.%m.%y')
#     elif str(row['Order Date']).__contains__('-'):
#         return pd.to_datetime(row['Order Date'],format='%d-%m-%y')
#     else:
#         return pd.to_datetime(row['Order Date'],errors='coerce')
def datecall(row):
    if (row['Order Date'].year==2022) & (row['Order Date'].month in [1,2,3]):
        try:
            return pd.to_datetime(row['Date of Call'],dayfirst=False)
        except:
            return pd.to_datetime(row['Date of Call'],dayfirst=True)
    elif (row['Order Date'].year==2023) & (row['Order Date'].month ==1):
        try:
            return pd.to_datetime(row['Date of Call'],dayfirst=False)
        except:
            return pd.to_datetime(row['Date of Call'],dayfirst=True)
    else:
        return pd.to_datetime(row['Date of Call'],dayfirst=True)

def fetch_frame_only_sales():
   gc = pygsheets.authorize(service_file=service_file)
   sh = gc.open_by_key('1nGVDxrOmcmkKeBI58-sqteA-FomxXMM7xFw84o8wwIU')
   sh = pd.DataFrame(sh.worksheet_by_title("FR Only Sales").get_all_records())

   #sh['Order Date'] = sh['Order Date'].str.replace('05/01/2022','05/01/22')
   sh['Order Date'] = pd.to_datetime(sh['Order Date'],dayfirst=True)

   sh['Date of Call'] = sh['Date of Call'].str.replace('18-03-20223','18-03-2023')
   sh['Date of Call'] = sh.apply(lambda row: datecall(row),axis=1)

   sh.rename(
      columns={
         "Loyalty Code": "loyalty_code",
         "Order Date": "create_date",
         "Outlet": "outlet_id",
         "Order Number":"order_number",
         "Normal(Or)Repair Order":"normal_repair_order",
         "Customer Name": "customer_name",
         "Mobile Number": "mobile_number",
         "Date of Call": "date_of_call",
         "Call By?": "call_by",
         "Time of Call": "time_of_call",
         "Branch response": "branch_response",
         "Customer Remarks": "customer_remarks",
         "Additional Comments": "additional_comments",
         "Department to Escalate To": "department_to_escalate_to",
         "Action Points": "action_points"     
        
      },
      inplace=True,
      )
  
   sh= sh.drop_duplicates(subset='order_number',keep = 'first')
   sh = sh.set_index('order_number')

   upsert(
      engine=engine,
      df=sh,
      schema="reports_tables",
      table_name="internalaudit_frameonlysales",
      if_row_exists="update",
      create_table=False,
   ) 

def datesR(row):
    
    if str(row['Registration Date']).__contains__('/'):
        if int(row["Registration Date"].split("/")[1]) > 12:
            try:
                return pd.to_datetime(row['Registration Date'].replace(" ", ""),format='%m/%d/%Y')
            except:
                return pd.to_datetime(row['Registration Date'].replace(" ", ""),format='%m/%d/%y')
        return pd.to_datetime(row['Registration Date'].replace(" ", ""),format='%d/%m/%Y')
    elif str(row['Registration Date']).__contains__('.'):
        return pd.to_datetime(row['Registration Date'],format='%d.%m.%y')
    
    elif str(row['Registration Date']).__contains__('-'):
        
        try:

            if int(row["Registration Date"].split("-")[1]) > 12:
                if len(row["Registration Date"].split("-")[2]) > 3:
                    return pd.to_datetime(row['Registration Date'].replace(" ", ""),format='%m-%d-%Y')
                return pd.to_datetime(row['Registration Date'].replace(" ", ""),format='%m-%d-%y')
        except:
            return pd.to_datetime(row['Registration Date'], errors = "coerce")

        if len(str(row["Registration Date"].split("-")[2])) > 3:
 
            try:
                return pd.to_datetime(row['Registration Date'].replace(" ", ""),format='%m-%d-%Y')
            except:
                return pd.to_datetime(row['Registration Date'].replace(" ", ""), format='%d-%m-%Y')
        if int(row["Registration Date"].split("-")[0]) > 12 and int(row["Registration Date"].split("-")[1]) < 12 and len(str(row["Registration Date"].split("-")[2])) > 3:
            
            return pd.to_datetime(row['Registration Date'].replace(" ", ""),format='%d-%m-%Y')
        else:
            return pd.to_datetime(row['Registration Date'], errors = "coerce")
            
        #if (int(row["Date of Call"].split("-")[0]) > 12) and (len(str(row["Date of Call"].split("-")[2])) > 3):
            #return pd.to_datetime(row['Date of Call'].replace(" ", ""),format='%d-%m-%Y')
        return pd.to_datetime(row['Registration Date'],format='%d-%m-%y')
    else:
        return pd.to_datetime(row['Registration Date'], errors = "coerce")

def datecallR(row):
    
    if str(row['Date of Call']).__contains__('/'):
        try:
            if int(row["Date of Call"].split("/")[1]) > 12:
                try:
                    return pd.to_datetime(row['Date of Call'].replace(" ", ""),format='%m/%d/%Y')
                except:
                    return pd.to_datetime(row['Date of Call'].replace(" ", ""),format='%m/%d/%y')
            return pd.to_datetime(row['Date of Call'].replace(" ", ""),format='%d/%m/%Y')
        except ValueError:
            print(row['Date of Call'])
            
    elif str(row['Date of Call']).__contains__('.'):
        return pd.to_datetime(row['Date of Call'],format='%d.%m.%y')
    
    elif str(row['Date of Call']).__contains__('-'):
        print(row['Date of Call'])
        try:

            if int(row["Date of Call"].split("-")[1]) > 12:
                if len(row["Date of Call"].split("-")[2]) > 3:
                    return pd.to_datetime(row['Date of Call'].replace(" ", ""),format='%m-%d-%Y')
                return pd.to_datetime(row['Date of Call'].replace(" ", ""),format='%m-%d-%y')
        except:
            return pd.to_datetime(row['Date of Call'], errors = "coerce")

        if len(str(row["Date of Call"].split("-")[2])) > 3:
 
            try:
                return pd.to_datetime(row['Date of Call'].replace(" ", ""),format='%m-%d-%Y')
            except:
                return pd.to_datetime(row['Date of Call'].replace(" ", ""), format='%d-%m-%Y')
        if int(row["Date of Call"].split("-")[0]) > 12 and int(row["Date of Call"].split("-")[1]) < 12 and len(str(row["Date of Call"].split("-")[2])) > 3:
            
            return pd.to_datetime(row['Date of Call'].replace(" ", ""),format='%d-%m-%Y')
        else:
            return pd.to_datetime(row['Date of Call'], errors = "coerce")
            
        #if (int(row["Date of Call"].split("-")[0]) > 12) and (len(str(row["Date of Call"].split("-")[2])) > 3):
            #return pd.to_datetime(row['Date of Call'].replace(" ", ""),format='%d-%m-%Y')
        return pd.to_datetime(row['Date of Call'],format='%d-%m-%y')
    else:
        return pd.to_datetime(row['Date of Call'], errors = "coerce")

def fetch_registrations_data():
   gc = pygsheets.authorize(service_file=service_file)
   sh = gc.open_by_key('1nGVDxrOmcmkKeBI58-sqteA-FomxXMM7xFw84o8wwIU')
   sh = pd.DataFrame(sh.worksheet_by_title("Non-Converted Registrations").get_all_records())
   sh['Registration Date'] = sh['Registration Date'].str.replace('17-Feb-2022','17-2-2022')
   sh['Date of Call'] = sh['Date of Call'].str.replace('17-Feb-2022','17-2-2022').replace('21/102023','21/10/2023').replace('7/3/3023','7/3/2023').replace('3//29/2022','3/29/2022').replace('4//27/2022','4/27/2022')
   sh['Registration Date'] = sh.apply(lambda row: datesR(row),axis=1)
   sh['Date of Call'] = sh.apply(lambda row: datecallR(row),axis=1)
   print(sh)

   sh.rename(
      columns={
         "Loyalty Code": "loyalty_code",
         "Registration Date": "create_date",
         "Branch": "branch_code",
         "Optom Name":'optom_name',
         "Customer Type":"cust_type",
         "Registration Person":'registration_person',
         "Old/New Customer":"old_or_new",
         "Mobile Number": "mobile_number",
         "Date of Call": "date_of_call",
         "Call By?": "call_by",
         "Time of Call": "time_of_call",
         "Non-converted reason by Branch": "branch_response",
         "Customer Remarks": "customer_remarks",
         "Additional Comments": "additional_comments",
         "Department to Escalate To": "department_to_escalate_to",
         "Action Point": "action_points",
         "Response Given Post Questioning":"response_post_questioning"
        
        
      },
      inplace=True,
      )
   
   sh = sh[['branch_code', 'create_date', 'registration_person', 'loyalty_code',
      'mobile_number', 'cust_type', 'old_or_new', 'branch_response',
      'call_by', 'date_of_call', 'time_of_call', 'customer_remarks',
      'additional_comments', 'action_points', 'department_to_escalate_to',
      'optom_name','response_post_questioning']]

   sh= sh.drop_duplicates(subset='loyalty_code',keep = 'first')
   sh = sh.set_index('loyalty_code')

   upsert(
      engine=engine,
      df=sh,
      schema="reports_tables",
      table_name="internalaudit_registrations",
      if_row_exists="update",
      create_table=False,
   ) 

def daterx(row):
    if str(row['Eye Test Date']).__contains__('/'):
        try:
            if len(row['Eye Test Date'].split('/')[2]>3):
                if int(row['Eye Test Date']).split('/')[1]>12:
                    return pd.to_datetime(row['Eye Test Date'],format='%m/%d/%Y')
                return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
            elif len(row['Eye Test Date'].split('/')[2]<3):
                if int(row['Eye Test Date']).split('/')[1]>12:
                    return pd.to_datetime(row['Eye Test Date'],format='%m/%d/%y')
                return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
        except:
            return pd.to_datetime(row['Eye Test Date'],dayfirst=True)

    if str(row['Eye Test Date']).__contains__('.'):
        try:
            if len(row['Eye Test Date'].split('.')[2]>3):
                if int(row['Eye Test Date']).split('.')[1]>12:
                    return pd.to_datetime(row['Eye Test Date'],format='%m.%d.%Y')
                return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
            elif len(row['Eye Test Date'].split('.')[2]<3):
                if int(row['Eye Test Date']).split('.')[1]>12:
                    return pd.to_datetime(row['Eye Test Date'],format='%m.%d.%y')
                return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
        except:
            return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
    if str(row['Eye Test Date']).__contains__('-'):
        try:
            if len(row['Eye Test Date'].split('-')[2]>3):
                if int(row['Eye Test Date']).split('-')[1]>12:
                    return pd.to_datetime(row['Eye Test Date'],format='%m-%d-%Y')
                return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
            elif len(row['Eye Test Date'].split('-')[2]<3):
                if int(row['Eye Test Date']).split('-')[1]>12:
                    return pd.to_datetime(row['Eye Test Date'],format='%m-%d-%y')
                return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
        except:
            return pd.to_datetime(row['Eye Test Date'],dayfirst=True)
    else:
        return pd.to_datetime(row['Eye Test Date'],errors='coerce')

def callrx(row):
    if str(row['Date of Call']).__contains__('/'):
        try:
            if len(row['Date of Call'].split('/')[2])>3:
                if int(row['Date of Call'].split('/')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m/%d/%Y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
            elif len(row['Date of Call'].split('/')[2])<3:
                if int(row['Date of Call'].split('/')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m/%d/%y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
        except:
            return pd.to_datetime(row['Date of Call'],dayfirst=True)
    if str(row['Date of Call']).__contains__('.'):
                
        try:
            if len(row['Date of Call'].split('.')[2])>3:
                if int(row['Date of Call'].split('.')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m.%d.%Y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
            elif len(row['Date of Call'].split('.')[2])<3:
                if int(row['Date of Call'].split('.')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m.%d.%y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
        except:
            return pd.to_datetime(row['Date of Call'],dayfirst=True)
    
    if str(row['Date of Call']).__contains__('-'):
                
        try:
            if len(row['Date of Call'].split('.')[2])>3:
                if int(row['Date of Call'].split('-')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m-%d-%Y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
            elif len(row['Date of Call'].split('-')[2])<3:
                if int(row['Date of Call'].split('-')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m-%d-%y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
        except:
            return pd.to_datetime(row['Date of Call'],dayfirst=True)
    
    else:
        return pd.to_datetime(row['Date of Call'],errors='coerce')



def fetch_highrxnonconv():
   gc = pygsheets.authorize(service_file=service_file)
   sh = gc.open_by_key('1nGVDxrOmcmkKeBI58-sqteA-FomxXMM7xFw84o8wwIU')
   sh = pd.DataFrame(sh.worksheet_by_title("Non-Converted EyeTest(High RX)").get_all_records())
   sh['Eye Test Date'] = sh.apply(lambda row:daterx(row),axis=1)
   sh['Date of Call']= sh['Date of Call'].replace('209/6/2023','29/6/2023')
   sh['Date of Call'] = sh.apply(lambda row:callrx(row),axis=1)

   sh.rename(
      columns={
         "Loyalty Code": "loyalty_code",
         "Eye Test Date": "create_date",
         "Optom Name":"optom_name",
         "Branch": "branch_code",
         "Customer Type":"cust_type",
         "RX Print":"rx_print",
         "Customer Name":"customer_name",
         "Old/New Customer":"old_or_new",
         "Mobile Number": "mobile_number",
         "Date of Call": "date_of_call",
         "Call By": "call_by",
         "Time of Call": "time_of_call",
         "Non-converted Remark by Branch": "branch_response",
         "Customer Remarks": "customer_remarks",
         "Addtional Comments": "additional_comments",
         "Department to Escalate To": "department_to_escalate_to",
         "Action Taken": "action_taken",
         "Branch Response":"branch_response2"      
        
      },
      inplace=True,
      )
      

   sh = sh [['branch_code', 'create_date', 'optom_name', 'old_or_new',
       'loyalty_code', 'rx_print', 'customer_name', 'branch_response',
       'mobile_number', 'call_by', 'date_of_call', 'time_of_call', 'cust_type',
       'customer_remarks','additional_comments', 'action_taken', 'branch_response2',
       'department_to_escalate_to'
      ]]
   
   sh['loyalty_codeandcreate_date'] = sh['loyalty_code'].astype(str) + ' ' + sh['create_date'].astype(str) + ' ' + sh['branch_code'].astype(str)
  
   sh= sh.drop_duplicates(subset='loyalty_codeandcreate_date',keep = 'first')

   sh = sh.set_index('loyalty_codeandcreate_date')

   upsert(
      engine=engine,
      df=sh,
      schema="reports_tables",
      table_name="internalaudit_highrxdata",
      if_row_exists="update",
      create_table=False,
   ) 

def dateoph(row):
    if str(row['Visit Date']).__contains__('/'):
        try:
            if len(row['Visit Date'].split('/')[2])>3:
                if int(row['Visit Date'].split('/')[1])>12:
                    return pd.to_datetime(row['Visit Date'],format='%m/%d/%Y')
                return pd.to_datetime(row['Visit Date'],dayfirst=True)
            elif len(row['Visit Date'].split('/')[2])<3:
                if int(row['Visit Date'].split('/')[1])>12:
                    return pd.to_datetime(row['Visit Date'],format='%m/%d/%y')
                return pd.to_datetime(row['Visit Date'],dayfirst=True)
        except:
            return pd.to_datetime(row['Visit Date'],dayfirst=True)
    if str(row['Visit Date']).__contains__('.'):
                
        try:
            if len(row['Visit Date'].split('.')[2])>3:
                if int(row['Visit Date'].split('.')[1])>12:
                    return pd.to_datetime(row['Visit Date'],format='%m.%d.%Y')
                return pd.to_datetime(row['Visit Date'],dayfirst=True)
            elif len(row['Visit Date'].split('.')[2])<3:
                if int(row['Visit Date'].split('.')[1])>12:
                    return pd.to_datetime(row['Visit Date'],format='%m.%d.%y')
                return pd.to_datetime(row['Visit Date'],dayfirst=True)
        except:
            return pd.to_datetime(row['Visit Date'],dayfirst=True)
    
    if str(row['Visit Date']).__contains__('-'):
                
        try:
            if len(row['Visit Date'].split('.')[2])>3:
                if int(row['Visit Date'].split('-')[1])>12:
                    return pd.to_datetime(row['Visit Date'],format='%m-%d-%Y')
                return pd.to_datetime(row['Visit Date'],dayfirst=True)
            elif len(row['Visit Date'].split('-')[2])<3:
                if int(row['Visit Date'].split('-')[1])>12:
                    return pd.to_datetime(row['Visit Date'],format='%m-%d-%y')
                return pd.to_datetime(row['Visit Date'],dayfirst=True)
        except:
            return pd.to_datetime(row['Visit Date'],dayfirst=True)
    else:
        return pd.to_datetime(row['Visit Date'],errors='coerce')

def calloph(row):
    if str(row['Date of Call']).__contains__('/'):
        try:
            if len(row['Date of Call'].split('/')[2])>3:
                if int(row['Date of Call'].split('/')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m/%d/%Y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
            elif len(row['Date of Call'].split('/')[2])<3:
                if int(row['Date of Call'].split('/')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m/%d/%y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
        except:
            return pd.to_datetime(row['Date of Call'],dayfirst=True)
    if str(row['Date of Call']).__contains__('.'):
                
        try:
            if len(row['Date of Call'].split('.')[2])>3:
                if int(row['Date of Call'].split('.')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m.%d.%Y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
            elif len(row['Date of Call'].split('.')[2])<3:
                if int(row['Date of Call'].split('.')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m.%d.%y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
        except:
            return pd.to_datetime(row['Date of Call'],dayfirst=True)
    
    if str(row['Date of Call']).__contains__('-'):
                
        try:
            if len(row['Date of Call'].split('.')[2])>3:
                if int(row['Date of Call'].split('-')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m-%d-%Y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
            elif len(row['Date of Call'].split('-')[2])<3:
                if int(row['Date of Call'].split('-')[1])>12:
                    return pd.to_datetime(row['Date of Call'],format='%m-%d-%y')
                return pd.to_datetime(row['Date of Call'],dayfirst=True)
        except:
            return pd.to_datetime(row['Date of Call'],dayfirst=True)
    else:
        return pd.to_datetime(row['Date of Call'],errors='coerce')

def fetch_ophreferrals():
   gc = pygsheets.authorize(service_file=service_file)
   sh = gc.open_by_key('1nGVDxrOmcmkKeBI58-sqteA-FomxXMM7xFw84o8wwIU')
   sh = pd.DataFrame(sh.worksheet_by_title("Ophthalmologists Referral").get_all_records())
   sh['Visit Date'] = sh.apply(lambda row:dateoph(row),axis=1)
   sh['Date of Call'] = sh['Date of Call'].replace('11-12023','11-01-2023').replace('10-2-0203','10-02-2023').replace('04-05-0203','04-05-2023')
   sh['Date of Call'] = sh.apply(lambda row:calloph(row),axis=1)
   print(sh)

   sh.rename(
      columns={
         "Customer Code": "loyalty_code",
         "Visit Date": "create_date",
         "Branch": "branch_code",
         "Partner Ophth Name":"partner_ophth",
         "Hospital/Clinic Referred":"hospital_referred",
         "Mobile No": "mobile_number",
         "Date of Call": "date_of_call",
         "Call By?": "call_by",
         "Time of call": "time_of_call",
         "Ophal Note":"oph_note",
         "Customer Remarks": "customer_remarks",
         "Additional Comments": "additional_comments",
         "Optom Explaination":"optom_explanation",
         "Department to Escalate To": "department_to_escalate_to",
         "Action Points": "action_points"      
      },
      inplace=True,
      )
   sh = sh[['create_date', 'branch_code', 'loyalty_code',
       'mobile_number', 'partner_ophth', 'call_by', 'date_of_call',
       'time_of_call', 'hospital_referred', 'customer_remarks',
       'additional_comments', 'oph_note', 'action_points',
       'optom_explanation', 'department_to_escalate_to'
       ]]

   sh['loyalty_codeandcreate_date'] = sh['loyalty_code'].astype(str) + ' ' + sh['create_date'].astype(str) + ' ' + sh['branch_code'].astype(str)
  
   sh= sh.drop_duplicates(subset='loyalty_codeandcreate_date',keep = 'first')
   sh = sh.set_index('loyalty_codeandcreate_date')

   upsert(
      engine=engine,
      df=sh,
      schema="reports_tables",
      table_name="internalaudit_ophreferrals",
      if_row_exists="update",
      create_table=False,
   )


