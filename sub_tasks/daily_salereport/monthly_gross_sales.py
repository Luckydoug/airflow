import sys
import numpy as np
sys.path.append(".")
import psycopg2
import pandas as pd
from airflow.models import Variable
from datetime import date
import datetime
import pandas as pd
from sub_tasks.data.connect import pg_execute 
# from sub_tasks.api_login.api_login import(login)
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")

def create_gross_payments():
    
    query = """truncate mabawa_dw.gross_payments;
        insert into mabawa_dw.gross_payments
        SELECT branch_code,warehouse_name, month_year,"year","month",new_mode_of_payment, amount
        FROM mabawa_mviews.v_gross_payments"""
    
    query = pg_execute(query) 
    print('truncate finished')  


def summary_gross_payments():
    grosspayments = """
    SELECT branch_code as "Branch", warehouse_name as "Branch name" ,month_year,case when month like '%March%' then replace(month,'March','Mar')
        when month like '%Sept%' then replace(month,'Sept','Sep') else month end as month,year,
        new_mode_of_payment, amount
    FROM mabawa_dw.gross_payments
    where "year" >= 2020;
    """
    grosspayments = pd.read_sql_query(grosspayments,con=conn)
    
    ###Create a pivot table
    grosspayments_pivot = grosspayments.pivot_table(index = ['Branch name','year'],columns = ["month","new_mode_of_payment"],aggfunc = {"amount":np.sum})
    grosspayments_pivot = grosspayments_pivot.droplevel([0],axis = 1).reset_index()
    grosspayments_pivot = grosspayments_pivot.fillna(0)
    (grosspayments_pivot.iloc[:, grosspayments_pivot.columns != ('Branch name',          '')]) = grosspayments_pivot.iloc[:, grosspayments_pivot.columns != ('Branch name',          '')].astype("int64")
    
    #Add the column Total
    for month in grosspayments_pivot.columns.levels[0][:-2]:
        col_name = (month, 'Total')
        result = (grosspayments_pivot[(month, 'Cash')] + grosspayments_pivot[(month, 'Insurance')])
        grosspayments_pivot.insert(grosspayments_pivot.columns.get_loc((month, 'Insurance')) + 1, col_name, result)

    month = ['Branch name',"year","Jan","Feb","Mar","Apr","May","June","July","Aug","Sep","Oct","Nov","Dec"]
    grosspayments_pivot = grosspayments_pivot.reindex(level=0,columns = month)   
    cash_cols = [col for col in grosspayments_pivot.columns if col[1] == 'Cash']
    insurance_cols = [col for col in grosspayments_pivot.columns if col[1] == 'Insurance']
    total_cols = [col for col in grosspayments_pivot.columns if col[1] == 'Total']
    grosspayments_pivot['Insurance Total'] = grosspayments_pivot[insurance_cols].sum(axis=1)
    grosspayments_pivot['Cash Total'] = grosspayments_pivot[cash_cols].sum(axis=1)
    grosspayments_pivot['Cumulative Total'] = grosspayments_pivot[total_cols].sum(axis=1)

    ##Clear the first branch
    grosspayments_pivot['Branch name'] = grosspayments_pivot['Branch name'].where(~grosspayments_pivot['Branch name'].duplicated(), '')
    
    ####################Get the gross payments since 2019
    grosspaymentsall = """
    SELECT branch_code, warehouse_name as "Branch name" , month_year,case when month like '%March%' then replace(month,'March','Mar')
        when month like '%Sept%' then replace(month,'Sept','Sep') else month end as month,year, 
        new_mode_of_payment, amount
    FROM mabawa_dw.gross_payments;
    """
    grosspaymentsall = pd.read_sql_query(grosspaymentsall,con=conn)
    print(grosspaymentsall)
    ###Create a pivot table
    grosspaymentsall_pivot = grosspaymentsall.pivot_table(index = 'year',columns = ["month","new_mode_of_payment"],aggfunc = {"amount":np.sum})
    grosspaymentsall_pivot = grosspaymentsall_pivot.droplevel([0],axis = 1).reset_index()
    grosspaymentsall_pivot = grosspaymentsall_pivot.fillna(0)

    #create a copy (grosspaymentsall_pivot)
    grosspaymentsall_pivot1 = grosspaymentsall_pivot.copy()

    #Add the column Total
    for month in grosspaymentsall_pivot1.columns.levels[0][:-1]:
        print(month)
        col_name = (month, 'Total')
        result = (grosspaymentsall_pivot1[(month, 'Cash')] + grosspaymentsall_pivot1[(month, 'Insurance')])
        grosspaymentsall_pivot1.insert(grosspaymentsall_pivot1.columns.get_loc((month, 'Insurance')) + 1, col_name, result)
        print(grosspaymentsall_pivot1)
    month = ['Branch name',"year","Jan","Feb","Mar","Apr","May","June","July","Aug","Sep","Oct","Nov","Dec"]
    grosspaymentsall_pivot1 = grosspaymentsall_pivot1.reindex(level=0,columns = month)

    # Get Totals for Cash and Insurance
    cash_cols = [col for col in grosspaymentsall_pivot1.columns if col[1] == 'Cash']
    insurance_cols = [col for col in grosspaymentsall_pivot1.columns if col[1] == 'Insurance']
    total_cols = [col for col in grosspaymentsall_pivot1.columns if col[1] == 'Total']
    grosspaymentsall_pivot1['Insurance Total'] = grosspaymentsall_pivot1[insurance_cols].sum(axis=1)
    grosspaymentsall_pivot1['Cash Total'] = grosspaymentsall_pivot1[cash_cols].sum(axis=1)
    grosspaymentsall_pivot1['Yearly Total'] = grosspaymentsall_pivot1[total_cols].sum(axis=1)

    # get the current year and month
    current_year = datetime.datetime.now().year
    lastyear = current_year-1
    current_month = datetime.datetime.now().month-1

    # create a list of month names for the current year and months up to and including the current month
    month_names = [datetime.date(lastyear, month_num, 1).strftime('%b') for month_num in range(1, current_month)]
    month_names = [month.replace('Jun', 'June').replace('Jul','July') for month in month_names]
    print(month_names)

    ##add a column to your list
    extra_column = 'year'
    month_names.append(extra_column)

    ##Sum all the months we have covered so far
    grosspaymentsall_pivotnew = grosspaymentsall_pivot[month_names]
    print('grosspaymentsall_pivotnew')
    print(grosspaymentsall_pivotnew)
    grosspaymentsall_pivotnew['year'] = grosspaymentsall_pivotnew['year'].astype('object').astype('object')
    grosspaymentsall_pivotnew["Year to Date"] = grosspaymentsall_pivotnew.iloc[:, grosspaymentsall_pivotnew.columns != ('year',          '')].sum(axis=1)

    ##merge with the original table
    grosspaymentsall_pivot = pd.merge(grosspaymentsall_pivot1,grosspaymentsall_pivotnew[['year','Year to Date']],on ='year',how = 'left')
    
    ##Calculate the Year on Year Growth
    yoy_growth = grosspaymentsall_pivot.pct_change(periods=1) * 100
    grosspaymentsall_pivot = grosspaymentsall_pivot.assign(YoY_Growth=yoy_growth['Year to Date'])
    grosspaymentsall_pivot = grosspaymentsall_pivot.rename(columns={"YoY_Growth":"YTD Growth"}).fillna(0)
    grosspaymentsall_pivot['YTD Growth'] = grosspaymentsall_pivot['YTD Growth'].replace(np.inf,0)
    grosspaymentsall_pivot["year"] = grosspaymentsall_pivot["year"].astype('object')
    grosspaymentsall_pivot = grosspaymentsall_pivot.rename(columns = {"year":"Year"})
    
    print(grosspayments_pivot.columns)

    ## Adjust the columns and set index
    grosspayments_pivot[(            'year',          '')] = grosspayments_pivot[(            'year',          '')].astype(str)
    grosspayments_pivot = grosspayments_pivot.set_index([(     'Branch name',          '')])
    print('successful')
    #------------------------------------
    grosspaymentsall_pivot[(           'Year',          '')] = grosspaymentsall_pivot[(           'Year',          '')].astype(str)
    grosspaymentsall_pivot.set_index((           'Year',          ''),inplace = True)
        #-------------------------------------------------------------------------------------------------------------
    #WRITE TO EXCEL
    with pd.ExcelWriter(r"/home/opticabi/Documents/optica_reports/Gross Sales.xlsx", engine='xlsxwriter') as writer:
        # Write the dataframe to the Excel file
        grosspayments_pivot.to_excel(writer, sheet_name='Gross Comp from 2020 to 2023', index=True)
        grosspaymentsall_pivot.to_excel(writer,sheet_name="2020 2023 Gross Sales", index=True)

        # Get the workbook and worksheet objects
        workbook = writer.book        

        #---------------------------------------------------------------------------------------------------------------
        worksheet = writer.sheets['Gross Comp from 2020 to 2023']
        # Add borders to the cells
        border_format = workbook.add_format({'border': 1})
        worksheet.conditional_format('A1:BW247', {'type': 'no_errors', 'format': border_format})
        
        # Autofit the column widths
        worksheet.set_column(0, grosspayments_pivot.shape[1] - 1, None, None, {'autofit': True})    
        
        # Set the number format for the columns
        num_format = workbook.add_format({'num_format': '#,##0'})
        num_format.set_align('right')
        worksheet.set_column('A:BW', None, num_format)

        #-------------------------------------------------------------------------------------------------------------------
        worksheet1 = writer.sheets['2020 2023 Gross Sales']
        # Add borders to the cells
        border_format = workbook.add_format({'border': 1})
        worksheet1.conditional_format('A1:BW247', {'type': 'no_errors', 'format': border_format})
        
        # Autofit the column widths
        worksheet1.set_column(0, grosspayments_pivot.shape[1] - 1, None, None, {'autofit': True})    
        
        # Set the number format for the columns
        num_format = workbook.add_format({'num_format': '#,##0'})
        num_format.set_align('right')
        worksheet1.set_column('A:BW', None, num_format)


def summary_net_payments():
    netpay = """
    SELECT branch_code as "Branch", warehouse_name as "Branch name",month_year, "year", case when month like '%March%' then replace(month,'March','Mar')
        when month like '%Sept%' then replace(month,'Sept','Sep') else month end as month, new_mode_of_payment, amount,amount * 100/116 as "net amount"
    FROM mabawa_dw.gross_payments
    where "year" >= 2020;
    """
    netpay = pd.read_sql_query(netpay,con = conn)
    print(netpay)
    ###Create a pivot table
    netpay_pivot = netpay.pivot_table(index = ['Branch name','year'],columns = ["month","new_mode_of_payment"],aggfunc = {"net amount":np.sum})
    netpay_pivot = netpay_pivot.droplevel([0],axis = 1).reset_index()
    netpay_pivot = netpay_pivot.fillna(0)
    (netpay_pivot.iloc[:, netpay_pivot.columns != ('Branch name',          '')]) = netpay_pivot.iloc[:, netpay_pivot.columns != ('Branch name',          '')].astype("int64")

    #Add the column Total
    for month in netpay_pivot.columns.levels[0][:-2]:
        col_name = (month, 'Total')
        result = (netpay_pivot[(month, 'Cash')] + netpay_pivot[(month, 'Insurance')])
        netpay_pivot.insert(netpay_pivot.columns.get_loc((month, 'Insurance')) + 1, col_name, result)
    
    month = ['Branch name',"year","Jan","Feb","Mar","Apr","May","June","July","Aug","Sep","Oct","Nov","Dec"]
    netpay_pivot = netpay_pivot.reindex(level=0,columns = month) 
    cash_cols = [col for col in netpay_pivot.columns if col[1] == 'Cash']
    insurance_cols = [col for col in netpay_pivot.columns if col[1] == 'Insurance']
    total_cols = [col for col in netpay_pivot.columns if col[1] == 'Total']
    netpay_pivot['Cumulative Insurance Total'] = netpay_pivot[insurance_cols].sum(axis=1)
    netpay_pivot['Cumulative Cash Total'] = netpay_pivot[cash_cols].sum(axis=1)
    netpay_pivot['Yearly Total'] = netpay_pivot[total_cols].sum(axis=1)
    print(netpay_pivot)
    netpay_pivot_new = netpay_pivot.copy()
    netpay_pivot_new['Branch name'] = netpay_pivot_new['Branch name'].where(~netpay_pivot_new['Branch name'].duplicated(), '')

    #-----------------------------------------------------------------------------------------------------------------------------
    #PUTTING COMMA SEPERATION
    netpay_pivot_new[(            'year',          '')] = netpay_pivot_new[(            'year',          '')].astype(str)
    netpay_pivot_new = netpay_pivot_new.set_index([(          'Branch name',          '')])
    
    with pd.ExcelWriter(r"/home/opticabi/Documents/optica_reports/Net Sales.xlsx", engine='xlsxwriter') as writer:
        netpay_pivot_new.to_excel(writer, sheet_name='Net Comp from 2020 - 2023', index=True)
        for group, dataframe in netpay_pivot.groupby((  'year',          '')):            
            # save the dataframe for each group to a csv
            dataframe = dataframe.drop([(  'year',          '')],axis=1)
            dataframe = dataframe.sort_values(by = (  'Branch name',          ''),ascending = True)   
            dataframe = dataframe.set_index((  'Branch name',          ''))       
            name = f'{group}'
            dataframe.to_excel(writer,sheet_name=name) 
        
        # Get the workbook and worksheet objects
        workbook = writer.book
        #___________________________________________________________________________________________________________
        worksheet = writer.sheets['Net Comp from 2020 - 2023']

        # Add borders to the cells
        border_format = workbook.add_format({'border': 1})
        worksheet.conditional_format('A1:BW247', {'type': 'no_errors', 'format': border_format})
        
        # Autofit the column widths
        worksheet.set_column(0, netpay_pivot_new.shape[1] - 1, None, None, {'autofit': True})    
        
        # Set the number format for the columns
        num_format = workbook.add_format({'num_format': '#,##0'})
        num_format.set_align('right')
        worksheet.set_column('A:BW', None, num_format) 

        #________________________________________________________________________________________________________________
        worksheets1 = writer.sheets

        # Apply formatting to each worksheet
        for worksheet_name, worksheet in worksheets1.items():
            # Add borders to the cells
            border_format = workbook.add_format({'border': 1})
            worksheet.conditional_format('A1:BW247', {'type': 'no_errors', 'format': border_format})

            # Autofit the column widths
            worksheet.set_column(0, dataframe.shape[1] - 1, None, None, {'autofit': True})

            # Set the number format for the columns
            num_format = workbook.add_format({'num_format': '#,##0'})
            num_format.set_align('right')
            worksheet.set_column('A:BW', None, num_format)
   
def shops_ytd_net_sales():
    netpay = """
    SELECT branch_code,warehouse_name as "Branch name", month_year, "year", case when month like '%March%' then replace(month,'March','Mar')
        when month like '%Sept%' then replace(month,'Sept','Sep') else month end as month, new_mode_of_payment, amount,amount * 100/116 as "net amount"
    FROM mabawa_dw.gross_payments
    where "year" >= 2020;
    """
    netpay = pd.read_sql_query(netpay,con = conn)

    current_year = datetime.datetime.now().year
    lastyear = current_year-1
    current_month = datetime.datetime.now().month-1

    # create a list of month names for the current year and months up to and including the current month
    month_names = [datetime.date(lastyear, month_num, 1).strftime('%b') for month_num in range(1, current_month+1)]
    print(month_names)

    ##GET SHOPS YTD CASH COMPARISON FROM THE PREVIOUS YEAR
    year = ((datetime.datetime.now().year-1),(datetime.datetime.now().year))
    net_ytd= netpay[(netpay['month'].isin(month_names)) & (netpay['year'].isin(year))]
    net_ytdcash = net_ytd[net_ytd['new_mode_of_payment'] == 'Cash']
    ytd_netcash = net_ytdcash.pivot_table(index = ['Branch name'],columns = ['month','year'],aggfunc = {'net amount':np.sum})
    ytd_netcash = ytd_netcash.droplevel([0],axis = 1).reset_index().fillna(0)
    currentyear = datetime.datetime.now().year
    previousyear = datetime.datetime.now().year-1
    for month in ytd_netcash.columns.levels[0][:-1]:
        col_name = (month, 'GAIN(DROP)')
        col_name1 = (month, '% GAIN(DROP)')
        result = (ytd_netcash[(month,currentyear )] - ytd_netcash[(month, previousyear)])
        result1 = (((ytd_netcash[(month,currentyear )] - ytd_netcash[(month, previousyear)])/ytd_netcash[(month, previousyear)])*100).round(0)
        ytd_netcash.insert(ytd_netcash.columns.get_loc((month, currentyear)) + 1, col_name, result)
        ytd_netcash.insert(ytd_netcash.columns.get_loc((month, currentyear)) + 1, col_name1, result1)
    ytd_netcash = ytd_netcash.replace(np.inf,0)
    ytd_netcash = ytd_netcash.replace(np.nan,0)
   

    ##Rearrange the columns
    month = ['Branch name',"Jan","Feb","Mar","Apr","May","June","July","Aug","Sep","Oct","Nov","Dec"]
    ytd_netcash = ytd_netcash.reindex(level=0,columns = month) 

    ##GET SHOPS YTD INSURANCE COMPARISON FROM THE PREVIOUS YEAR
    net_ytdinsurance = net_ytd[net_ytd['new_mode_of_payment'] == 'Insurance']
    ytd_netinsurance = net_ytdinsurance.pivot_table(index = ['Branch name'],columns = ['month','year'],aggfunc = {'net amount':np.sum})
    ytd_netinsurance = ytd_netinsurance.droplevel([0],axis = 1).reset_index().fillna(0)
    currentyear = datetime.datetime.now().year
    previousyear = datetime.datetime.now().year-1

    for month in ytd_netinsurance.columns.levels[0][:-1]:
        col_name = (month, 'GAIN(DROP)')
        col_name1 = (month, '% GAIN(DROP)')
        result = (ytd_netinsurance[(month,currentyear )] - ytd_netinsurance[(month, previousyear)])
        result1 = (((ytd_netinsurance[(month,currentyear )] - ytd_netinsurance[(month, previousyear)])/ytd_netinsurance[(month, previousyear)])*100).round(0)
        ytd_netinsurance.insert(ytd_netinsurance.columns.get_loc((month, currentyear)) + 1, col_name, result)
        ytd_netinsurance.insert(ytd_netinsurance.columns.get_loc((month, currentyear)) + 1, col_name1, result1)
    ytd_netinsurance = ytd_netinsurance.replace(np.inf,0)
    ytd_netinsurance = ytd_netinsurance.replace(np.nan,0)    
    

    ##Rearrange the columns
    month = ['Branch name',"Jan","Feb","Mar","Apr","May","June","July","Aug","Sep","Oct","Nov","Dec"]
    ytd_netinsurance = ytd_netinsurance.reindex(level=0,columns = month) 
    # #--------------------------------------------------------------------------------------------------------------------------------------

    #PUTTING COMMA SEPERATION
    ytd_netinsurance = ytd_netinsurance.set_index([(          'Branch name',          '')])
    ytd_netcash = ytd_netcash.set_index([(          'Branch name',          '')])

    #-------------------------------------------------------------------------------------------------------------
    #WRITE TO EXCEL
    with pd.ExcelWriter(r"/home/opticabi/Documents/optica_reports/YTD Branch Cash and Ins Net Sales.xlsx", engine='xlsxwriter') as writer:
        # Write the dataframe to the Excel file
        ytd_netcash.to_excel(writer, sheet_name='Monthly Cash Gain (Loss)', index=True)
        ytd_netinsurance.to_excel(writer,sheet_name="Monthly Insurance Gain (Loss)", index=True)

        # Get the workbook and worksheet objects
        workbook = writer.book        

        #---------------------------------------------------------------------------------------------------------------
        worksheet = writer.sheets['Monthly Cash Gain (Loss)']
        # Add borders to the cells
        border_format = workbook.add_format({'border': 1})
        worksheet.conditional_format('A1:BW247', {'type': 'no_errors', 'format': border_format})
        
        # Autofit the column widths
        worksheet.set_column(0, ytd_netcash.shape[1] - 1, None, None, {'autofit': True})    
        
        # Set the number format for the columns
        num_format = workbook.add_format({'num_format': '#,##0'})
        num_format.set_align('right')
        worksheet.set_column('A:BW', None, num_format)

        #-------------------------------------------------------------------------------------------------------------------
        worksheet1 = writer.sheets['Monthly Insurance Gain (Loss)']
        # Add borders to the cells
        border_format = workbook.add_format({'border': 1})
        worksheet1.conditional_format('A1:BW247', {'type': 'no_errors', 'format': border_format})
        
        # Autofit the column widths
        worksheet1.set_column(0, ytd_netinsurance.shape[1] - 1, None, None, {'autofit': True})    
        
        # Set the number format for the columns
        num_format = workbook.add_format({'num_format': '#,##0'})
        num_format.set_align('right')
        worksheet1.set_column('A:BW', None, num_format)             

# create_gross_payments()
# summary_gross_payments()
# summary_net_payments()
# shops_ytd_net_sales()   