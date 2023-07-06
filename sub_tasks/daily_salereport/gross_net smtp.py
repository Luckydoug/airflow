#     ###########################################SMTP##################################################
#     # get a dictionary of column names and their respective data types
#     dtype_dict = grosspaymentsall_pivot.dtypes.to_dict()
#     # create a dictionary that maps the column names to the desired formatting string
#     format_dict = {col: "{:,d}" for col in dtype_dict.keys() if dtype_dict[col] == 'int64'}

#     # get a dictionary of column names and their respective data types
#     dtype_dict1 = grosspayments_pivot.dtypes.to_dict()
#     # create a dictionary that maps the column names to the desired formatting string
#     format_dict1 = {col: "{:,d}" for col in dtype_dict1.keys() if dtype_dict1[col] == 'int64'}



# ###Styling
#     grosspayments_pivot = grosspayments_pivot.style.hide_index().set_properties(**properties).set_table_styles(styles).format(format_dict1)
#     ###Convert the dataframe to html
#     grosspayments_pivot_html = grosspayments_pivot.to_html(doctype_html=True)
    
#     ###Styling
#     grosspaymentsall_pivot = grosspaymentsall_pivot.style.hide_index().set_properties(**properties).set_table_styles(styles).format(format_dict)                                                                                          
#     ###Convert the dataframe to html
#     grosspaymentsall_pivot_html = grosspaymentsall_pivot.to_html(doctype_html=True) 


     

#     #HTML
#     ##Create the SMTP for the table above    
#     html = """
#     <!DOCTYPE html>
#     <html lang="en">
#         <head>
#             <meta charset="UTF-8">
#             <meta http-equiv="X-UA-Compatible" content="IE=edge">
#             <meta name="viewport" content="width=device-width, initial-scale=1.0">
#             <title>MTD & Daily Net Sales</title>

#             <style>
#                 table {{border-collapse: collapse;font-family:Comic Sans MS; font-size:9;}}
#                 th {{text-align: left;font-family:Comic Sans MS; font-size:9; padding: 6px;}}
#                 body, p, h3, div, span, var {{font-family:Comic Sans MS; font-size:11}}
#                 td {{text-align: left;font-family:Comic Sans MS; font-size:9; padding: 8px;}}
#                 h4 {{font-size: 12px; font-family: Comic Sans MS, sans-serif;}}
#                 ul {{list-style-type: none;}}
            

#                 .salutation {{
#                     width: 20%;
#                     margin: 0 auto;
#                     text-align: left;
#                 }}
                
#             </style>
#         </head>

#         <body>
#             <div>
#                 <div class="inner-content">
#                     <p><b>Dear All,</b></p>
#                     <p>
#                         This report shows the gross sales received since 2019 to date. We also have a month - month breakdown of the current year as shown below
#                     </p>
#                     <div>                       
#                         <ul>
#                          <h4>1. Annual Gross Sales </h4>
#                             <li>
#                                 <table>{grosspaymentsall_pivot_html}</table>
#                             </li>
#                         <ul>     
                        
#                         <ul>
                                                                 
#                         </ul>
#                     </div>                   
#                 </div><br />
#                 <hr style = "margin: 0 auto; color: #F8F8F8;"/>
#             </div>
#             <br>
#             <div class = "salutation">
#                 <p><b>Kind Regards, <br> Data Team<b></p>
#             </div>
#         </body>
#     </html>
#         """.format(
#             grosspaymentsall_pivot_html=grosspaymentsall_pivot_html,grosspayments_pivot_html=grosspayments_pivot_html
#         )
    
#     to_date = get_todate()
#     sender_email = 'wairimu@optica.africa'
#     # receiver_email = ['yuri@optica.africa','kush@optica.africa','wazeem@optica.africa','lahiru@optica.africa']
#     receiver_email = ['wairimu@optica.africa']
#     email_message = MIMEMultipart()
#     email_message["From"] = sender_email
#     email_message["To"] = r','.join(receiver_email)
#     email_message["Subject"] = f"Gross Sales per Year by Branch"
#     email_message.attach(MIMEText(html, "html"))

#     # Open the Excel file and attach it to the email
#     with open('/home/opticabi/Documents/optica_reports/Gross Sales.xlsx', 'rb') as attachment:
#         excel_file = MIMEApplication(attachment.read(), _subtype='xlsx')
#         excel_file.add_header('Content-Disposition', 'attachment', filename='Gross Sales.xlsx')
#         email_message.attach(excel_file)

#     smtp_server = smtplib.SMTP("smtp.gmail.com", 587)
#     smtp_server.starttls()
#     smtp_server.login(sender_email, "maureen##3636")
#     text = email_message.as_string()
#     smtp_server.sendmail(sender_email, receiver_email, text)
#     smtp_server.quit()
# if __name__ == '__main__': 
#     summary_gross_payments()  
    

 



