import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email():
    sender_email = ''
    receiver_email = ''
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = "Airflow email notification"
    body = "This is a test email sent from Airflow"
    message.attach(MIMEText(body, "plain"))
    smtp_server = smtplib.SMTP("smtp.gmail.com", 587)
    smtp_server.starttls()
    smtp_server.login(sender_email, "")
    text = message.as_string()
    smtp_server.sendmail(sender_email, receiver_email, text)
    smtp_server.quit()

if __name__ == '__main__':
    send_email()


import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

def send_email():
    sender_email = ''
    receiver_email = ''
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = "Airflow email notification"
    body = "This is a test email sent from Airflow"
    message.attach(MIMEText(body, "plain"))
    
    # Open the Excel file and attach it to the email
    with open('/path/to/my_data.xlsx', 'rb') as attachment:
        excel_file = MIMEApplication(attachment.read(), _subtype='xlsx')
        excel_file.add_header('Content-Disposition', 'attachment', filename='my_data.xlsx')
        message.attach(excel_file)
        
    # Send the email using the SMTP server
    smtp_server = smtplib.SMTP("smtp.gmail.com", 587)
    smtp_server.starttls()
    smtp_server.login(sender_email, "")
    text = message.as_string()
    smtp_server.sendmail(sender_email, receiver_email, text)
    smtp_server.quit()

if __name__ == '__main__':
    send_email()
