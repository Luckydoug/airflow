branch_queue_time_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Document</title>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.7/MathJax.js?config=TeX-MML-AM_CHTML" async></script>
        <style>
            table {{border-collapse: collapse;font-family:Times New Roman; font-size:9;}}
            th {{text-align: left;font-family:Times New Roman; font-size:9; padding: 4px;}}
            body, p, h3, div, span, var {{font-family:Times New Roman; font-size:13}}
            td {{text-align: left;font-family:Times New Roman; font-size:9; padding: 8px;}}
            h4 {{font-size: 12px; font-family:Times New Roman;}}
        </style>
    </head>
    <body>
    <p><b>Hi {branch_name},</b></p> </br>
    <p>I Hope this email finds you well.</p> </br>
    This report shows average queue and eye test time for the entire branch and each optom. <br>
    Use the attached to see the optoms keeping the clients so long on queue and advise how you will imporove on it.</p> <br>
    <br> 
    <b>1) Branch Summary</b>
    <table style = "width: 70%;">{branch_report_html}</table> 
    <br>
    <b>2) Optom Summary</b>
    <table style = "empty-cells: hide !important;">{optom_report_html}</table>
    <br>
    <br>
    <b><i>Best Regards</i></b><br>
    <b><i>Douglas</i></b>
    </body>
    </html>
"""

management_html = """
  <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Document</title>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.7/MathJax.js?config=TeX-MML-AM_CHTML" async></script>
        <style>
            table {{border-collapse: collapse;font-family:Times New Roman; font-size:9; }}
            th {{text-align: left;font-family:Times New Roman; padding: 4px;}}
            body, p, h3, div, span, var {{font-family:Times New Roman; font-size:13}}
            td {{text-align: left;font-family:Times New Roman; font-size:9; padding: 8px;}}
            h4 {{font-size: 12px; font-family:Times New Roman;}}
        </style>
    </head>
    <body>
    <p>
    Dear All,
    Please view the report below for analysis on the average time clients wait before <br>
    being accepted by an optom for an eye test as well as the average time a check up takes per client.
    </p> <br>
    <b>1) Over all Summary</b>
    <table style = "width: 70%;">{country_html}</table> 
    <br>
    <b>2) Branch Summary</b>
    <table style = "empty-cells: hide !important;">{branch_html}</table>
    <br>
    <br>
    <b><i>Best Regards</i></b><br>
    <b><i>Douglas</i></b>
    </body>
    </html>
"""



queue_time_weekly = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Document</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.7/MathJax.js?config=TeX-MML-AM_CHTML" async></script>
    <style>
        table {{border-collapse: collapse;font-family:Times New Roman; font-size:9;}}
        th {{text-align: left;font-family:Times New Roman; padding: 4px;}}
        body, p, h3, div, span, var {{font-family:Times New Roman; font-size:15}}
        td {{text-align: left;font-family:Times New Roman; font-size:9; padding: 8px;}}
        h4 {{font-size: 13px; font-family:Times New Roman;}}
    </style>
</head>
<body>
<p><b>Hi {branch_name},</b></p> </br>
<p>I Hope this email finds you well.</p> </br>
Below is the overall average time clients have spent on the queue before being accepted by <br>
the optom for an eye check up as well as the average time taken for a check up to be completed..</p> 
<br> <br>
<b>1) Branch Summary</b> <br>
<table style = "width: 70%;">{branch_report_html}</table> 
<br>
<b>2) Optom Summary</b>
<p>
Below is the average time the optoms have taken to accept clients from the optom queue for a check up during the past 2 weeks.<br>
<b>NOTE:</b> For optoms with an average queue time of 0 mins, <br>
kindly check on the attachments to confirm whether they were present in the branch during that specific week.
</p>
<table style = "empty-cells: hide !important;">{optom_report_html}</table>
<br>
<br>
<b>Best Regards</b><br>
<b>Douglas Kathurima</b> <br>
<b>From Data Team</b>
</body>
</html>
"""
