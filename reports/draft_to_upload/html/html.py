drafts_html = """

    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Document</title>
        <style>
            table {{border-collapse: collapse;font-family:Garamond; font-size: 10px;}}
            th {{text-align: left;font-family:Garamond; padding: 2px;}}
            body, p, h3, div, span, var {{font-family:Garamond; font-size:11}}
            td {{text-align: left;font-family:Garamond; font-size:11px; padding: 4px;}}
            h4 {{font-size: 14px; font-family: Garamond;}}
        </style>
    </head>
    <body>
        <div class = "container">
            <div class = "branch_late">
                <h4>1) Branches that opened after their opening time</h4>
               <table>{opening_html}</table>
            </div>

            <div class = "rejections">
                <h4>2) Insurance Rejections</h4>
                <table>{rejections_html}</table>
            </div>

            <div class = "draft">
                <h4>3) Insurance (Draft to Upload) time taken</h4>
                <table>{draft_html}</table>
            </div>

            <div class = "detractors">
                <h4>4) Detractors </h4>
                <table>{detractors_html}<table>
            </div>

            <div class = "sops">
                <h4>5) SOP Non Compliance</h4>
                <table>{sops_html}</table>
            </div>
            <br><br>
            <b><i>{plano_html}</i></b>
        </div>
    </body>
    </html>

"""

branches_html = """

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{branch} Report</title>

    <style>
        table {{border-collapse: collapse;font-family:Garamond; font-size: 10px;}}
        th {{text-align: left;font-family:Garamond; padding: 2px;}}
        body, p, h3, div, span, var {{font-family:Garamond; font-size:11}}
        td {{text-align: left;font-family:Garamond; font-size:11px; padding: 4px;}}
        h4 {{font-size: 14px; font-family: Garamond;}}
    </style>

</head>
<body>
    <div>
        <b><p>Hi {branch_manager},</p></b>
        <p>Please see the below reports.</p>

        <div class="planos">
            <h3>1. Insurance Errors</h3>
            <p>
                Please help us understand why the below mentioned insurance error 
                has occurred and what action you have taken to curtail this going forward.
            </p>
            <table>{rejections}</table>
        </div>

        <div class="rejections">
            <h3>2. Plano Preauth non submission</h3>
            <p>
                Also, Please help us understand, 
                why the below Plano Rx was not submitted to insurance for preauth 
                and what action you have taken to curtail this going forward.
            </p>

            <table>{planos}</table>
        </div>
       <br><br>
       <b><i>Best Regards <br> Optica Data Team</i></b>
    </div>
</body>
</html>

"""

html_rejections = """

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{branch} Report</title>

    <style>
        table {{border-collapse: collapse;font-family:Garamond; font-size: 10px;}}
        th {{text-align: left;font-family:Garamond; padding: 2px;}}
        body, p, h3, div, span, var {{font-family:Garamond; font-size:11}}
        td {{text-align: left;font-family:Garamond; font-size:11px; padding: 4px;}}
        h4 {{font-size: 14px; font-family: Garamond;}}
    </style>

</head>
<body>
    <div>
        <b><p>Hi {branch_manager},</p></b>
        <div class="rejections">
            <h3>1. Insurance Errors</h3>
            <p>
                Please help us understand why the below mentioned insurance error 
                has occurred and what action you have taken to curtail this going forward.
            </p>
            <table>{rejections}</table>
        </div>
       <br><br>
       <b><i>Best Regards <br> Optica Data Team</i></b>
    </div>
</body>
</html>

"""

html_planos = """

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{branch} Report</title>

    <style>
        table {{border-collapse: collapse;font-family:Garamond; font-size: 10px;}}
        th {{text-align: left;font-family:Garamond; padding: 2px;}}
        body, p, h3, div, span, var {{font-family:Garamond; font-size:11}}
        td {{text-align: left;font-family:Garamond; font-size:11px; padding: 4px;}}
        h4 {{font-size: 14px; font-family: Garamond;}}
    </style>

</head>
<body>
    <div>
        <b><p>Hi {branch_manager},</p></b>

        <div class="planos">
            <h3>1. Plano Preauth non submission</h3>
            <p>
                Please help us understand, 
                why the below Plano Rx was not submitted to insurance for preauth 
                and what action you have taken to curtail this going forward.
            </p>

            <table>{planos}</table>
        </div>
       <br><br>
       <b><i>Best Regards <br> Optica Data Team</i></b>
    </div>
</body>
</html>

"""


branch_efficiency_html = """

    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Document</title>
        <style>
            table {{border-collapse: collapse;font-family:Trebuchet MS; font-size:9;}}
            th {{text-align: left;font-family:Trebuchet MS; font-size:9; padding: 4px;}}
            body, p, h3, div, span, var {{font-family:Trebuchet MS; font-size:13}}
            td {{text-align: left;font-family:Trebuchet MS; font-size:9; padding: 8px;}}
            h4 {{font-size: 12px; font-family: Trebuchet MS;}}
        </style>
    </head>
    <body>
    <p><b>Hi {branch_name},</b></p> </br>
    <p>I Hope this email finds you well.</p> </br>
    This report shows the Draft to Upload efficiency for the entire branch and each salesperson. <br>
    Kindly use the details in these tables to identify areas that need improvement.</p> <br>
    <br>
    <b>1) Branch Efficiency</b>
    <table style = "width: 70%;">{branch_report_html}</table> 
    <br>
    <b>2) Sales Persons Efficiency</b>
    <table style = "empty-cells: hide !important;">{sales_person_report_html}</table>
    <br>
    <p>Kindly see on the attachments a list of orders and the time taken from Draft Order Created to Upload Attachment statuses</p>
    <br>
    <b><i>Best Regards</i></b><br>
    <b><i>Douglas</i></b>
    </body>
    </html>

"""