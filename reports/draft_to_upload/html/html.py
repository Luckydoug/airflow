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
                <h4>3) Insurance Draft to Upload %Efficiency (orders-on-time / total_orders * 100)</h4>
                <table>{draft_html}</table>
            </div>

            <div class = "detractors">
                <h4>4) Detractors </h4>
                <table>{detractors_html}</table>
            </div>

            <div class = "sops">
                <h4>5) SOP Non Compliance</h4>
                <table>{sops_html}</table>
            </div>
            <br><br>
            <b>{plano_html}</b>
            <br><br>
            <b><{no_view_html}</b>
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
        <script src="https://cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.7/MathJax.js?config=TeX-MML-AM_CHTML" async></script>
        <style>
            table {{border-collapse: collapse;font-family:Trebuchet MS; font-size:9;}}
            th {{text-align: left;font-family:Trebuchet MS; font-size:9; padding: 4px;}}
            body, p, h3, div, span, var {{font-family:Trebuchet MS; font-size:13}}
            td {{text-align: left;font-family:Trebuchet MS; font-size:9; padding: 8px;}}
            h4 {{font-size: 12px; font-family: Trebuchet MS;}}

            .equation {{
                padding: 5px;
                background-color: white;
                display: inline-block;
                color: gray;
                font-weight: bold;
            }}

            .fraction {{
                display: inline-block;
                vertical-align: middle;
                margin: 0 0.2em 0.4ex;
                text-align: center;
                position: relative;
            }}

            .numerator, .denominator {{
                display: block;
                padding: 0;
            }}

            .denominator {{
                border-top: solid gray 1px;
            }}

            .multiply {{
                display: inline-block;
                vertical-align: middle;
                margin-left: 0.2em;
                font-weight: bold;
            }}

        </style>
    </head>
    <body>
    <p><b>Hi {branch_name},</b></p> </br>
    <p>I Hope this email finds you well.</p> </br>
    This report shows the Draft to Upload efficiency for the entire branch and each salesperson. <br>
    Use the attached to see if any orders have delayed and share the reason for the delay in the reply to this email.</p> <br>
    <p>The formula to calculate efficiency is:</p> <br>
    <div class="equation">
        % Efficiency =
        <div class="fraction">
            <div class="numerator">
                <var>Orders Uploaded on Time</var>
            </div>
            <div class="denominator">
                <var>Total Orders</var>
            </div>
        </div>
        <span class="multiply">x 100</span>
    </div>
    <br> <br>
    <b>1) Branch Efficiency</b>
    <table style = "width: 70%;">{branch_report_html}</table> 
    <br>
    <b>2) Sales Persons Efficiency</b>
    <table style = "empty-cells: hide !important;">{sales_person_report_html}</table>
    <br>
    <div>{msg}</div>
    <table>{et_order_html}</table>
    <br>
    <p style = "color: {color};">{message}</p>
    <br>
    <b><i>Best Regards</i></b><br>
    <b><i>Douglas</i></b>
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
        <p>Kindly comment on the below.</p>

        <div class="planos">
            <h3>1. Insurance Errors</h3>
            <ol>
                <table>{rejections}</table>
            </ol>
        </div>

        <div class="rejections">
            <h3>2. Insurance Customers Eye Tests Non Submission</h3>
             <ol>
                <table>{planos}</table>
            </ol>
        </div>
       <br><br>
       <b><i>Best Regards <br> Optica Data Team</i></b>
    </div>
</body>
</html>
"""


