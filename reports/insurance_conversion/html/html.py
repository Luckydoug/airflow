management_html = """

<!DOCTYPE html>
    <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta http-equiv="X-UA-Compatible" content="IE=edge">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Document</title>
            <style>
                table {{border-collapse: collapse;font-family:Times New Roman; font-size:7;}}
                th {{text-align: left;font-family:Times New Roman; font-size:9; padding: 6px;}}
                body, p, h3, div, span, var {{font-family:Times New Roman; font-size:10}}
                td {{text-align: left;font-family:Times New Roman; font-size:9; padding: 8px;}}
                h4 {{font-size: 12px; font-family: Verdana, sans-serif;}}
                .content {{
                    border: none;
                    background-color: white;
                    padding: 4%;
                    width: 78%;
                    margin: 0 auto;
                    border-radius: 3px;
                }}

            </style>
        </head>
        <body style = "background-color: #F0F0F0; padding: 3% 4% 4% 4%;">
            <div class = "content">
                <p><b>Hi All,</b></p>
                <p>Please see the analyis for insurance conversion per the Insurance Feedback for the period on the subject.</p>
                <ol>
                    <li>
                        <h4>Requests Sent Vs Feedbacks</h4>
                        <table>{no_feedback_html}</table>
                    </li>
                
                    <li>
                        <h4>Overall Company Insurance Conversion </h4> 
                        <p> 
                            The table provided is a summary of the insurance conversion process for different time periods.<br>
                            It shows the number of requests and conversions for each feedback type, including "Insurance Fully Approved", "Insurance Partially Approved", "Use Available Amount on SMART", and "Declined by Insurance Company". <br>
                        </p> <br>
                        <table style = "width: 90%;">{company_conversion_html}</table>
                    </li>
                    <br>
                    <li>
                        <h4>Branches Conversion</h4>
                        <b><i>Kindly note that the data in the table below shows the conversion for the period on the subject line only</i></b> <br>
                        <table style = "width: 100%; padding: 6px">{branches_conversion_html}</table>
                    </li>
                </ol>

                <p>Also see on the attachement a list of orders that didn't convert for each branch.</p>
                <br>
                <b><i>Best Regards <br> Douglas</i></b>
            </div>
        </body>
    </html>

"""

branches_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Document</title>
        <style>
            table {{border-collapse: collapse;font-family:Times New Roman; font-size:9}}
            th {{text-align: left;font-family:Times New Roman; font-size:9; padding: 4px;}}
            body, p, h3, div, span, var {{font-family:Times New Roman; font-size:13}}
            td {{text-align: left;font-family:Times New Roman; font-size:9; padding: 8px;}}
            h4 {{font-size: 12px; font-family: Verdana, sans-serif;}}
        </style>
    </head>
    <body>
        <p><b>Hi {branch_name},</b></p> </br>
        <p><i>I Hope this email finds you well.</i></p> </br>
        <p>Kindly see insurance conversion per the insurance feedback for the period on the subject.</p>
        <br>
        <table style = "width: 70%;">{overall_feedbacks_html}</table> 
        <br>
        <p>Also see the the breakdown per the sales person</p>
        <table style = "empty-cells: hide !important;">{individual_feedback_html}</table>
        <br>
        <p>Kindly see on the attachments a list of insurance orders that didn’t convert</p>
        <br>
        <b><i>Best Regards</i></b><br>
        <b><i>Douglas</i></b>
    </body>
    </html>
    """