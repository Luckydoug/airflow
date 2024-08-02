branch_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Document</title>
        <style>


            .spacing {{
                margin-bottom: 20px;
                margin-top: 10px;
            }}

            table {{border-collapse: collapse; font-family: Bahnschrift, sans-serif; font-size: 10px;}}
            th {{text-align: left; font-family: Bahnschrift, sans-serif; padding: 2px;}}
            body, p, h3, div, span, var {{font-family: Bahnschrift, sans-serif;}}
            td {{text-align: left; font-family: Bahnschrift, sans-serif; font-size:11px; padding: 6px;}}
            h4 {{font-size: 14px; font-family: Bahnschrift, sans-serif;}}
        </style>

    </head>
    <body>
        <div>
            <p>Hi {rm_name},</p>
            <p>Please see {branch_name} performance for the metrics listed in the sections below</p>

            <ol>

                 <li>
                    <b>Eye Test Conversion Trends Last 12 Weeks (Latest week on the right)</b>
                    <ol>
                    <li>
                        <p>Overall Eye Test Conversion</p>
                        <div class="spacing">
                            <img src="cid:conversion_trend_image" align="middle" width="480px" height="">
                        </div>
                    </li>


                    <li>
                        <p>Low RX Eye Test Conversion</p>
                        <div class="spacing">
                            <img src="cid:lowrx_conversion_trend_image" align="middle" width="480px" height="">
                        </div>
                    </li>

                    </ol>

                </li>

                <li>
                    <b>Branch Performance</b>
                    <div class="spacing">
                        <table>{branch_performance_html}</table>
                    </div>
                </li>
                <li>
                    <b>Optom(s) Performance</b>
                    <div class="spacing">
                        <table>{optom_performance_html}</table>
                    </div>
                </li>
                <li>
                    <b>Sales Person(s) Performance</b>
                    <div class="spacing">
                        <table>{salesperson_performance_html}</table>
                    </div>
                </li>
                <li>
                    <b>Insurance Related Performance</b>
                    <div class="spacing">
                        <table>{insurance_performance_html}</table>
                    </div>
                </li>
            </ol>
        </div>  
    </body>
    </html>

"""
