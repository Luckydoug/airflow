conversion_html = """
    <!DOCTYPE html>
    <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta http-equiv="X-UA-Compatible" content="IE=edge">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Conversion Report</title>

            <style>
                table {{border-collapse: collapse;font-family:Trebuchet MS; font-size:9}}
                th {{text-align: left;font-family:Trebuchet MS; font-size:9; padding: 9px; background-color: #A7C7E7;}}
                body, p, h3, div, span, var {{font-family:Cambria; font-size:13}}
                td {{text-align: left;font-family:Trebuchet MS; font-size:9; padding: 8px;}}
                h4 {{font-size: 12px; font-family: Verdana, sans-serif;}}

                .content {{
                    margin-top: 20px !important;
                    border: none;
                    background-color: white;
                    padding: 4%;
                    width: 85%;
                    margin: 0 auto;
                    border-radius: 3px;
                }}

                .salutation {{
                    width: 20%;
                    margin: 0 auto;
                    margin-top: 20px;
                    font-size: 10px;
                }}

                .division-line {{
                    display: inline-block;
                    border-top: 1px solid black;
                    margin: 0 5px;
                    padding: 0 2px;
                    vertical-align: middle;
                }}

            </style>

        </head>
        <body style = "background-color: #F0F0F0; padding: 2% 2% 2% 2%;">
            <div class="content">
                <div>
                    <ol>
                        <li>
                            <h3>Registrations Conversion</h3>
                            <br><br>
                            <ol>
                                <li><b>{weekly_monthly} Overall Comparison</b>
                                <br> <br>
                                    <table>{reg_summary_html}</table>
                                </li>
                                    <br><br>
                                <li><b>Branch Wise {weekly_monthly} Comparison</b>
                                <br> <br>
                                    <table>{branches_reg_summary_html}</table>
                                </li>
                            </ol>
                        </li>

                         <li>
                            <h3>Eyetest Conversions</h3>
                            <ol>
                                <li><b>{weekly_monthly} Overall Comparison</b>
                                <br> <br>
                                    <table>{overall_et_html}</table>
                                </li>
                                <br><br>
                                <li><b>Branchwise {weekly_monthly} Conversion Comparison</b>
                                <br>
                                    <p><b>Note</b> The below eye tests excludes referrals to the opthalmologists.</p>
                                    <table>{branches_et_html}</table>
                                </li>
                                <br><br>
                                <li><b>High RX {weekly_monthly} Conversion Comparison</b>
                                <br><br>
                                    <table>{summary_higrx_html}</table>
                                </li>
                            </ol>
                        </li>

                        <li>
                            <h3>View RX Conversions</h3>
                            <ol>
                                <li><b>{weekly_monthly} Overall Comparison</b>
                                <br> <br>
                                    <table>{overall_views_html}</table>
                                </li>
                                <br><br>
                                <li><b>Branchwise {weekly_monthly} Conversion Comparison</b>
                                <br>
                                    <table>{branches_views_html}</table>
                                </li>
                                <br><br>
                            </ol>
                        </li>
                    </ol>

                    <br><br>
                    Also see the non-conversions on the attachments.
                </div>
            </div>

            <div class = "salutation">
                <b><i>
                    Best Regards <br>
                    Optica Data Team 
                </i></b>
            </div>
        </body>
    </html>
    """


branches_html =  """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Uganda Outlets Conversion Report</title>

        <style>
            table {{border-collapse: collapse;font-family:Trebuchet MS; font-size:9;}}
            th {{text-align: left;font-family:Trebuchet MS; font-size:9; padding: 3px;}}
            body, p, h3, div, span, var {{font-family:Trebuchet MS; font-size:13}}
            td {{text-align: left;font-family:Trebuchet MS; font-size:9; padding: 8px;}}
            h4 {{font-size: 12px; font-family: Verdana, sans-serif;}}

            .content {{
                margin-top: 20px !important;
                border: none;
                background-color: white;
                padding: 4%;
                width: 85%;
                margin: 0 auto;
                border-radius: 3px;
            }}

            .salutation {{
                width: 20%;
                margin: 0 auto;
                margin-top: 20px;
                font-size: 10px;
            }}

            .inner {{
                margin: 5px 0px 5px 0px;
                padding: 4px;
            }}

        </style>
    </head>
    <body style = "background-color: #F0F0F0; padding: 2% 2% 2% 2%;">
        <div class = "content">
            <div>
                <div>

                <h3>Conversion Report</h3>
                <b>Dear {branch_name},</b>
                <p>This report shows conversions for registrations, eye tests, and view RX. 
                Kindly review this report to identify any areas that need improvement. </p>
                </div>

                <ol>
                    <li><h3>Registrations Conversion</h3>
                        <ol>
                            <li class = "inner">Branch Conversion
                                <table>{branch_reg_html}</table>
                            </li>

                            <li class = "inner">Staff Conversion <br><br>
                                <tabel>{salespersons_reg_html}</tabel>
                            </li>
                        </ol>
                    </li>


                    <li><h3>Eye Tests Conversion</h3>
                        <ol>
                            <li class = "inner">Branch Conversion
                                <table>{branch_eyetest_html}</table>
                            </li>

                            <li class = "inner"> Conversion by Optom <br><br>
                                <table>{optom_eyetest_html}</table>
                            </li>

                            <li class = "inner">Conversion by EWC Handover<br><br>
                                <table>{salesperson_eyetest_html}</table>
                            </li>
                        </ol>

                    </li>

                    <li><h3>View RX Conversion</h3>
                        <ol>
                            <li class = "inner">Branch Conversion
                                <table>{views_branch_html}</table>
                            </li>

                            <li class = "inner">Staff Conversion <br><br>
                                <tabl>{salespersons_views_html}</tabl>
                            </li>
                        </ol>

                    </li>
                </ol>

                <p>Kindly see a list of non-conversions of the attachments.</p>

            </div>

        </div>
    </body>
    </html>
    """