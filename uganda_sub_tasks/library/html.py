draft_ug_html = """
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
            td {{text-align: left;font-family:Garamond; font-size:10px; padding: 2px;}}
            h4 {{font-size: 14px; font-family: Garamond;}}
        </style>
    </head>
    <body>
        <div class = "container">
            <div class = "branch_late">
                <h4>1) Branches that opened after their opening time</h4>
               <table>lateness_html</table>
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
                <h4>5) SOP Compliance</h4>
                <table>{sops_html}</table>
            </div>

            <b><i>{plano_html}</i></b>
        </div>
    </body>
    </html>
"""