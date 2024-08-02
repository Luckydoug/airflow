import pandas as pd 

def generate_html_and_subject(branch, branch_name, dataframe_dict, date, styles):
    html_start_template = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <title>Document</title>
        <style>
            table {{border-collapse: collapse; font-family: Bahnschrift, sans-serif; font-size: 10px;}}
            th {{text-align: left; font-family: Bahnschrift, sans-serif; padding: 2px;}}
            body, p, h3, div, span, var {{font-family: Bahnschrift, sans-serif;}}
            td {{text-align: left; font-family: Bahnschrift, sans-serif; font-size:11px; padding: 4px;}}
            h4 {{font-size: 14px; font-family: Bahnschrift, sans-serif;}}
        </style>
        </head>
        <body>
        <b>Hi {branch_name},</b> <br>
        <p>Kindly comment on the following.</p>
    """
   
    html_end_template = """
        <p>Kind regards,</p>
        <p>Data Team</p>
        </body>
        </html>
    """
    
    html_content = ""
    subject_parts = []
    counter = 1
    
    for df_key, df in dataframe_dict.items():
        branch_data = df[df["Outlet"] == branch]
        branch_data = branch_data.drop(columns = ["Outlet"])
        
        if not branch_data.empty:
            subject_parts.append(df_key)
            html_content += f"<b>{counter}. {df_key}</b>"
            html_content += "<br>"

            if df_key == "Viewed Eye test older than 30 days and did not convert":
                html_content += "<p>Any eye test done more than 30 days ago and viewed yesterday but no order made.</p>"

            html_content += f"<table>{branch_data.style.hide_index().set_table_styles(styles).to_html(doctype_html=True)}</table>"
            html_content += "<br>"
            counter += 1    
    
    if not subject_parts:
        return None, None
    
    # subject = branch + " Response Required - Daily Insurance Checks for " + str(date) + "."
    subject = branch + " Response Required - " + ", ".join(subject_parts[:-1]) + " and " + subject_parts[-1] + " for " + str(date)  if len(subject_parts) > 1 else branch + " Response Required - " + subject_parts[0] + " for " + str(date)
    return html_start_template + html_content + html_end_template, subject

    