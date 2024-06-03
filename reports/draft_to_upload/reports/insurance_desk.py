import pandas as pd

def format_datetime_columns(dataframe, columns):
    for column in columns:
        dataframe[column] = pd.to_datetime(dataframe[column], format="%Y-%m-%d %H:%M")
        dataframe[column] = dataframe[column].dt.strftime("%Y-%m-%d %H:%M")

def save_to_excel(dataframe, file_path):
    with pd.ExcelWriter(file_path) as writer:
        dataframe.to_excel(writer, sheet_name="Data", index=False)

def efficiencyBeforeFeedback(path, insurance_desk, no_insurance_desk):
    format_datetime_columns(insurance_desk, ["Start Time", "End Time"])
    save_to_excel(insurance_desk, f"{path}draft_upload/insurance_desk.xlsx")

    format_datetime_columns(no_insurance_desk, ["Draft Order Created", "Upload Attachment", "Sent Pre-Auth"])
    save_to_excel(no_insurance_desk, f"{path}draft_upload/no_insurance_desk.xlsx")

def efficiencyAfterFeedback(path, insurance_desk, no_insurance_desk):
    format_datetime_columns(insurance_desk, ["Approval Feedback Sent to Branch", "Branch Received Approval Feedback"])
    save_to_excel(insurance_desk, f"{path}draft_upload/after_desk.xlsx")

    format_datetime_columns(no_insurance_desk, ["Approval Feedback Received From Insurance Company", "Branch Received Approval Feedback"])
    save_to_excel(no_insurance_desk, f"{path}draft_upload/no_desk_after.xlsx")


