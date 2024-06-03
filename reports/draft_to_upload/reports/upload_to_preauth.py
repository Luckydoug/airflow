import pandas as pd

def uploadToSentPreauth(data, path, selection) -> None:
    if selection != "Daily":
        return 
    
    data["Upload Time"] = pd.to_datetime(data["Upload Time"], format="%Y-%m-%d %H:%M")
    data["Sent-Preuath Time"] = pd.to_datetime(data["Sent-Preuath Time"], format="%Y-%m-%d %H:%M")

    data["Upload Time"] = data["Upload Time"].dt.strftime("%Y-%m-%d %H:%M")
    data["Sent-Preuath Time"] = data["Sent-Preuath Time"].dt.strftime("%Y-%m-%d %H:%M")

    data["Time Taken (Target = 5)"] = data["Time Taken (Target = 5)"].astype(int).astype(str)

    with pd.ExcelWriter(f"{path}draft_upload/upload_sent_preauth.xlsx") as writer:
        data.to_excel(writer, sheet_name = "Data", index = False)



def sap_update_efficieny(selection, data, path) -> None:
    if selection != "Daily":
        return
    
    with pd.ExcelWriter(f"{path}draft_upload/approval_update.xlsx") as writer:
        data.to_excel(writer, sheet_name = "Data", index = False)


