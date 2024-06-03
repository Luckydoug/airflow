import pandas as pd

def createDirectInsuranceConversion(data, selection, path):
    if selection == "Daily":
        with pd.ExcelWriter(f"{path}/draft_upload/direct_insurance.xlsx") as writer:
            data.to_excel(writer, sheet_name = "Data", index = False)