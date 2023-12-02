import pandas as pd
import numpy as np

def eyetest_order_time(data, path, selection):
    if selection == "Weekly" or selection == "Monthly":
        return

    with pd.ExcelWriter(f"{path}draft_upload/et_to_order.xlsx") as writer:
        late_orders = data[data["time_taken"] > 60]

        late_orders["et_completed_time"] = pd.to_datetime(late_orders["et_completed_time"], format="%Y-%m-%d %H:%M").dt.strftime("%Y-%m-%d %H:%M")
        late_orders["order_date"] = pd.to_datetime(late_orders["order_date"], format="%Y-%m-%d %H:%M").dt.strftime("%Y-%m-%d %H:%M")
        late_orders.to_excel(writer, sheet_name = "Data", index = False)
