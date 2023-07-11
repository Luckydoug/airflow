import pandas as pd
import numpy as np

def create_branch_salesperson_efficiency(data, target, path, cols_req):
    sales_persons_efficiency = pd.pivot_table(
            data,
            index=[
                "Outlet", "Order Creator"],
            values="Draft to Upload",
            aggfunc={"Draft to Upload": [
                pd.Series.count,
                "mean",
                lambda x: (x <= target).sum(), lambda x: (x > target).sum()
            ]},
            sort=False
    ).reset_index()

    sales_persons_efficiency.columns = [
        "Outlet", "Order Creator", f"Orders <= {target} mins (Draft to Upload)", "Late Orders", "Total Orders", "Average Upload Time"]
    sales_persons_efficiency[f"% Efficiency (Target: {target} mins)"] = round(
        sales_persons_efficiency[f"Orders <= {target} mins (Draft to Upload)"] /
        sales_persons_efficiency["Total Orders"] * 100, 0
    ).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)
    sales_persons_efficiency["Average Upload Time"] = sales_persons_efficiency["Average Upload Time"].round(
        0).astype(int)

    sales_persons_efficiency = sales_persons_efficiency[
        [
            "Outlet",
            "Order Creator",
            "Total Orders",
            "Average Upload Time",
            "Orders <= 8 mins (Draft to Upload)",
            "Late Orders",
            "% Efficiency (Target: 8 mins)"
        ]
    ]

    branch_efficiency = pd.pivot_table(
        data,
        index=["Outlet"],
        values=["Draft to Upload"],
        aggfunc={"Draft to Upload": [
            pd.Series.count,
            "mean",
            lambda x: (
                x <= target).sum(),
            lambda x: (x > target).sum()]}
    ).reset_index()


    branch_efficiency.columns = [
        "Outlet", f"Orders <= {target} mins (Draft to Upload)", "Late Orders", "Total Orders", "Average Upload Time"]
    branch_efficiency[f"% Efficiency (Target: {target} mins)"] = round(
        branch_efficiency[f"Orders <= {target} mins (Draft to Upload)"] /
        branch_efficiency["Total Orders"] * 100, 0
    ).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)
    branch_efficiency["Average Upload Time"] = branch_efficiency["Average Upload Time"].round(
        0).astype(int)
    branch_efficiency = branch_efficiency[[
        "Outlet",
        "Total Orders",
        "Average Upload Time",
        "Orders <= 8 mins (Draft to Upload)",
        "Late Orders",
        "% Efficiency (Target: 8 mins)"
    ]]

    with pd.ExcelWriter(f"{path}draft_upload/draft_to_upload_sales_efficiency.xlsx") as writer:
            for group, dataframe in sales_persons_efficiency.groupby("Outlet"):
                name = f'{group}'
                dataframe.iloc[:, 1:].to_excel(
                    writer, sheet_name=name, index=False)

    with pd.ExcelWriter(f"{path}draft_upload/draft_to_upload_branch_efficiency.xlsx") as writer:
        for group, dataframe in branch_efficiency.groupby("Outlet"):
            name = f'{group}'
            dataframe.iloc[:, 1:].to_excel(
                writer, sheet_name=name, index=False)

    with pd.ExcelWriter(f"{path}draft_upload/efficiency_raw_data.xlsx") as writer:
        for group, dataframe in data.groupby("Outlet"):
            name = f'{group}'
            dataframe.sort_values(by="Order Creator")[cols_req].to_excel(
                writer, sheet_name=name, index=False)


