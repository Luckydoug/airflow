import pandas as pd
import numpy as np
import pygsheets
from airflow.models import Variable
from calendar import monthrange
from sub_tasks.libraries.utils import (
    service_file,
    format_payroll_number,
    return_incentives_daterange,
    path
)

start_date, end_date = return_incentives_daterange()


def create_incentive(insurance_payments, cash_payments, targets):
    service_key = pygsheets.authorize(service_file=service_file)
    sheet = service_key.open_by_key('1jTTvbk8g--Q3FWKMLZaLquDiJJ5a03hsJEtZcUTTFr8')
    staff = pd.DataFrame(sheet.worksheet_by_title("Emails").get_all_records())
    branch_insurance_pivot = pd.pivot_table(
        insurance_payments,
        index=["Outlet", "Creator", "Order Creator"],
        aggfunc="count",
        values="DocNum"
    ).reset_index()
    branch_insurance_pivot.rename(columns={"DocNum": "Actual"}, inplace=True)
    branch_insurance_pivot["Creator"] = branch_insurance_pivot["Creator"].astype(
        str)
    branch_insurance_pivot = branch_insurance_pivot.rename(
        columns={"Creator": "Payroll Number","Actual": "MTD Insurance Sales(count)"}
    )
    final_insurance_pivot = branch_insurance_pivot[
        ["Outlet", "Payroll Number","MTD Insurance Sales(count)", "Order Creator"]
    ].rename(columns={"Order Creator": "Name"})

    branch_cash_pivot = pd.pivot_table(
        cash_payments,
        index=["Outlet", "Creator","Order Creator"],
        aggfunc="sum",
        values="Full Amount"
    ).reset_index()
    branch_cash_pivot["Creator"] = branch_cash_pivot["Creator"].astype(str)
    branch_cash_pivot.rename(columns={"Creator": "Payroll Number","Full Amount": "MTD Cash Sales", "Order Creator": "Name"},inplace=True)

    final_insurance_cash = pd.merge(
        final_insurance_pivot,
        branch_cash_pivot,
        on=["Outlet", "Payroll Number", "Name"],
        how="outer"
    ).fillna(0)
    final_insurance_cash["MTD Insurance Sales(count)"] = final_insurance_cash["MTD Insurance Sales(count)"].astype(int)

    staff["Payroll No"] = staff["Payroll No"].apply(format_payroll_number)
    staff_data = staff.drop_duplicates(subset=["Payroll No"])
    staff_data = staff_data.dropna(subset=["Payroll No"])
    staff_data["Payroll No"] = staff_data["Payroll No"].astype(str)
    staff_data = staff_data.rename(columns={"Payroll No": "Payroll Number"})

    final_report = pd.merge(
        final_insurance_cash,
        staff_data[["Payroll Number", "Email"]],
        on="Payroll Number",
        how="left"
    )
    targets = targets.drop_duplicates(subset=["Payroll Number", "Outlet"])
    sales_and_targets = pd.merge(
        final_report,
        targets,
        on=["Outlet", "Name", "Payroll Number"],
        how="outer"
    ).fillna(0)

    sales_and_targets["Insurance Target"] = round(
        sales_and_targets["Insurance Target"], 0).fillna(0).astype(int)
    sales_and_targets["Cash Target"] = round(
        sales_and_targets["Cash Target"], 0).fillna(0).astype(int)
    sales_and_targets["Insurance Target"] = sales_and_targets["Insurance Target"].fillna(0)
    sales_and_targets["Cash Target"] = sales_and_targets["Cash Target"].fillna(0)

    date = pd.to_datetime(end_date)
    month_number = date.month
    year = date.year
    days = date.day
    number_of_days = monthrange(year, month_number)[1]

    scheduled_branches = ["OHO", "YOR"]

    with pd.ExcelWriter(f"{path}branches.xlsx", engine='xlsxwriter') as writer:
        branch_sales_targets = sales_and_targets[
            (sales_and_targets["Cash Target"] != 0) &
            (sales_and_targets["Insurance Target"] != 0) &
            (sales_and_targets["Payroll Number"] != 0) &
            (sales_and_targets["Outlet"].isin(scheduled_branches))]
        for group, dataframe in branch_sales_targets.groupby('Outlet'):
            name = f'{group}'
            dataframe["Cash MTD% Target"] = (((((days * 100) / number_of_days)) / 100) * 100)

            dataframe["Insurance MTD Target"] = (
                (((days * dataframe["Insurance Target"]) / number_of_days))
            ).fillna(0).round(0).astype(int)
            dataframe["MTD Cash Sales % Achieved"] = (
                (dataframe["MTD Cash Sales"] / ((days *dataframe["Cash Target"]) / number_of_days) * 100)
            )
            dataframe[["Cash MTD% Target", "MTD Cash Sales % Achieved"]] = dataframe[
                ["Cash MTD% Target", "MTD Cash Sales % Achieved"]
            ].replace([np.inf, -np.inf], 0).fillna(0).round(0).astype(int).astype(str) + "%"
            dataframe = dataframe[
                ["Outlet", "Name", "Payroll Number", "MTD Cash Sales",
                 "Cash Target", "Insurance Target",
                 "Cash MTD% Target", "Insurance MTD Target",
                 "MTD Cash Sales % Achieved", "MTD Insurance Sales(count)"]
            ]
            dataframe["Cash Target"] = str(100) + "%"
            multindex = pd.MultiIndex.from_tuples([
                ('Full Month Target', 'Cash Target'),
                ('Full Month Target', 'Insurance Target'),
                ('MTD Target', "Cash MTD% Target"),
                ('MTD Target', 'Insurance MTD Target'),
                ('Actual Performance', 'MTD Cash Sales % Achieved'),
                ('Actual Performance', 'MTD Insurance Sales(count)')
            ])
            dataframe = dataframe.drop(["MTD Cash Sales", "Outlet"], axis=1)
            dataframe = dataframe.set_index(["Name", "Payroll Number"])
            dataframe.columns = multindex
            dataframe.to_excel(writer, sheet_name=name)

    with pd.ExcelWriter(f"{path}total_branches.xlsx", engine='xlsxwriter') as writer:
        total_branch_sales = sales_and_targets[sales_and_targets["Outlet"].isin(
            scheduled_branches)]
        for group, dataframe in total_branch_sales.groupby('Outlet'):
            name = f'{group}'
            dataframe = dataframe.sort_values(
                by="Cash Branch Target", ascending=True).reset_index()
            cash_branch_target = dataframe["Cash Branch Target"].max()
            insurance_target = dataframe["Insurance Branch Target"].max()
            insurance_achieved = dataframe["MTD Insurance Sales(count)"].sum()
            cash_achieved = dataframe["MTD Cash Sales"].sum()
            insurance_mtd = (days * insurance_target) / number_of_days
            cash_mtd = (days * cash_branch_target) / number_of_days
            dicti = {
                "Insurance MTD Target(count)": round(insurance_mtd, 0),
                "Insurance MTD Achieved(count)": insurance_achieved,
                "Cash MTD Target%": 100,
                "Cash MTD % Achieved": round((cash_achieved / cash_mtd) * 100, 0)
            }
            branch_achievement = pd.DataFrame(dicti, index=[0])
            branch_achievement.to_excel(writer, sheet_name=name, index=False)

    sales_persons_report = sales_and_targets[
        (sales_and_targets["Cash Target"] != 0) &
        (sales_and_targets["Insurance Target"] != 0) &
        (sales_and_targets["Payroll Number"] != 0) &
        (sales_and_targets["Email"] != 0) &
        (sales_and_targets["Outlet"].isin(scheduled_branches))
    ]

    desired_columns_order = [
        "Name",
        "Payroll Number",
        "Insurance Target",
        "MTD Insurance Sales(count)",
        "MTD Achieved Insurance(%)",
        "Cash Target",
        "MTD Cash Sales",
        "MTD Achieved Cash(%)",
        "Email"
    ]
    
    for branch in scheduled_branches:
        branch_sales = sales_persons_report[sales_persons_report["Outlet"] == branch]
        with pd.ExcelWriter(f"{path}{branch}.xlsx".format(branch=branch), engine='xlsxwriter') as writer:
            for group, dataframe in branch_sales.groupby('Payroll Number'):
                name = f'{group}'
                dataframe["MTD Achieved Cash(%)"] = (
                    (dataframe["MTD Cash Sales"] / ((days *dataframe["Cash Target"]) / number_of_days) * 100)
                ).fillna(0).astype(int).astype(str) + "%"
                dataframe["MTD Achieved Insurance(%)"] = (
                    (dataframe["MTD Insurance Sales(count)"] / ((days *dataframe["Insurance Target"]) / number_of_days) * 100)
                ).fillna(0).astype(int).astype(str) + "%"
                dataframe = dataframe[desired_columns_order]
                dataframe.to_excel(writer, sheet_name=name, index=False)
