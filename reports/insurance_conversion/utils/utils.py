from airflow.models import variable
import pandas as pd
import numpy as np


def check_conversion(row, sales):
    sales_orders = sales["Order Number"].tolist()
    if row["Order Number"] in sales_orders:
        return True
    else:
        customer_code = row["Customer Code"]
        customer_orders = sales[sales["Customer Code"].astype(str) == str(customer_code)]
        later_orders = customer_orders[
            (customer_orders["Order Create Date"] >= row["Order Create Date"]) &
            (customer_orders["Order Number"] != row["Order Number"])
        ]
        if len(later_orders):
            return True
        else:
            return False


def check_number_requests(row, data):
    if row["Converted"] == True:
        return 1
    elif row["Converted"] == False and row["Dif Order Converted"] == False:
        customer_orders = data[data["Customer Code"] == row["Customer Code"]]
        if len(customer_orders) == 1:
            return 1
        elif len(customer_orders) > 1 and row["Order Create Date"] == customer_orders["Order Create Date"].max():
            return 1
        else:
            return 0
    else:
        customer_orders = data[data["Customer Code"] == row["Customer Code"]]
        if len(customer_orders) == 1:
            return 1
        else:
            statuses = customer_orders["Status"].tolist()
            if len(customer_orders) > 1 and row["Status"] == "Cancel" and "Open" not in statuses and "Close" not in statuses and row["Order Create Date"] == customer_orders["Order Create Date"].max():
                return 1
            else:
                return 0


def calculate_conversion(row, data):
    if row["Converted"] == True:
        return 1
    elif row["Converted"] == False and row["Dif Order Converted"] == False:
        return 0
    elif row["Converted"] == False and row["Dif Order Converted"] == True and row["Requests"] == 1:
        return 1
    elif row["Converted"] == False and row["Dif Order Converted"] == True and row["Status"] == "Cancel":
        return 0
    else:
        customer_orders = data[data["Customer Code"] == row["Customer Code"]]
        if len(customer_orders) == 1 and row["Dif Order Converted"] == True:
            return 1
        elif len(customer_orders) > 1 and row["Status"] == "Cancel":
            return 0


def date_in_range(date, start_date, end_date):
    if start_date <= date <= end_date:
        return True
    return False


def get_rm_srm_total(dataframe):
    df = dataframe.copy()
    df = df.sort_values(by=["SRM", "RM"])
    df = df.reset_index()

    grouped_srm = df.groupby('SRM').sum(numeric_only=True)
    srm_names = grouped_srm.index.tolist()
    grouped_rm = df.groupby('RM').sum(numeric_only=True)

    grouped_rm[('Insurance Fully Approved', '%Conversion')] = round(
        (grouped_rm[('Insurance Fully Approved',   'Converted')] / grouped_rm[('Insurance Fully Approved',    'Requests')]) * 100, 0)
    grouped_rm[('Insurance Partially Approved', '%Conversion')] = round(
        (grouped_rm[('Insurance Partially Approved',   'Converted')] / grouped_rm[('Insurance Partially Approved',    'Requests')]) * 100, 0)
    grouped_rm[('Use Available Amount on SMART', '%Conversion')] = round(
        (grouped_rm[(('Use Available Amount on SMART',   'Converted'))] /
         grouped_rm[('Use Available Amount on SMART',    'Requests')]) * 100
    )

    grouped_srm[('Insurance Fully Approved', '%Conversion')] = round(
        (grouped_srm[('Insurance Fully Approved',   'Converted')] / grouped_srm[('Insurance Fully Approved',    'Requests')]) * 100, 0)
    grouped_srm[('Insurance Partially Approved', '%Conversion')] = round(
        (grouped_srm[('Insurance Partially Approved',   'Converted')] / grouped_srm[('Insurance Partially Approved',    'Requests')]) * 100, 0)
    grouped_srm[('Use Available Amount on SMART', '%Conversion')] = round(
        (grouped_srm[(('Use Available Amount on SMART',   'Converted'))] /
         grouped_srm[('Use Available Amount on SMART',    'Requests')]) * 100
    )

    rm_names = grouped_rm.index.tolist()
    columns = grouped_srm.columns
    for srm in srm_names:
        new_row = {('Outlet', ''): None, ('RM', ''): None,
                   ('SRM', ''): str(f"Total ({srm})")}
        for i in range(len(columns)):
            new_row[columns[i]] = grouped_srm.loc[srm, columns[i]]

        if not ((df['SRM'] == f"Total ({srm})")).any():
            last_srm_index = df[df['SRM'] == srm].index[-1]
            df = pd.concat([df.loc[:last_srm_index], pd.DataFrame(new_row, index=[
                           last_srm_index + 1], columns=df.columns), df.loc[last_srm_index + 1:]])

    for rm in rm_names:
        new_row = {('Outlet', ''): None, ('RM', ''): str(
            f"Total ({rm})"), ('SRM', ''): None}
        for i in range(len(columns)):
            new_row[columns[i]] = grouped_rm.loc[rm, columns[i]]

        if not ((df['RM'] == f"Total ({rm})")).any():
            last_rm_index = df[df['RM'] == rm].index[-1]
            df = pd.concat([df.loc[:last_rm_index], pd.DataFrame(new_row, index=[
                           last_rm_index + 1], columns=df.columns), df.loc[last_rm_index + 1:]])

    return df
