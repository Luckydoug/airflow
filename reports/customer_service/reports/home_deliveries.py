from airflow.models import variable
import pandas as pd
from workalendar.africa import Kenya
import datetime
from datetime import date
from reports.phone_calls.utils.utils import save_file
from reports.customer_service.utils.utils import call_date


def home_deliveries(holidays) -> None:
    home_deliveries = save_file(
        sender="support@optica.africa", subject="", date_since="", path=""
    )

    delivered = home_deliveries[
        home_deliveries["Home Delivery Feedback"].str.contains("Delivered in time")
    ].copy()
    delivered["Order Condition"] = (
        delivered["Home Delivery Feedback"].str.split("-").str[1]
    )
    order_condition_pivot = (
        pd.pivot_table(
            delivered,
            index="Order Condition",
            values="Order Number",
            aggfunc="count",
            margins=True,
            margins_name="Total",
        )
        .reset_index()
        .rename(columns={"Order Number": "Count"})
    )
    order_condition_pivot["Percentage"] = (
        (
            order_condition_pivot["Count"]
            / order_condition_pivot.loc[
                order_condition_pivot["Order Condition"] == "Total", "Count"
            ].iloc[0]
        )
        * 100
    ).round(0)

    home_deliveries["Type"] = (
        home_deliveries["Home Delivery Feedback"].str.split("-").str[0]
    )

    delivery_type = (
        pd.pivot_table(
            home_deliveries,
            index="Type",
            values="Order Number",
            aggfunc="count",
            margins=True,
            margins_name="Total",
            observed=True,
        )
        .reset_index()
        .rename(columns={"Type": "Delivery Type", "Order Number": "Count"})
    )

    delivery_type["Percentage"] = (
        (
            delivery_type["Count"]
            / delivery_type.loc[
                delivery_type["Delivery Type"] == "Total", "Count"
            ].iloc[0]
        )
        * 100
    ).round(0)

    home_deliveries["Status Date"] = pd.to_datetime(
        home_deliveries["Status Date"], format="%d.%m.%y"
    )

    home_deliveries["First Call"] = pd.to_datetime(
        home_deliveries["First Call"], format="%d %b %Y"
    )

    home_deliveries["Call Date"] = home_deliveries["Status Date 1"].apply(
        lambda row: call_date(row)
    )
    home_deliveries["Is First Call After Date"] = (
        home_deliveries["First Call 1"] > home_deliveries["Call Date"]
    )
    home_deliveries["In Time or Not"] = home_deliveries[
        "Is First Call After Date"
    ].apply(lambda row: "In Time" if row == False else "Not In Time")

    intime_pivot = pd.pivot_table(
        home_deliveries,
        index="In Time or Not",
        aggfunc="count",
        values="Ticket Id",
        margins=True,
    )

    intime_pivot["Percentage"] = round(
        (intime_pivot["Ticket Id"] / intime_pivot.loc["All", "Ticket Id"]) * 100, 1
    )
    
    intime_pivot.rename(columns={"Ticket Id": "Count"}, inplace=True)
