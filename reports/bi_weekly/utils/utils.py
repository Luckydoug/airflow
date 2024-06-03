import pandas as pd
from datetime import datetime, timedelta
from sub_tasks.libraries.styles import bi_weekly
import numpy as np


def get_date_range(mode):
    today = datetime.now().date()
    end_date = today - timedelta(days=30)
    start_date = end_date
    date_range = []
    for i in range(2):
        end_date = start_date + timedelta(days=14)
        date_range.append((start_date, end_date))
        start_date = end_date + timedelta(days=1)
    return date_range


date_ranges = get_date_range()
first_range_start = (date_ranges[0][0]).strftime("%Y-%m-%d")
first_range_end = date_ranges[0][1].strftime("%Y-%m-%d")
second_range_start = date_ranges[1][0].strftime("%Y-%m-%d")
second_range_end = date_ranges[1][1].strftime("%Y-%m-%d")

first_range_start = "2024-04-01"
first_range_end = "2024-04-15"
second_range_start = "2024-04-16"
second_range_end = "2024-04-30"


def return_week_ranges():
    first = (
        str(pd.to_datetime(first_range_start).strftime("%b-%d"))
        + " "
        + "to"
        + " "
        + str(pd.to_datetime(first_range_end).strftime("%b-%d"))
    )
    second = (
        str(pd.to_datetime(second_range_start).strftime("%b-%d"))
        + " "
        + "to"
        + " "
        + str(pd.to_datetime(second_range_end).strftime("%b-%d"))
    )

    return [first, second]


def convert_values(val):
    if pd.isna(val):
        return ""
    if val != "nan":
        if isinstance(val, str):
            if "g" in val:
                return val.split("g")[0]
            return val
        else:
            return str(int(val))
    else:
        return ""


def highlight_threshold(row):
    styles = [""] * len(row)
    for column, value in row.items():
        if column in thresholds:
            try:
                numeric_value = float(value)
                if column == "Times Opened Late(Thresh = 0)":
                    if numeric_value > 0:
                        styles[row.index.get_loc(column)] = "background-color: red"

                elif column == "Customer Issue During Collection":
                    if numeric_value > 0:
                        styles[row.index.get_loc(column)] = "background-color: red"

                elif column == "Orders Collected and Discounted Same Day":
                    if numeric_value > 0:
                        styles[row.index.get_loc(column)] = "background-color: red"

                elif column == "SOPs/ Customers":
                    if numeric_value > 5:
                        styles[row.index.get_loc(column)] = "background-color: red"

                elif numeric_value < thresholds[column]:
                    styles[row.index.get_loc(column)] = "background-color: red"
            except (ValueError, TypeError):
                pass
    return styles


def highligh_overall(row):
    styles = [""] * len(row)
    for column, value in row.items():
        if value == "OVERALL":
            styles[row.index.get_loc(column)] = "font-weight: bold"
    return styles


def html_style_dataframe(dataframe):
    dataframe.replace(" ", np.nan, inplace=True)
    dataframe = dataframe.dropna(axis=1, how="all")
    dataframe = dataframe.iloc[:, 1:]
    dataframe_style = (
        dataframe.style.hide_index()
        .set_table_styles(bi_weekly)
        .apply(highlight_threshold, axis=1)
        .apply(highligh_overall, axis=1)
        .format(convert_values)
    )
    dataframe_html = dataframe_style.to_html(doctype_html=True)
    return dataframe_html


def custom_sort(value):
    return value != "OVERALL", value


def highlight_overall(row):
    if row["Payroll Number"] == "OVERALL":
        return ["background-color: lightgray", "font-weight: bold"]
    else:
        return ["", ""]


thresholds = {
    "Times Opened Late(Thresh = 0)": 0,
    "SOPs/ Customers": 5,
    "NPS Score(Target = 90)": 90,
    "Google Reviews Average Rating": 4.9,
    "Optom Low RX Conversion (Target = 65)": 75,
    "Optom Low RX Conversion (Target = 65)": 75,
    "Printing Identifier Efficiency (Target = 5 Mins)": 90,
    "Customer Issue During Collection": 0,
    "Use Available Amount Conversion": 100,
    "Declined Conversion": 20,
    "Approval Received from Insurance to Update Approval on SAP (Target = 90% in 5 Minutes)": 90,
    "Insurance Feedback to Customer Contacted time taken (Target = 90% in 60 Minutes)": 90,
    "Use Available Amount Conversion": 100,
    "Orders Collected and Discounted Same Day": 0,
    "Corrected Forms Resent (Target = 90% in 5 Minutes)": 90,
    "Viewed Eyetest Older than 30 Days Conversion": 85,
    "Eye Test to Order Efficiency (Target = 90% in 45 minutes)": 90
}

zero_threshold = [
    "Customer Issue During Collection",
    "Orders Collected and Discounted Same Day",
    "Times Opened Late(Thresh = 0)",
]


def metrics_missed(row) -> int:
    metrics_missed = 0
    for key, value in thresholds.items():
        if not pd.isna(row[key]):
            if key in zero_threshold and row[key] > 0:
                metrics_missed += 1
            elif key == "SOPs/ Customers":
                if row[key] > 5:
                    metrics_missed += 1
            else:
                if row[key] < value:
                    metrics_missed += 1
                else:
                    metrics_missed += 0
        else:
            metrics_missed += 0

    return metrics_missed


def add_metrics_missed(data):
    data["Missed Metrics"] = data.apply(lambda row: metrics_missed(row), axis=1)
    columns = list(data.columns)
    insert_position = columns.index("Active Days") + 1
    columns.insert(insert_position, columns.pop(columns.index("Missed Metrics")))
    data = data[columns]

    return data


def highlight_first_row(row):
    color = "#FFFF99"
    if "OVERALL" in row.values:
        return [
            "background-color: {}".format(color) + "; font-weight: bold" for v in row
        ]
    else:
        return ["" for v in row.values]


def parse_data(excel_file, sheet_name):
    try:
        return excel_file.parse(sheet_name, index_col=False)
    except ValueError as e:
        if f"Worksheet named '{sheet_name}' not found" in str(e):
            print(f"Sheet '{sheet_name}' does not exist. Returning an empty DataFrame.")
            return pd.DataFrame()
        else:
            raise


def return_branch_data(dataframe, branch):
    if not dataframe.empty:
        branch_data = dataframe[dataframe["Branch"] == branch]

        return branch_data
    else:
        pd.DataFrame()


def write_to_excel(dataframes, writer) -> None:
 
    border_format = writer.book.add_format({"border": 1, "font_size": 10})
    header_format = writer.book.add_format(
        {
            "bold": True,
            "align": "center",
            "fg_color": "#DCE6F1",
        }
    )
    
    tab_colors = ["#FF6347", "#4682B4", "#32CD32", "#FFD700", "#6A5ACD", "#FF69B4", "#8A2BE2"]

    for index, (key, value) in enumerate(dataframes.items()):
        if not value.empty:
            style = value.style.hide_index()

            style.to_excel(writer, index=False, sheet_name=key)
            worksheet = writer.sheets[key]
            worksheet.set_column(0, len(style.columns) - 1, 15)

            # Set the tab color
            color = tab_colors[index % len(tab_colors)] 
            worksheet.set_tab_color(color)

            for col_num, column_name in enumerate(style.columns.values):
                worksheet.write(0, col_num, column_name, header_format)

            worksheet.conditional_format(
                0,
                0,
                style.data.shape[0],
                style.data.shape[1],
                {"type": "no_errors", "format": border_format},
            )
            worksheet.freeze_panes(1, 3)
