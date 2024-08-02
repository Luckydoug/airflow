import pandas as pd
from datetime import datetime, timedelta
from sub_tasks.libraries.styles import bi_weekly
import calendar
import numpy as np
import matplotlib.pyplot as plt


def get_date_range():
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
first_range_end = "2024-04-30"
second_range_start = "2024-05-01"
second_range_end = "2024-05-31"


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


thresholds = {
    "Times Opened Late(Thresh = 0)": 0,
    "Error Deductable": 0,
    "SOPs/ Customers": 5,
    "NPS Score(Target = 90)": 90,
    "Google Reviews Average Rating": 4.9,
    "Optom Low RX Conversion (Target = 65)": 65,
    "EWC Low RX Conversion (Target = 65)": 65,
    "Printing Identifier Efficiency (Target = 5 Mins)": 90,
    "Customer Issue During Collection": 0,
    "Use Available Amount Conversion (Target = 100%)": 100,
    "Declined by Insurance Conversion (Target = 20%)": 20,
    "Approval Received from Insurance to Update Approval on SAP (Target = 90% in 5 Minutes)": 90,
    "Insurance Feedback to Customer Contacted time taken (Target = 90% in 10 Minutes)": 90,
    "Orders Collected and Discounted Same Day": 0,
    "Corrected Forms Resent (Target = 90% in 5 Minutes)": 90,
    # "Viewed Eyetest Older than 30 Days Conversion": 85,
    "Eye Test to Order Efficiency (Target = 90% in 45 minutes)": 90,
    "Approval Received to SMART Forwarded Efficiency (Target = 90% in 60 Minutes)": 90
}

zero_threshold = [
    "Customer Issue During Collection",
    "Orders Collected and Discounted Same Day",
    "Times Opened Late(Thresh = 0)",
    "Error Deductable"
]


def return_multindex_thresholds(data):
    multindex_thresholds = {}
    column_level = list(set(data.columns.get_level_values(0)))
    date_level = list(set(data.columns.get_level_values(1)))

    for column in column_level:
        if column in thresholds:
            threshold = thresholds[column]
            for date in date_level:
                multindex_thresholds[(column, date)] = threshold

    return multindex_thresholds


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

                elif column == "Error Deductable":
                    if numeric_value > 0:
                        styles[row.index.get_loc(column)] = "background-color: red"       

                elif numeric_value < thresholds[column]:
                    styles[row.index.get_loc(column)] = "background-color: red"
            except (ValueError, TypeError):
                pass
    return styles


def highlight_threshold_multindex(row, thresholds):
    styles = [""] * len(row)
    for column, value in row.items():
        if column in thresholds.keys():
            try:
                numeric_value = float(value)
                if "Times Opened Late(Thresh = 0)" in column[0]:
                    if numeric_value > 0:
                        styles[row.index.get_loc(column)] = "background-color: red"

                elif "Customer Issue During Collection" in column[0]:
                    if numeric_value > 0:
                        styles[row.index.get_loc(column)] = "background-color: red"

                elif "Orders Collected and Discounted Same Day" in column[0]:
                    if numeric_value > 0:
                        styles[row.index.get_loc(column)] = "background-color: red"

                elif "SOPs/ Customers" in column[0]:
                    if numeric_value > 5:
                        styles[row.index.get_loc(column)] = "background-color: red"

                elif "Error Deductable" in column[0]:
                    if numeric_value > 0:
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
        branch_data = branch_data.drop(columns=["Branch"])

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

    tab_colors = [
        "#FF6347",
        "#4682B4",
        "#32CD32",
        "#FFD700",
        "#6A5ACD",
        "#FF69B4",
        "#8A2BE2",
    ]

    for index, (key, value) in enumerate(dataframes.items()):
        if not isinstance(value,pd.DataFrame) and pd.isna(value):
            continue
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


def get_last_two_months(mode = None):
    today = datetime.today()

    def month_start_end(year, month):
        start_date = datetime(year, month, 1)
        _, last_day = calendar.monthrange(year, month)
        end_date = datetime(year, month, last_day)
        return start_date, end_date

    current_month = today.month
    current_year = today.year

    if current_month == 1:
        prev_month_1 = 12
        prev_month_2 = 11
        year_1 = current_year - 1
        year_2 = current_year - 1
    elif current_month == 2:
        prev_month_1 = 1
        prev_month_2 = 12
        year_1 = current_year
        year_2 = current_year - 1
    else:
        prev_month_1 = current_month - 1
        prev_month_2 = current_month - 2
        year_1 = current_year
        year_2 = current_year

    start_date_1, end_date_1 = month_start_end(year_1, prev_month_1)
    start_date_2, end_date_2 = month_start_end(year_2, prev_month_2)

    month_name_1 = calendar.month_name[prev_month_1]
    month_name_2 = calendar.month_name[prev_month_2]

    if mode == "Dates":
        return (
        start_date_2.strftime("%Y-%m-%d"),
        end_date_2.strftime("%Y-%m-%d"),
        start_date_1.strftime("%Y-%m-%d"),
        end_date_1.strftime("%Y-%m-%d"),
        )

    else:
        return (month_name_2,  month_name_1)
    


# def reorder_dataframe(df):
#     priority_columns = ['Active Days', 'Missed Metrics']

#     conversion_cols = all_columns[-4:]
#     low_priority_columns = all_columns[:6]


#     margin_dict = {}
#     for col in df.columns:
#         if col in priority_columns:
#             continue
#         if col in thresholds:
#             margin = df[col].iloc[0] - thresholds[col] if not pd.isnull(df[col].iloc[0]) else float('inf')
#             margin_dict[col] = margin
#         else:
#             margin_dict[col] = -float('inf') if pd.isnull(df[col].iloc[0]) else float('inf')
    
#     sorted_columns = sorted(margin_dict, key=margin_dict.get, reverse=False)
    
#     columns_with_nans = [col for col in sorted_columns if pd.isnull(df[col].iloc[0])]
#     # columns_without_nans = [col for col in sorted_columns if col not in columns_with_nans]
#     # final_columns = [col for col in columns_without_nans if col not in conversion_columns]
#     for col in low_priority_columns:
#         if col in columns_with_nans:
#             low_priority_columns.remove(col)

#     for col in conversion_cols:
#         if col in columns_with_nans:
#             conversion_cols.remove(col)
    

#     # conversion_cols + columns_with_nans
#     ordered_columns = priority_columns + low_priority_columns
#     other_cols = [col for col in sorted_columns if col not in ordered_columns and col not in conversion_cols and col not in columns_with_nans]
#     final_list = ordered_columns + other_cols + conversion_cols + columns_with_nans

#     df = df[ordered_columns]
#     return final_list


def reorder_dataframe(df):
    priority_columns = ['Active Days', 'Missed Metrics']
    
    conversion_cols = all_columns[-4:]
    low_priority_columns = all_columns[:6]
    
    margin_dict = {}
    for col in df.columns:
        if col in priority_columns:
            continue
        if col in thresholds:
            margin = df[col].iloc[0] - thresholds[col] if not pd.isnull(df[col].iloc[0]) else float('inf')
            margin_dict[col] = margin
        else:
            margin_dict[col] = -float('inf') if pd.isnull(df[col].iloc[0]) else float('inf')
    
    sorted_columns = sorted(margin_dict, key=margin_dict.get, reverse=False)
    
    columns_with_nans = [col for col in sorted_columns if pd.isnull(df[col].iloc[0])]
    
    # Make copies to avoid modifying the list while iterating
    low_priority_columns_copy = low_priority_columns.copy()
    for col in low_priority_columns_copy:
        if col in columns_with_nans:
            low_priority_columns.remove(col)
    
    conversion_cols_copy = conversion_cols.copy()
    for col in conversion_cols_copy:
        if col in columns_with_nans:
            conversion_cols.remove(col)
    
    ordered_columns = priority_columns + low_priority_columns
    other_cols = [col for col in sorted_columns if col not in ordered_columns and col not in conversion_cols and col not in columns_with_nans]
    final_list = ordered_columns + other_cols + conversion_cols + columns_with_nans
    
    # df = df[final_list]
    return final_list








def plot_trends(data, path, name) -> str:
    image_path = f"{path}bi_weekly_report/{name}.png"
    conversion_data = data["Overall Conversion"].to_list()
    plt.figure(figsize=(10, 2))
    plt.plot(conversion_data, marker="o", linestyle="-")

    for i, value in enumerate(conversion_data):
        plt.text(i, value, str(value), ha="center", va="bottom", fontsize=12)

    plt.ylim(20, 105)
    plt.axis("off")
    plt.savefig(image_path, bbox_inches="tight")
    plt.close()

    return image_path


all_columns = [
    "Times Opened Late(Thresh = 0)",
    "Warranties Not Returned",
    "SOPs/ Customers",
    "NPS Score(Target = 90)",
    "Google Reviews Average Rating",
    "Error Deductable",
    "Printing Identifier Efficiency (Target = 5 Mins)",
    "Customer Issue During Collection",
    "Quality Rejected at Branch",
    "Orders Collected and Discounted Same Day",
    "Average Eye Test Time",
    "Eye Test to Order Efficiency (Target = 90% in 45 minutes)",
    "Corrected Forms Resent (Target = 90% in 5 Minutes)",
    "Insurance Feedback to Customer Contacted time taken (Target = 90% in 10 Minutes)",
    "Approval Received to SMART Forwarded Efficiency (Target = 90% in 60 Minutes)",
    "Approval Received from Insurance to Update Approval on SAP (Target = 90% in 5 Minutes)",
    "Use Available Amount Conversion (Target = 100%)",
    "Declined by Insurance Conversion (Target = 20%)",
    "Optom Low RX Conversion (Target = 65)",
    "EWC Low RX Conversion (Target = 65)"
]


    





