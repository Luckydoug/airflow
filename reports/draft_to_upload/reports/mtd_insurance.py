from airflow.models import variable
import pandas as pd
import numpy as np
from reports.insurance_conversion.utils.utils import(
    check_conversion,
    calculate_conversion,
    check_number_requests,
    create_branch_report
)
from reports.draft_to_upload.utils.utils import today, get_start_end_dates

def create_mtd_insurance_conversion(
    all_orders: pd.DataFrame,
    orderscreen: pd.DataFrame,
    insurance_companies: pd.DataFrame,
    sales_orders: pd.DataFrame,
    path: str,
    branch_data: pd.DataFrame,
    selection: str,
    date
) -> None:
    if not len(all_orders) or not len(orderscreen) or not len(insurance_companies) or not len(sales_orders):
        return

    preauths = [
        "Sent Pre-Auth to Insurance Company",
        "Resent Pre-Auth to Insurance Company"
    ]

    preauth_requests = orderscreen[
        orderscreen.Status.isin(preauths)
    ]

    requests = preauth_requests.sort_values(by = ["Date", "Time"], ascending = True)
    unique_requests = requests.drop_duplicates(subset = "Order Number", keep = "last")
    unique_requests_rename = unique_requests.rename(columns={"Status": "Request"})
    unique_requests_rename.loc[:, "Full Request Date"] = pd.to_datetime(
        unique_requests_rename["Date"].astype(str) + " " +
        unique_requests_rename["Time"].astype(str),
        format="%Y-%m-%d %H:%M:%S"
    )

    insurance_feedbacks = [
        'Insurance Fully Approved',
        'Insurance Partially Approved',
        'Declined by Insurance Company',
        'Use Available Amount on SMART'
    ]

    feedbacks = orderscreen[orderscreen.Status.isin(insurance_feedbacks)]
    feedbacks_asc = feedbacks.sort_values(by = ["Date","Time"],ascending=True)
    unique_feedbacks = feedbacks_asc.drop_duplicates(subset = "Order Number", keep = "last")
    unique_feedback_rename = unique_feedbacks.rename(columns={"Status": "Feedback"})
    unique_feedback_rename['Datee'] = unique_feedback_rename['Remarks'].str.extract('(\d{4}-\d{2}-\d{2})')[0]
    unique_feedback_rename['Timee'] = unique_feedback_rename['Remarks'].str.extract('\| \d{4}-\d{2}-\d{2} & (\d{4})')[0]
    unique_feedback_rename['Feedback Time'] = pd.to_datetime(unique_feedback_rename['Datee'] + ' ' + unique_feedback_rename['Timee'], format='%Y-%m-%d %H%M')


    unique_feedback_rename.loc[:, "Full Feedback Date"] = pd.to_datetime(
        unique_feedback_rename["Date"].astype(str) + " " +
        unique_feedback_rename["Time"].astype(str),
        format="%Y-%m-%d %H:%M:%S"
    )

    requests_feedbacks = pd.merge(
        unique_requests_rename[["Order Number", "Request", "Full Request Date"]],
        unique_feedback_rename[["Order Number", "Feedback", "Feedback Time", "Full Feedback Date", "Remarks"]], 
        on = "Order Number", 
        how = "right"
    )

    unique_request_feedbacks = requests_feedbacks[
        ~requests_feedbacks["Feedback"].isna()
    ].drop_duplicates(subset = "Order Number") 


    all_orders["Order Create Date"] = pd.to_datetime(all_orders["CreateDate"],dayfirst=True)
    orders_req_feed = pd.merge(
        all_orders[[
            "Order Number", 
            "Order Create Date", 
            "Status", 
            "Current Status",
            "% Approved",
            "Customer Code", 
            "Creator", 
            "Order Creator", 
            "Outlet"
        ]], 
        unique_request_feedbacks, 
        on = "Order Number", 
        how = "right"
    )

    req_feed_comp = pd.merge(
        orders_req_feed, 
        insurance_companies[["Insurance Company", "Scheme Type", "Order Number"]], 
        on = "Order Number",
        how="left"
    )

    req_feed_sales = pd.merge(
        req_feed_comp, 
        sales_orders[["Order Number", "DocEntry"]], 
        on = "Order Number", 
        how = "left"
    )
    req_feed_sales["Converted"] = ~req_feed_sales["DocEntry"].isna()
    pivoting_data = req_feed_sales.copy()

    order_sales = pd.merge(
        sales_orders, 
        all_orders[
            ["Order Create Date", "Order Number", "Customer Code"]
        ], 
        on = "Order Number", 
        how = "left" 
    )
    
    
    feed_sales = pivoting_data[
        (pivoting_data["Full Feedback Date"].dt.date >= pd.to_datetime(date, format="%Y-%m-%d").date()) &
        (pivoting_data["Full Feedback Date"].dt.date <= pd.to_datetime(today, format="%Y-%m-%d").date()) 
    ]


    final = feed_sales.copy()
    final["Dif Order Converted"] = np.nan
    final["Dif Order Converted"] = final.apply(lambda row: check_conversion(row, order_sales), axis=1)

    data = final.copy()
    final["Requests"] = final.apply(lambda row: check_number_requests(row, data, order_sales), axis= 1)  
    final["Conversion"] = final.apply(lambda row: calculate_conversion(row, data), axis = 1)

    final = final.drop_duplicates(subset=["Order Number"])
    # final = final.dropna(subset = ["Insurance Company"])
    comparison_data = final.copy()

    pstart, pend = get_start_end_dates(selection=selection)


    mtd_data = comparison_data[
        (comparison_data["Full Feedback Date"].dt.date >= pd.to_datetime(pstart, format="%Y-%m-%d").date()) &
        (comparison_data["Full Feedback Date"].dt.date <= pd.to_datetime(today, format="%Y-%m-%d").date())
    ]

    mtd_pivot = create_branch_report(
        mtd_data
    )

    non_converted = mtd_data[
        (mtd_data["Conversion"] == 0) & 
        (mtd_data["Requests"] == 1) & 
        (mtd_data["Feedback"] != "Declined by Insurance Company")
    ]


    with pd.ExcelWriter(f"{path}draft_upload/mtd_insurance_conversion.xlsx", engine='xlsxwriter') as writer: 
        mtd_pivot.to_excel(writer, sheet_name = "MTD Summary") 
        non_converted[
            [
                "Order Number", 
                "Outlet", 
                "Order Creator", 
                "Order Create Date", 
                "Status", 
                "Customer Code", 
                "Insurance Company", 
                "Scheme Type", 
                "Feedback"
            ]
        ].sort_values(by= "Outlet").to_excel(writer,sheet_name = "NON Conversions", index=False)  
        mtd_data.to_excel(writer, sheet_name = "Master Data", index = False)
            
    writer.save()





         






