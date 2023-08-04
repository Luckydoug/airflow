from airflow.models import variable
import pandas as pd
import numpy as np
from reports.insurance_conversion.utils.utils import(
    check_conversion,
    calculate_conversion,
    check_number_requests,
    get_rm_srm_total
)

from sub_tasks.libraries.utils import (
    fourth_week_start,
    fourth_week_end,
    first_week_start,
    check_date_range
)

def create_insurance_conversion(
    all_orders: pd.DataFrame,
    orderscreen: pd.DataFrame,
    insurance_companies: pd.DataFrame,
    sales_orders: pd.DataFrame,
    path: str,
    branch_data: pd.DataFrame
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
    unique_requests_rename["Full Request Date"] = pd.to_datetime(unique_requests_rename["Date"],dayfirst=True)

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
    unique_feedback_rename["Full Feedback Date"] = pd.to_datetime(unique_feedback_rename["Date"],dayfirst=True)

    requests_feedbacks = pd.merge(
        unique_requests_rename[["Order Number", "Request", "Full Request Date"]],
        unique_feedback_rename[["Order Number", "Feedback", "Full Feedback Date"]], 
        on = "Order Number", 
        how = "right"
    )

    unique_request_feedbacks = requests_feedbacks[
        ~requests_feedbacks["Feedback"].isna()
    ].drop_duplicates(subset = "Order Number") 

    all_orders["Order Create Date"] = pd.to_datetime(all_orders["CreateDate"],dayfirst=True)
    print(all_orders)
    orders_req_feed = pd.merge(
        all_orders[[
            "Order Number", 
            "Order Create Date", 
            "Status", 
            "Customer Code", 
            "Creator", 
            "Order Creator", 
            "Outlet"
        ]], 
        unique_request_feedbacks, 
        on = "Order Number", 
        how = "inner"
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

    order_sales = pd.merge(
        sales_orders, 
        all_orders[
            ["Order Create Date", "Order Number", "Customer Code"]
        ], 
        on = "Order Number", 
        how = "left" 
    )

    req_feed_sales = req_feed_sales[
        (req_feed_sales["Full Feedback Date"] >= first_week_start) &
        (req_feed_sales["Full Feedback Date"] <= fourth_week_end) 
    ]

    final = req_feed_sales.copy()

    final["Dif Order Converted"] = np.nan
    final["Dif Order Converted"] = final.apply(lambda row: check_conversion(row, order_sales), axis=1)

    data = final.copy()
    final["Requests"] = final.apply(lambda row: check_number_requests(row, data), axis= 1)  
    final["Conversion"] = final.apply(lambda row: calculate_conversion(row, data), axis = 1)

    final = final.drop_duplicates(subset=["Order Number"])
    final = final.dropna(subset = ["Insurance Company"])
    comparison_data = final.copy()

    final_data = final[
        (final["Full Feedback Date"] >= fourth_week_start) & 
        (final["Full Feedback Date"] <=  fourth_week_end)
    ]

    feedbacks = final_data.copy()
    feedbacks["Feedback"] = pd.Categorical(
        feedbacks["Feedback"],
        categories=[
            "Insurance Fully Approved", 
            "Insurance Partially Approved", 
            "Use Available Amount on SMART", 
            "Declined by Insurance Company"
        ]
    )

    insurance_feedback_pivot = pd.pivot_table(
        feedbacks, 
        index = ["Outlet", "Feedback"], 
        values = ["Requests", "Conversion"], 
        aggfunc="sum").reset_index().rename(columns={"Conversion": "Converted"}
    )

    final_branch = insurance_feedback_pivot[["Outlet", "Feedback", "Requests", "Converted"]]
    final_branch["Target Conversion"] = np.where(
        final_branch["Feedback"] == "Declined by Insurance Company", "20%", 
        np.where(
        final_branch["Feedback"] == "Insurance Fully Approved", "100%",
        np.where(
        final_branch["Feedback"] == "Insurance Partially Approved", "95%", "100%"))
    )
    final_branch["Actual Conversion"] = round(
        (final_branch["Converted"]/ final_branch["Requests"]) * 100, 0
    ).fillna(0).replace([np.inf, -np.inf], 0).astype(int).astype(str) + "%"
    final_branch = final_branch[[
        "Outlet",
        "Feedback", 
        "Requests", 
        "Converted", 
        "Actual Conversion", 
        "Target Conversion"
    ]]

    individual_pivot = pd.pivot_table(
        final_data, 
        index = ["Outlet", "Order Creator"], 
        columns="Feedback", 
        values = ["Requests", "Conversion"], 
        aggfunc="sum"
    ).fillna(0)

    individual_pivot2 = individual_pivot.rename(columns={"Conversion": "Converted", "Count": "Requests"}) 

    stacked = individual_pivot2.stack()
    stacked[["Requests", "Converted"]] = stacked[["Requests", "Converted"]].astype(int)
    stacked["Conversion"] = round(stacked["Converted"] / stacked["Requests"] * 100, 0).fillna(0).astype(int).astype(str) + "%"
    stacked2 = stacked.unstack()

    final_individual = stacked2.swaplevel(0, 1, 1).sort_index(level = 1).reindex(["Requests", "Conversion"],axis = 1, level = 1)
    final_individual = final_individual.reindex(
        ["Insurance Fully Approved", 
         "Insurance Partially Approved", 
         "Use Available Amount on SMART", 
         "Declined by Insurance Company"
        ], 
        axis = 1, 
        level = 0
    )

    final_data = pd.merge(final_data, branch_data[["Outlet", "RM", "SRM"]], on = "Outlet", how = "left")
    branch_performance = pd.pivot_table(
        final_data, 
        index = ["Outlet", "RM", "SRM"], 
        columns = "Feedback", 
        aggfunc="sum", 
        values= ["Requests", "Conversion"]
    ).fillna(0)

    branch_performance =  branch_performance.rename(columns={"Conversion": "Converted"}) 
    branch_stacked = branch_performance.stack()
    branch_stacked[["Requests", "Converted"]] = branch_stacked[["Requests", "Converted"]].astype(int)
    branch_stacked["%Conversion"] = round(branch_stacked["Converted"] / branch_stacked["Requests"] * 100, 0).fillna(0).astype(int)
    branch_unstacked = branch_stacked.unstack()

    branch_final = branch_unstacked.swaplevel(0, 1, 1).sort_index(level = 1).reindex(["Requests", "Converted", "%Conversion"],axis = 1, level = 1)
    branch_final = branch_final.reindex(
        ["Insurance Fully Approved", "Insurance Partially Approved", "Use Available Amount on SMART"], 
        axis = 1, 
        level = 0
    )

    branch_final = branch_final.reindex(
        ["Requests", "Converted", "%Conversion"], 
        axis = 1, 
        level = 1
    )

    columns_todo = [
        ("Insurance Fully Approved", "Requests"),
        ("Insurance Fully Approved", "Converted"),
        ("Insurance Fully Approved", "%Conversion"),
        ("Insurance Partially Approved", "Requests"),
        ("Insurance Partially Approved", "Converted"),
        ("Insurance Partially Approved", "%Conversion"),
        ("Use Available Amount on SMART", "Requests"),
        ("Use Available Amount on SMART", "Converted"),
        ("Use Available Amount on SMART", "%Conversion")
    ]

    branch_final =  branch_final.reindex(
        columns_todo, 
        axis=1, 
        fill_value=0
    )

    branch_final = get_rm_srm_total(branch_final)
    new_cols = [(y, x) if x in ['Outlet', 'RM', 'SRM'] else (x, y) for x, y in branch_final.columns]
    branch_final.columns = pd.MultiIndex.from_tuples(new_cols)
    branch_final = branch_final.fillna(" ")

    overall_performance = pd.pivot_table(
        feedbacks, 
        index = ["Feedback"], 
        values = ["Requests", "Conversion"], 
        aggfunc="sum").reset_index().rename(columns={"Conversion": "Converted", "Count": "Requests"}
    )

    comparison_data["Week Range"] = comparison_data.apply(
        lambda row: check_date_range(row, "Full Feedback Date"), 
        axis = 1
    )
    complete = comparison_data[comparison_data["Week Range"] != "None"]

    complete["Feedback"] = pd.Categorical(
        complete["Feedback"], 
        categories=[
            "Insurance Fully Approved",
            "Insurance Partially Approved", 
            "Use Available Amount on SMART", 
            "Declined by Insurance Company"
        ], 
        ordered=True
    )

    conversion = pd.pivot_table(
        complete, 
        index = "Feedback", 
        columns=["Week Range"], 
        values = ["Requests", "Conversion"], 
        aggfunc="sum"
    ).fillna(0)

    stacked_conversion = conversion.stack()
    stacked_conversion["%Conversion"] = round(
        (stacked_conversion["Conversion"] / stacked_conversion["Requests"]) * 100, 1
    ).fillna(0).astype(str) + "%"
    unstacked_conversion = stacked_conversion.unstack()

    stack_columns = unstacked_conversion.columns
    company_conversion = unstacked_conversion.swaplevel(0, 1, 1).sort_index(level=1, axis=1).reindex(["Requests", "%Conversion"],axis = 1, level = 1)
    multi_columns = company_conversion.columns
    dates = []
    for col in multi_columns:
        date_range = col[0]
        start_date = pd.to_datetime(date_range.split(" to ")[0])
        dates.append(start_date)

    unique_dates = list(set(dates))
    sorted_dates = sorted(unique_dates)
    sorted_columns = []

    for date in sorted_dates:
        date_range = f"{date.strftime('%Y-%b-%d')} to " + f"{(date + pd.Timedelta(6, unit='d')).strftime('%Y-%b-%d')}"
        sorted_columns.append(date_range)

    company_conversion = company_conversion.reindex(sorted_columns,axis = 1, level = 0)
    company_conversion = company_conversion.reindex(["Requests", "%Conversion"], level = 1, axis = 1)

    with pd.ExcelWriter(f"{path}insurance_conversion/overall.xlsx", engine='xlsxwriter') as writer:    
       for group, dataframe in final_branch.groupby('Outlet'):
            name = f'{group}'
            dataframe[
                [
                    "Feedback", 
                    "Requests", 
                    "Converted", 
                    "Actual Conversion", 
                    "Target Conversion"
                ]
            ].to_excel(writer,sheet_name=name, index=False)          
    writer.save()

    with pd.ExcelWriter(f"{path}insurance_conversion/individual.xlsx", engine='xlsxwriter') as writer:    
       for group, dataframe in final_individual.groupby(level = ["Outlet"], axis=0):
            name = f'{group}'
            dataframe[
                [
                    "Insurance Fully Approved", 
                    "Insurance Partially Approved", 
                    "Use Available Amount on SMART",
                    "Declined by Insurance Company"
                ]
            ].to_excel(writer,sheet_name=name)          
    writer.save()


    non_converted = final_data[
        (final_data["Conversion"] == 0) & 
        (final_data["Requests"] == 1) & 
        (final_data["Feedback"] != "Declined by Insurance Company")
    ]

    with pd.ExcelWriter(f"{path}insurance_conversion/noncoverted.xlsx", engine='xlsxwriter') as writer:  
        for group, dataframe in non_converted.groupby("Outlet"):
            name = f'{group}'
            dataframe[
                [
                    "Order Number", 
                    "Order Creator", 
                    "Order Create Date", 
                    "Status", 
                    "Customer Code", 
                    "Insurance Company", 
                    "Scheme Type", 
                    "Feedback"
                ]].to_excel(writer,sheet_name=name, index=False)          
    writer.save()  

    with pd.ExcelWriter(f"{path}insurance_conversion/mng_noncoverted.xlsx", engine='xlsxwriter') as writer:  
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
        ].sort_values(by= "Outlet").to_excel(writer,sheet_name = "Data", index=False)  
        final_data.to_excel(writer, sheet_name="Master", index=False)        
    writer.save()

    with pd.ExcelWriter(f"{path}insurance_conversion/conversion_management.xlsx", engine='xlsxwriter') as writer:
        company_conversion.to_excel(writer, sheet_name="overall")
        branch_final.to_excel(writer, sheet_name = "all_branches", index = "Outlet")
    writer.close()





