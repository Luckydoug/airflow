from airflow.models import variable
import pandas as pd
import numpy as np
from reports.insurance_conversion.utils.utils import(
    check_conversion,
    calculate_conversion,
    check_number_requests,
    get_rm_srm_total,
    create_monthly_report,
    create_branch_report,
    seek_feedback
)
from reports.draft_to_upload.utils.utils import today, get_start_end_dates
from sub_tasks.libraries.time_diff import calculate_time_difference
from sub_tasks.libraries.time_diff import working_hours_dictionary
from sub_tasks.libraries.utils import (
    fourth_week_start,
    fourth_week_end,
    check_date_range,
    fetch_gsheet_data,
    get_comparison_months
)

def create_insurance_conversion(
    all_orders: pd.DataFrame,
    orderscreen: pd.DataFrame,
    insurance_companies: pd.DataFrame,
    sales_orders: pd.DataFrame,
    path: str,
    branch_data: pd.DataFrame,
    selection: str,
    date,
    working_hours: pd.DataFrame,
    no_feedbacks: pd.DataFrame,
    country: str,
    holidays: dict,
    contact_time: pd.DataFrame = pd.DataFrame()
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
    

    select = ''
    if selection == "Weekly":
        select = fourth_week_start
    else:
        select = date
    
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
   
    insurance_tat = fetch_gsheet_data()["insurance_tat"]
    daily_data = comparison_data[
        (comparison_data["Full Feedback Date"].dt.date >= pd.to_datetime(select, format="%Y-%m-%d").date()) &
        (comparison_data["Full Feedback Date"].dt.date <= pd.to_datetime(today, format="%Y-%m-%d").date())
    ].copy()

    feedback_non = daily_data[
        (daily_data["Requests"] == 1) & 
        (daily_data["Conversion"] == 0) & 
        (daily_data["Feedback"] != "Declined by Insurance Company")
    ]

    work_hours = working_hours_dictionary(
        working_hours=working_hours
    )

    if len(feedback_non):
        feedback_non["Time Taken"] = feedback_non.apply(
            lambda row: calculate_time_difference(
                row=row,
                x="Full Request Date",
                y="Feedback Time",
                working_hours=work_hours,
                holiday_dict=holidays
            ),
            axis=1
        )

        feedback_non = pd.merge(
            feedback_non,
            insurance_tat,
            on = "Insurance Company",
            how = "left"
        ).rename(columns = {"Turnaround Time": "TAT"})

        def convert_to_int_or_dash(value):
            if value == "-":
                return value
            else:
                return str(int(value))

        feedback_non["TAT"] = feedback_non["TAT"].fillna("-").apply(convert_to_int_or_dash)


        cols = [
            "Outlet",
            "Order Number",
            "Current Status",
            "Order Creator",
            "Insurance Company",
            "TAT",
            "% Approved",
            "Full Request Date",
            "Feedback Time",
            "Time Taken"
        ]


        creator_summary = pd.pivot_table(
            feedback_non,
            index=["Outlet","Order Creator"],
            columns="Feedback",
            values="Order Number",
            aggfunc="count"
        ).reset_index().fillna("-")

        feedback_non["% Approved"] = feedback_non["% Approved"].astype(int).astype(str) + "%"
        feedback_non["Feedback"] = pd.Categorical(
            feedback_non["Feedback"],
            categories=[
                "Insurance Fully Approved", 
                "Use Available Amount on SMART", 
                "Insurance Partially Approved"
            ],
            ordered=True
        )

        feedback_non = feedback_non.sort_values(by = ["Feedback", "Order Creator"], ascending=[True, False])
        feedback_non["Time Taken"] = feedback_non["Time Taken"].astype(int)
        not_received = no_feedbacks[
            no_feedbacks["Feedback"] == "no feedback"
        ]


        feedback_non["Full Request Date"] = pd.to_datetime(feedback_non["Full Request Date"]).dt.strftime('%Y-%m-%d %H:%M')
        feedback_non["Feedback Time"] = pd.to_datetime(feedback_non["Feedback Time"]).dt.strftime('%Y-%m-%d %H:%M')


        with pd.ExcelWriter(f"{path}draft_upload/insurance_daily.xlsx", engine='xlsxwriter') as writer:
            creator_summary.to_excel(writer, sheet_name="daily_summary", index=False)
            feedback_non = feedback_non[cols]
            print(contact_time)
            feedback_non = pd.merge(feedback_non, contact_time, on ="Order Number", how = "left")
            feedback_non = feedback_non.drop_duplicates(subset="Order Number")
            feedback_non.to_excel(writer, sheet_name = "daily_data", index = False)

        not_received["Request Date"] = pd.to_datetime(not_received["Request Date"]).dt.strftime('%Y-%m-%d %H:%M')
        with pd.ExcelWriter(f"{path}draft_upload/no_feedbacks.xlsx", engine='xlsxwriter') as writer:
            not_received[[
                "Outlet",
                "Order Number",
                "Order Creator",
                "Insurance Company",
                "Request Date"
            ]].to_excel(writer, sheet_name="no_feedback", index=False)
        writer.close()

    if selection == "Weekly":
        final_data = final[
            (final["Full Feedback Date"].dt.date >= pd.to_datetime(fourth_week_start, format="%Y-%m-%d").date()) & 
            (final["Full Feedback Date"].dt.date <=  pd.to_datetime(fourth_week_end, format="%Y-%m-%d").date())
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

        final_branch = insurance_feedback_pivot[
            [
                "Outlet", 
                "Feedback", 
                "Requests", 
                "Converted"
            ]
        ]

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
        stacked["Conversion"] = round(stacked["Converted"] / stacked["Requests"] * 100, 0).fillna("-").astype(str) + "%"
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
        ).rename(columns = {"Requests": "Feedbacks"}, level = 1)

        columns_todo = [
            ("Insurance Fully Approved", "Feedbacks"),
            ("Insurance Fully Approved", "Converted"),
            ("Insurance Fully Approved", "%Conversion"),
            ("Insurance Partially Approved", "Feedbacks"),
            ("Insurance Partially Approved", "Converted"),
            ("Insurance Partially Approved", "%Conversion"),
            ("Use Available Amount on SMART", "Feedbacks"),
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


        comparison_data["Week Range"] = comparison_data.apply(
            lambda row: check_date_range(row, "Full Feedback Date"), 
            axis = 1
        )
        complete = comparison_data[comparison_data["Week Range"] != "None"].copy()

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
        company_conversion = company_conversion.reindex(["Requests", "%Conversion"], level = 1, axis = 1).rename(columns = {"Requests": "Feedbacks"})

        pstart, pend = get_start_end_dates(selection=selection)

        mtd_data = comparison_data[
            (comparison_data["Full Feedback Date"].dt.date >= pd.to_datetime(pstart).date()) &
            (comparison_data["Full Feedback Date"].dt.date <= pd.to_datetime(pend).date())
        ]

        mtd_pivot = create_branch_report(
            mtd_data
        )

        """
        Request Vs Feedbacks Section
        """

        no_feedbacks["Date Range"] = no_feedbacks.apply(lambda row: check_date_range(row, "Request Date"), axis = 1)
        no_feedbacks = no_feedbacks[no_feedbacks["Date Range"] != "None"]
        no_feedbacks["Country"] = country

        no_requests_pivot = pd.pivot_table(
            no_feedbacks,
            index="Country",
            columns="Date Range",
            values="Feedback",
            aggfunc={"Feedback":[
                'count',
                lambda x: (x == "feedback").sum(),
                lambda x: (x == "no feedback").sum()]
                }
            )
        
        no_requests_pivot = no_requests_pivot.swaplevel(0, 1, 1).reindex(sorted_columns, level = 0, axis = 1)
        no_requests_pivot = no_requests_pivot.reindex(["count", "<lambda_1>"], axis = 1, level = 1)
        no_requests_pivot = no_requests_pivot.rename(
            columns = {
                "count": "Requests", 
                "<lambda_1>": "No Feedback"
            }, 
            level = 1
        )

        last_week =  no_feedbacks[no_feedbacks["Date Range"] == sorted_columns[-1]]
        last_week_no_feedback = last_week[last_week["Feedback"] == 'no feedback']

        # no_feedback_pivot = pd.pivot_table(
        #     last_week_no_feedback,
        #     index="Insurance Company",
        #     values="Order Number",
        #     aggfunc="count"
        # ).reset_index().rename(columns =  {"Order Number": "Count of No Feedbacks"}).sort_values(by = "Count of No Feedbacks", ascending = False)

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
                        "Feedback",
                        "% Approved"
                    ]].to_excel(writer,sheet_name=name, index=False)          
        writer.save()  

        with pd.ExcelWriter(f"{path}insurance_conversion/mng_noncoverted.xlsx", engine='xlsxwriter') as writer: 
            mtd_pivot.to_excel(writer, sheet_name = "MTD Summary") 

        with pd.ExcelWriter(f"{path}insurance_conversion/mng_noncoverted.xlsx", engine='xlsxwriter') as writer: 
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
            comparison_data.to_excel(writer, sheet_name = "Master Data", index = False)
            no_feedbacks.to_excel(writer, sheet_name = "All Requests", index = False)
            # no_feedback_pivot.to_excel(writer, sheet_name = "Insurance Company", index = False)
             
        writer.save()

        with pd.ExcelWriter(f"{path}insurance_conversion/conversion_management.xlsx", engine='xlsxwriter') as writer:
            company_conversion.to_excel(
                writer, 
                sheet_name="overall"
            )
            branch_final.to_excel(
                writer, 
                sheet_name = "all_branches", 
                index = "Outlet"
            )

            no_requests_pivot.to_excel(
                writer,
                sheet_name = "no feedback"
            )

        writer.close()

    elif selection == "Monthly":
        first_month, second_month = get_comparison_months()
        comparison_data["Month"] = comparison_data["Full Feedback Date"].dt.month_name()
        

        monthly_data = comparison_data[
            (comparison_data["Month"] == first_month) |
            (comparison_data["Month"] == second_month)
        ]

        monthly_data["Feedback"] = pd.Categorical(
            monthly_data["Feedback"],
            categories=[
                "Insurance Fully Approved",
                "Insurance Partially Approved", 
                "Use Available Amount on SMART", 
                "Declined by Insurance Company"
            ], 
            ordered=True
        )

        summary_report = create_monthly_report(
            data=monthly_data,
            index="Feedback"
        )

        recent_month_data = comparison_data[
            comparison_data["Month"] == second_month
        ]

        branches_report = create_branch_report(
            recent_month_data
        )
        
        non_converted =   recent_month_data[
            (recent_month_data["Conversion"] == 0) & 
            (recent_month_data["Requests"] == 1) & 
            (recent_month_data["Feedback"] != "Declined by Insurance Company") &
            (recent_month_data["Status"] != "Collected") &
            (recent_month_data["Status"] != "Sent Forms to Invoice Desk")
        ]

        company_no_feedback = no_feedbacks[
            (no_feedbacks["Month"] == second_month) | 
            (no_feedbacks["Month"] == first_month)
        ]

        company_no_feedback["Country"] = country

        no_requests_pivot = pd.pivot_table(
            company_no_feedback,
            index="Country",
            columns="Month",
            values="Feedback",
            aggfunc={"Feedback":[
                'count',
                lambda x: (x == "feedback").sum(),
                lambda x: (x == "no feedback").sum()]
                }
            )
        
        no_requests_pivot = no_requests_pivot.swaplevel(0, 1, 1).reindex([first_month, second_month], level = 0, axis = 1)
        no_requests_pivot = no_requests_pivot.reindex(["count", "<lambda_1>"], axis = 1, level = 1)
        no_requests_pivot = no_requests_pivot.rename(
            columns = {
                "count": "Requests", 
                "<lambda_1>": "No Feedback"
            }, 
            level = 1
        )

        with pd.ExcelWriter(f"{path}insurance_conversion/conversion_management.xlsx", engine='xlsxwriter') as writer:
            non_converted.to_excel(writer, sheet_name = "NON Conversions", index = False)
            comparison_data.to_excel(writer, sheet_name = "Master Data", index = False)
            summary_report.to_excel(writer, sheet_name="overall")
            branches_report.to_excel(writer, sheet_name = "all_branches")
            company_no_feedback.to_excel(writer, sheet_name = "all_feedbacks", index=False)
            no_requests_pivot.to_excel(
                writer,
                sheet_name = "no feedback"
            )



         






