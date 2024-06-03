import pandas as pd
from airflow.models import variable
from reports.bi_weekly.utils.utils import return_week_ranges
from reports.bi_weekly.utils.utils import add_metrics_missed
import matplotlib.pyplot as plt


columns = {
    "Efficiency (Target = 45 Mins)": "Eye Test to Order Efficiency (Target = 90% in 45 minutes)",
    "ApprovalReceivedInsuranceUpdateSAP": "Approval Received from Insurance to Update Approval on SAP (Target = 90% in 5 Minutes)",
    "EWC Overall Conversion": "EWC Overall Conversion (Target = 75)",
    "Optom Overall Conversion": "Optom Overall Conversion (Target = 75)",
    "InsuranceFeedbackToCustomerTime": "Insurance Feedback to Customer Contacted time taken (Target = 90% in 60 Minutes)",
    "CorrectedFormsResentEfficiency": "Corrected Forms Resent (Target = 90% in 5 Minutes)",
}


def create_biweekly_report(data, path) -> None:
    with pd.ExcelWriter(f"{path}bi_weekly_report/report.xlsx") as writer:
        data = data.rename(columns=columns)
        data = add_metrics_missed(data)
        data.to_excel(writer, index=False)


def create_biweekly_comparison(data1, data2, path) -> None:
    range_one, range_two = return_week_ranges()
    data1 = data1.rename(columns=columns)
    data2 = data2.rename(columns=columns)
    data1 = add_metrics_missed(data1)
    data2 = add_metrics_missed(data2)

    data1 = data1.set_index(
        ["Branch", "SRM", "RM", "Staff Name", "Designation", "Payroll Number"]
    )

    data2 = data2.set_index(
        ["Branch", "SRM", "RM", "Staff Name", "Designation", "Payroll Number"]
    )

    data1.columns = pd.MultiIndex.from_product([[range_one], data1.columns])

    data2.columns = pd.MultiIndex.from_product([[range_two], data2.columns])

    comparison = pd.merge(data1, data2, right_index=True, left_index=True, how="outer")

    columns_set = set(comparison.columns.get_level_values(1))
    comparison = (
        comparison.reindex(columns_set, axis=1, level=1)
        .swaplevel(0, 1, 1)
        .sort_index(axis=1, level=0)
        .reindex([range_one, range_two], level=1, axis=1)
    )

    with pd.ExcelWriter(f"{path}bi_weekly_report/comparison_report.xlsx") as writer:
        comparison.to_excel(writer)


def generate_biweekly_data(
    path: str,
    opening_time: pd.DataFrame,
    et_nt_converted: pd.DataFrame,
    et_order_delays: pd.DataFrame,
    identifier_delays: pd.DataFrame,
    passives: pd.DataFrame,
    poor_reviews: pd.DataFrame,
    sops_not_complied: pd.DataFrame,
    frame_only_orders: pd.DataFrame,
    insurance_non_conversions: pd.DataFrame,
) -> pd.DataFrame:

    def write_to_excel(dataframe, writer, sheet_name):
        dataframe.to_excel(writer, sheet_name=sheet_name, index=False)

    with pd.ExcelWriter(f"{path}bi_weekly_report/raw_data.xlsx") as writer:
        write_to_excel(opening_time, writer, "Branch Opening Late")
        write_to_excel(et_nt_converted, writer, "Eye Tests not Converted")
        write_to_excel(et_order_delays, writer, "Eye Test to Order Delays")
        write_to_excel(identifier_delays, writer, "Printing Identifier Delays")
        write_to_excel(passives, writer, "Passive Comments")
        write_to_excel(poor_reviews, writer, "Poor Google Reviews")
        write_to_excel(sops_not_complied, writer, "SOP not Complied")
        write_to_excel(frame_only_orders, writer, "Frame only Orders")
        write_to_excel(insurance_non_conversions, writer, "Insurance Feedbacks")


def create_month_trend(path, data):
    with pd.ExcelWriter(f"{path}bi_weekly_report/conversion_trend.xlsx") as writer:
        data.to_excel(writer, index=False)
