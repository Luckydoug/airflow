import pandas as pd
import numpy as np
from airflow.models import variable
from sub_tasks.libraries.utils import fourth_week_end, fourth_week_start
from sub_tasks.libraries.styles import ug_styles


def rejections_template(
    branch: str,
    rejections_data: pd.DataFrame,
    rej_cols: list
):
    rejections_report = rejections_data[
        rejections_data["Outlet"] == branch
    ][rej_cols]
    rejections_style = rejections_report.style.hide_index().set_table_styles(ug_styles)
    rejections_html = rejections_style.to_html(
        doctype_html=True
    )
    return rejections_html


def planos_template(
    branch: str,
    req_columns: list,
    planos_data: pd.DataFrame
):
    planos_report = planos_data[
        planos_data["Branch"] == branch
    ][req_columns]
    planos_style = planos_report.style.hide_index().set_table_styles(ug_styles)
    planos_html = planos_style.to_html(doctype_html=True)


    return planos_html


def feedback_template(
    branch: str,
    feedbacks_data: pd.DataFrame
):
    feedback_data = feedbacks_data[feedbacks_data["Outlet"] == branch].drop(columns=["Outlet"])
    feedback_data_style =  feedback_data.style.hide_index().set_table_styles(ug_styles)
    feedback_data_html = feedback_data_style.to_html(doctype_html = True)

    return feedback_data_html


def no_feedback_template(
    branch: str,
    data: str
):
    report = data[data["Outlet"] == branch].drop(columns=["Outlet"])
    report_style = report.style.hide_index().set_table_styles(ug_styles)
    report_html = report_style.to_html(doctype_html = True)

    return report_html


