import pandas as pd
import numpy as np
from airflow.models import variable
from sub_tasks.libraries.utils import fourth_week_end, fourth_week_start
from sub_tasks.libraries.styles import ug_styles


def rejections_template(
    branch: str,
    rejections_data: pd.DataFrame,
    rejection_branches_summary,
    rej_cols: list,
    rejections_ewc_summary: pd.DataFrame
):
    rejections_report = rejections_data[
        rejections_data["Outlet"] == branch
    ][rej_cols]
    rejections_style = rejections_report.style.hide_index().set_table_styles(ug_styles)
    rejections_html = rejections_style.to_html(
        doctype_html=True
    )

    rejection_branch_summary = rejection_branches_summary[
        rejection_branches_summary["Outlet"] == branch
    ]
    rejections_branch_summary_style = rejection_branch_summary.style.hide_index(
    ).set_table_styles(ug_styles)
    rejections_branch_summary_html = rejections_branch_summary_style.to_html(
        doctype_html=True
    )

    rejection_ewc_summary = rejections_ewc_summary[
        rejections_ewc_summary["Outlet"] == branch
    ]
    rejections_ewc_summary_style = rejection_ewc_summary.style.hide_index(
    ).set_table_styles(ug_styles)
    rejections_ewc_summary_html = rejections_ewc_summary_style.to_html(
        doctype_html=True
    )

    return rejections_html, rejections_branch_summary_html, rejections_ewc_summary_html


def planos_template(
    branch: str,
    req_columns: list,
    planos_data: pd.DataFrame,
    plano_branches_summary: pd.DataFrame,
    plano_ewc_summary: pd.DataFrame,
):
    planos_report = planos_data[
        planos_data["Branch"] == branch
    ][req_columns]
    planos_style = planos_report.style.hide_index().set_table_styles(ug_styles)
    planos_html = planos_style.to_html(doctype_html=True)

    plano_branch_summary = plano_branches_summary[
        plano_branches_summary["Branch"] == branch
    ]
    plano_branch_summary_style = plano_branch_summary.style.hide_index(
    ).set_table_styles(ug_styles)
    plano_branch_summary_html = plano_branch_summary_style.to_html(
        doctype_html=True
    )

    planos_ewcs_summary = plano_ewc_summary[
        plano_ewc_summary["Branch"] == branch
    ]
    plano_ewc_summary_style = planos_ewcs_summary.style.hide_index().set_table_styles(ug_styles)
    plano_ewc_summary_html = plano_ewc_summary_style.to_html(
        doctype_html=True
    )

    return planos_html, plano_branch_summary_html, plano_ewc_summary_html


def feedback_template(
    branch: str,
    feedbacks_data: pd.DataFrame,
    feedbacks_summary: pd.DataFrame,
):
    feedbacks_report = feedbacks_summary[feedbacks_summary["Outlet"] == branch]
    feedback_data = feedbacks_data[feedbacks_data["Outlet"] == branch]

    feebacks_style = feedbacks_report.style.hide_index().set_table_styles(ug_styles)
    feedback_data_style =  feedback_data.style.hide_index().set_table_styles(ug_styles)

    feedback_html = feebacks_style.to_html(doctype_html = True)
    feedback_data_html = feedback_data_style.to_html(doctype_html = True)

    return feedback_html, feedback_data_html


