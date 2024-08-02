from airflow.models import variable
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

DAG_ID = "KENYA_SRM_RM_ETL"


default_args = {
    "owner": "Data",
    "retries": 3,
    "retry_delay": timedelta(seconds=15),
    "start_date": datetime(2021, 12, 13),
    "email": [
        "ian.gathumbi@optica.africa",
        "wairimu@optica.africa",
        "douglas.kathurima@optica.africa",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
}


with DAG(
    DAG_ID,
    default_args=default_args,
    tags=["Live"],
    schedule_interval="30 03 * * *",
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")

    with TaskGroup("report") as report:
        with TaskGroup("build") as build:
            from kenya_automation.draft_upload.kenya_report import (
                push_kenya_efficiency_data,
                build_kenya_draft_upload,
                build_kenya_rejections,
                build_kenya_sops,
                build_kenya_plano_report,
                build_kenya_ratings_report,
                build_kenya_opening_time,
                build_kenya_insurance_conversion,
                build_kenya_non_view_non_conversions,
                build_mtd_insurance_conversion,
                build_eyetest_order,
                build_efficiency_before_feedback,
                build_direct_conversion,
                build_approval_update_efficieny
            )

            push_kenya_efficiency_data = PythonOperator(
                task_id="push_kenya_efficiency_data",
                python_callable=push_kenya_efficiency_data,
                provide_context=True,
            )

            build_kenya_draft_upload = PythonOperator(
                task_id="build_kenya_draft_upload",
                python_callable=build_kenya_draft_upload,
                provide_context=True,
            )

            build_kenya_rejections = PythonOperator(
                task_id="build_kenya_rejections",
                python_callable=build_kenya_rejections,
                provide_context=True,
            )

            build_kenya_sops = PythonOperator(
                task_id="build_kenya_sops",
                python_callable=build_kenya_sops,
                provide_context=True,
            )

            build_kenya_plano_report = PythonOperator(
                task_id="build_kenya_plano_report",
                python_callable=build_kenya_plano_report,
                provide_context=True,
            )

            build_kenya_ratings_report = PythonOperator(
                task_id="build_kenya_ratings_report",
                python_callable=build_kenya_ratings_report,
                provide_context=True,
            )

            build_kenya_opening_time = PythonOperator(
                task_id="build_kenya_opening_time",
                python_callable=build_kenya_opening_time,
                provide_context=True,
            )

            build_kenya_insurance_conversion = PythonOperator(
                task_id="build_kenya_insurance_conversion",
                python_callable=build_kenya_insurance_conversion,
                provide_context=True,
            )

            build_mtd_insurance_conversion = PythonOperator(
                task_id="build_mtd_insurance_conversion",
                python_callable=build_mtd_insurance_conversion,
                provide_context=True,
            )

            build_kenya_non_view_non_conversions = PythonOperator(
                task_id="build_kenya_non_view_non_conversions",
                python_callable=build_kenya_non_view_non_conversions,
                provide_context=True,
            )

            build_eyetest_order = PythonOperator(
                task_id="build_eyetest_order",
                python_callable=build_eyetest_order,
                provide_context=True,
            )

            build_efficiency_before_feedback = PythonOperator(
                task_id="build_efficiency_before_feedback",
                python_callable=build_efficiency_before_feedback,
                provide_context=True,
            )

            build_approval_update_efficieny = PythonOperator(
                task_id="build_approval_update_efficieny",
                python_callable=build_approval_update_efficieny,
                provide_context=True,
            )

            build_direct_conversion = PythonOperator(
                task_id="build_direct_conversion",
                python_callable=build_direct_conversion,
                provide_context=True,
            )

            (
                push_kenya_efficiency_data
                >> build_kenya_draft_upload
                >> build_kenya_sops
                >> build_kenya_rejections
                >> build_kenya_plano_report
                >> build_kenya_ratings_report
                >> build_kenya_opening_time
                >> build_kenya_insurance_conversion
                >> build_mtd_insurance_conversion
                >> build_kenya_non_view_non_conversions
                >> build_eyetest_order
                >> build_efficiency_before_feedback
                >> build_direct_conversion
                >> build_approval_update_efficieny
            )

    with TaskGroup("smtp") as smtp:
        with TaskGroup("send") as sends:
            from kenya_automation.draft_upload.kenya_report import (
                trigger_kenya_smtp,
                clean_kenya_folder,
                trigger_kenya_branches_smtp,
            )

            trigger_kenya_smtp = PythonOperator(
                task_id="trigger_kenya_smtp",
                python_callable=trigger_kenya_smtp,
                provide_context=True,
            )

            clean_kenya_folder = PythonOperator(
                task_id="clean_kenya_folder",
                python_callable=clean_kenya_folder,
                provide_context=True,
                trigger_rule="all_done",
            )

            trigger_kenya_branches_smtp = PythonOperator(
                task_id="trigger_kenya_branches_smtp",
                python_callable=trigger_kenya_branches_smtp,
                provide_context=True,
            )

            trigger_kenya_smtp >> trigger_kenya_branches_smtp >> clean_kenya_folder

        build >> sends

    finish = DummyOperator(task_id="finish")

    start >> report >> smtp >> finish

    """
    From Optica Data Team
    Unleash the Power of Automation
    
    """
