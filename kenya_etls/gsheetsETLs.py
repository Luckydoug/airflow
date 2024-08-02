import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from sub_tasks.gsheets.branch_user_mapping import (
    fetch_branch_user_mappings,
    create_dim_branch_user_mapping,
)
from sub_tasks.gsheets.branches import fetch_branch_tiers, create_dim_branches
from sub_tasks.gsheets.holiday import (
    fetch_holidays,
    fetch_uganda_holidays,
    fetch_rwanda_holidays,
)

from sub_tasks.gsheets.exempt_users import fetch_exempt_users
from sub_tasks.gsheets.draft_drop import fetch_draft_drop
from sub_tasks.gsheets.branch_data import (
    fetch_kenya_branch_data,
    fetch_uganda_branch_data,
    fetch_rwanda_branch_data,
    fetch_hrms_branch_data,
)
from sub_tasks.gsheets.opening_time import (
    fetch_kenya_opening_time,
    fetch_uganda_opening_time,
    fetch_rwanda_opening_time,
)
from sub_tasks.gsheets.working_hours import (
    fetch_kenya_working_hours,
    fetch_uganda_working_hours,
    fetch_rwanda_working_hours,
)
from sub_tasks.gsheets.daily_followups import (
    high_rx_nonconversion,
    long_queue_times,
    insurance_not_submitted,
    insurance_errors,
    upload_snt_preauth,
)

from sub_tasks.gsheets.internal_audit import (
    fetch_optom_queue_data,
    fetch_eyetests_on_hold,
    fetch_frame_only_sales,
    fetch_registrations_data,
    fetch_highrxnonconv,
    fetch_ophreferrals,
)

# from tmp.python_test
DAG_ID = "Gsheet_ETL_Pipeline"

default_args = {
    "owner": "Data Team",
    # 'depends_on_past': False,
    "retries": 3,
    "retry_delay": timedelta(seconds=1),
    "start_date": datetime(2021, 12, 13),
    "email": [
        "wairimu@optica.africa",
        "michael.ongoro@optica.africa"
    ],
    "email_on_failure": True,
    "email_on_retry": False,
}


with DAG(
    DAG_ID,
    default_args=default_args,
    tags=["Live"],
    schedule_interval="30 16 * * *",
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")

    """
    HOLIDAYS
    """
    with TaskGroup("holidays") as holidays:

        fetch_holidays = PythonOperator(
            task_id="fetch_holidays",
            python_callable=fetch_holidays,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_uganda_holidays = PythonOperator(
            task_id="fetch_uganda_holidays",
            python_callable=fetch_uganda_holidays,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_rwanda_holidays = PythonOperator(
            task_id="fetch_rwanda_holidays",
            python_callable=fetch_rwanda_holidays,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_holidays >> fetch_uganda_holidays >> fetch_rwanda_holidays

    """
    EXEMPT USERS
    """
    with TaskGroup("exempt_users") as exempt_users:

        fetch_exempt_users = PythonOperator(
            task_id="fetch_exempt_users",
            python_callable=fetch_exempt_users,
            provide_context=True,
            trigger_rule="all_done",
        )

    """
    DRAFT TO UPLOAD
    """

    with TaskGroup("draft_drop") as draft_drop:

        fetch_draft_drop = PythonOperator(
            task_id="fetch_draft_drop",
            python_callable=fetch_draft_drop,
            provide_context=True,
            trigger_rule="all_done",
        )

    """
    BRANCHES
    """
    with TaskGroup("branches") as branches:

        fetch_branch_user_mappings = PythonOperator(
            task_id="fetch_branch_user_mappings",
            python_callable=fetch_branch_user_mappings,
            provide_context=True,
            trigger_rule="all_done",
        )

        create_dim_branch_user_mapping = PythonOperator(
            task_id="create_dim_branch_user_mapping",
            python_callable=create_dim_branch_user_mapping,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_branch_tiers = PythonOperator(
            task_id="fetch_branch_tiers",
            python_callable=fetch_branch_tiers,
            provide_context=True,
            trigger_rule="all_done",
        )

        create_dim_branches = PythonOperator(
            task_id="create_dim_branches",
            python_callable=create_dim_branches,
            provide_context=True,
            trigger_rule="all_done",
        )

        (
            fetch_branch_user_mappings
            >> create_dim_branch_user_mapping
            >> fetch_branch_tiers
            >> create_dim_branches
        )

    with TaskGroup("branch_data") as branch_data:
        fetch_kenya_branch_data = PythonOperator(
            task_id="fetch_kenya_branch_data",
            python_callable=fetch_kenya_branch_data,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_uganda_branch_data = PythonOperator(
            task_id="fetch_uganda_branch_data",
            python_callable=fetch_uganda_branch_data,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_rwanda_branch_data = PythonOperator(
            task_id="fetch_rwanda_branch_data",
            python_callable=fetch_rwanda_branch_data,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_hrms_branch_data = PythonOperator(
            task_id="fetch_hrms_branch_data",
            python_callable=fetch_hrms_branch_data,
            provide_context=True,
            trigger_rule="all_done",
        )

        (
            fetch_kenya_branch_data
            >> fetch_uganda_branch_data
            >> fetch_rwanda_branch_data
            >> fetch_hrms_branch_data
        )

    with TaskGroup("opening_time") as opening_time:
        fetch_kenya_opening_time = PythonOperator(
            task_id="fetch_kenya_opening_time",
            python_callable=fetch_kenya_opening_time,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_uganda_opening_time = PythonOperator(
            task_id="fetch_uganda_opening_time",
            python_callable=fetch_uganda_opening_time,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_rwanda_opening_time = PythonOperator(
            task_id="fetch_rwanda_opening_time",
            python_callable=fetch_rwanda_opening_time,
            provide_context=True,
            trigger_rule="all_done",
        )

        (
            fetch_kenya_opening_time
            >> fetch_uganda_opening_time
            >> fetch_rwanda_opening_time
        )

    with TaskGroup("working_hours") as working_hours:
        fetch_kenya_working_hours = PythonOperator(
            task_id="fetch_kenya_working_hours",
            python_callable=fetch_kenya_working_hours,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_uganda_working_hours = PythonOperator(
            task_id="fetch_uganda_working_hours",
            python_callable=fetch_uganda_working_hours,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_rwanda_working_hours = PythonOperator(
            task_id="fetch_rwanda_working_hours",
            python_callable=fetch_rwanda_working_hours,
            provide_context=True,
            trigger_rule="all_done",
        )

        (
            fetch_kenya_working_hours
            >> fetch_uganda_working_hours
            >> fetch_rwanda_working_hours
        )

    with TaskGroup("follow_ups") as follow_ups:

        high_rx_nonconversion = PythonOperator(
            task_id="high_rx_nonconversion",
            python_callable=high_rx_nonconversion,
            provide_context=True,
            trigger_rule="all_done",
        )

        long_queue_times = PythonOperator(
            task_id="long_queue_times",
            python_callable=long_queue_times,
            provide_context=True,
            trigger_rule="all_done",
        )

        insurance_not_submitted = PythonOperator(
            task_id="insurance_not_submitted",
            python_callable=insurance_not_submitted,
            provide_context=True,
            trigger_rule="all_done",
        )

        insurance_errors = PythonOperator(
            task_id="insurance_errors",
            python_callable=insurance_errors,
            provide_context=True,
            trigger_rule="all_done",
        )

        upload_snt_preauth = PythonOperator(
            task_id="upload_snt_preauth",
            python_callable=upload_snt_preauth,
            provide_context=True,
            trigger_rule="all_done",
        )

        (
            high_rx_nonconversion
            >> long_queue_times
            >> insurance_not_submitted
            >> insurance_errors
            >> upload_snt_preauth
        )

    with TaskGroup("internal_audit") as internal_audit:

        fetch_optom_queue_data = PythonOperator(
            task_id="fetch_optom_queue_data",
            python_callable=fetch_optom_queue_data,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_eyetests_on_hold = PythonOperator(
            task_id="fetch_eyetests_on_hold",
            python_callable=fetch_eyetests_on_hold,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_frame_only_sales = PythonOperator(
            task_id="fetch_frame_only_sales",
            python_callable=fetch_frame_only_sales,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_registrations_data = PythonOperator(
            task_id="fetch_registrations_data",
            python_callable=fetch_registrations_data,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_highrxnonconv = PythonOperator(
            task_id="fetch_highrxnonconv",
            python_callable=fetch_highrxnonconv,
            provide_context=True,
            trigger_rule="all_done",
        )

        fetch_ophreferrals = PythonOperator(
            task_id="fetch_ophreferrals",
            python_callable=fetch_ophreferrals,
            provide_context=True,
            trigger_rule="all_done",
        )

        (
            fetch_optom_queue_data
            >> fetch_eyetests_on_hold
            >> fetch_frame_only_sales
            >> fetch_registrations_data
            >> fetch_highrxnonconv
            >> fetch_ophreferrals
        )

    finish = DummyOperator(task_id="finish")

    (
        start
        >> exempt_users
        >> holidays
        >> draft_drop
        >> branches
        >> branch_data
        >> opening_time
        >> working_hours
        >> follow_ups
        >> internal_audit
        >> finish
    )
