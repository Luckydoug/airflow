import sys, os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator

# from airflow.operators.cron_operator import CronOperator
# from airflow.operators.simple_dag_operator import SimpleDagOperator
# from airflow.sensors.timedelta_sensor import TimeDeltaSensor
from airflow.sensors.time_sensor import TimeSensor


from rwanda_sub_tasks.postgres.salesorders_views import (
    refresh_order_line_with_details,
    refresh_salesorders_line_cl_and_rr,
    refresh_fact_orders_header,
    refresh_order_contents,
    refresh_optom_queue_no_et
)
from rwanda_sub_tasks.postgres.prescriptions_views import refresh_et_conv
from rwanda_sub_tasks.postgres.incentives import (
    refresh_lens_silh,
    refresh_insurance_feedback_conversion,
)
from rwanda_sub_tasks.postgres.insurance_efficiency import (
    update_approvals_efficiency,
    update_insurance_efficiency_after_feedback,
    update_insurance_efficiency_before_feedback,
)

from rwanda_sub_tasks.postgres.printing_identifier import update_printing_identifier

DAG_ID = "RW_Update_Views"

default_args = {
    "owner": "Iconia ETLs",
    # 'depends_on_past': False,
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
    DAG_ID, default_args=default_args, schedule_interval="45 21 * * *", catchup=False
) as dag:

    start = DummyOperator(task_id="start")

    with TaskGroup("salesorders") as salesorders:

        refresh_order_line_with_details = PythonOperator(
            task_id="refresh_order_line_with_details",
            python_callable=refresh_order_line_with_details,
            provide_context=True,
        )

        refresh_salesorders_line_cl_and_rr = PythonOperator(
            task_id="refresh_salesorders_line_cl_and_rr",
            python_callable=refresh_salesorders_line_cl_and_rr,
            provide_context=True,
        )

        refresh_fact_orders_header = PythonOperator(
            task_id="refresh_fact_orders_header",
            python_callable=refresh_fact_orders_header,
            provide_context=True,
        )

        refresh_order_contents = PythonOperator(
            task_id="refresh_order_contents",
            python_callable=refresh_order_contents,
            provide_context=True,
        )

        refresh_insurance_feedback_conversion = PythonOperator(
            task_id="refresh_insurance_feedback_conversion",
            python_callable=refresh_insurance_feedback_conversion,
            provide_context=True,
        )

        refresh_optom_queue_no_et = PythonOperator(
            task_id="refresh_optom_queue_no_et",
            python_callable=refresh_optom_queue_no_et,
            provide_context=True,
        )

        (
            refresh_order_line_with_details
            >> refresh_salesorders_line_cl_and_rr
            >> refresh_fact_orders_header
            >> refresh_order_contents
            >> refresh_insurance_feedback_conversion
            >> refresh_optom_queue_no_et
        )

    with TaskGroup("prescriptions") as prescriptions:

        refresh_et_conv = PythonOperator(
            task_id="refresh_et_conv",
            python_callable=refresh_et_conv,
            provide_context=True,
        )

        refresh_et_conv

    with TaskGroup("lens_silh") as lens_silh:

        refresh_lens_silh = PythonOperator(
            task_id="refresh_lens_silh",
            python_callable=refresh_lens_silh,
            provide_context=True,
        )

        refresh_lens_silh

    with TaskGroup("efficiency") as efficiency:

        update_insurance_efficiency_before_feedback = PythonOperator(
            task_id="update_insurance_efficiency_before_feedback",
            python_callable=update_insurance_efficiency_before_feedback,
            provide_context=True,
        )

       
        update_insurance_efficiency_after_feedback = PythonOperator(
            task_id="update_insurance_efficiency_after_feedback",
            python_callable=update_insurance_efficiency_after_feedback,
            provide_context=True,
        )

        update_approvals_efficiency = PythonOperator(
            task_id="update_approvals_efficiency",
            python_callable=update_approvals_efficiency,
            provide_context=True,
        )

        update_insurance_efficiency_before_feedback >> update_insurance_efficiency_after_feedback >> update_approvals_efficiency

    with TaskGroup("printing_identifier") as printing_identifier:
        update_printing_identifier = PythonOperator(
            task_id="update_printing_identifier",
            python_callable=update_printing_identifier,
            provide_context=True,
        )

        update_printing_identifier

    finish = DummyOperator(task_id="finish")

    start >> salesorders >> prescriptions >> lens_silh >> efficiency >> finish
