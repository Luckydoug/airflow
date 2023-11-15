from uganda_sub_tasks.postgres.incentives import (
    refresh_lens_silh,
    refresh_insurance_rejections,
    refresh_insurance_feedback_conversion,
    refresh_sop, refresh_nps_summary,
    refresh_google_reviews_summary,
    refresh_sunglass_sales_summary
)
from uganda_sub_tasks.postgres.prescriptions_views import refresh_et_conv
from uganda_sub_tasks.postgres.salesorders_views import (
    refresh_order_line_with_details,
    refresh_salesorders_line_cl_and_rr,
    refresh_fact_orders_header,
    refresh_order_contents
)
from airflow.sensors.time_sensor import TimeSensor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.example_dags.subdags.subdag import subdag
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow import DAG
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))


DAG_ID = 'UG_Update_Views'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
    'start_date': datetime(2021, 12, 13),
    'email': ['ian.gathumbi@optica.africa', 'wairimu@optica.africa', 'douglas.kathurima@optica.africa'],
    'email_on_failure': True,
    'email_on_retry': False,
}


with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval='30 19 * * *',
    catchup=False
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    with TaskGroup('salesorders') as salesorders:

        refresh_order_line_with_details = PythonOperator(
            task_id='refresh_order_line_with_details',
            python_callable=refresh_order_line_with_details,
            provide_context=True
        )

        refresh_salesorders_line_cl_and_rr = PythonOperator(
            task_id='refresh_salesorders_line_cl_and_rr',
            python_callable=refresh_salesorders_line_cl_and_rr,
            provide_context=True
        )

        refresh_fact_orders_header = PythonOperator(
            task_id='refresh_fact_orders_header',
            python_callable=refresh_fact_orders_header,
            provide_context=True
        )

        refresh_order_contents = PythonOperator(
            task_id='refresh_order_contents',
            python_callable=refresh_order_contents,
            provide_context=True
        )

        refresh_order_line_with_details >> refresh_salesorders_line_cl_and_rr >> refresh_fact_orders_header >> refresh_order_contents

    with TaskGroup('prescriptions') as prescriptions:

        refresh_et_conv = PythonOperator(
            task_id='refresh_et_conv',
            python_callable=refresh_et_conv,
            provide_context=True
        )

    with TaskGroup('incentive_factors') as incentive_factors:

        refresh_lens_silh = PythonOperator(
            task_id='refresh_lens_silh',
            python_callable=refresh_lens_silh,
            provide_context=True
        )

        refresh_insurance_rejections = PythonOperator(
            task_id='refresh_insurance_rejections',
            python_callable=refresh_insurance_rejections,
            provide_context=True
        )

        refresh_insurance_feedback_conversion = PythonOperator(
            task_id='refresh_insurance_feedback_conversion',
            python_callable=refresh_insurance_feedback_conversion,
            provide_context=True
        )

        refresh_sop = PythonOperator(
            task_id='refresh_sop',
            python_callable=refresh_sop,
            provide_context=True
        )

        refresh_nps_summary = PythonOperator(
            task_id='refresh_nps_summary',
            python_callable=refresh_nps_summary,
            provide_context=True
        )

        refresh_google_reviews_summary = PythonOperator(
            task_id='refresh_google_reviews_summary',
            python_callable=refresh_google_reviews_summary,
            provide_context=True
        )

        refresh_sunglass_sales_summary = PythonOperator(
            task_id='refresh_sunglass_sales_summary',
            python_callable=refresh_sunglass_sales_summary,
            provide_context=True
        )

        refresh_lens_silh >> refresh_insurance_rejections >> refresh_insurance_feedback_conversion >> refresh_sop >> refresh_nps_summary >> refresh_google_reviews_summary >> refresh_sunglass_sales_summary

    finish = DummyOperator(
        task_id="finish"
    )

    start >> salesorders >> prescriptions >> incentive_factors >> finish
