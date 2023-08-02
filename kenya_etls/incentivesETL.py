import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

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


from sub_tasks.paymentsETLs.payments import (fetch_sap_payments)
from sub_tasks.paymentsETLs.ojdt import (fetch_sap_ojdt)
from sub_tasks.ordersETLs.ordersscreendetails import (fetch_sap_orderscreendetails, update_to_source_orderscreen)
from sub_tasks.ordersETLs.salesorders import (fetch_sap_orders)
from sub_tasks.ordersETLs.discounts import (fetch_sap_discounts)
from sub_tasks.paymentsETLs.incentives import (create_incentive_cash,create_incentive_insurance)
from sub_tasks.pentaho.cache import (clear_cache)

DAG_ID = 'Incentives_Pipeline'

default_args = {
    'owner': 'Iconia ETLs',
    # 'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
    'start_date': datetime(2021, 12, 13),
    # 'email': ['ian.gathumbi@optica.africa','wairimu@optica.africa'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
}


# default_args = {
#     'owner': 'Iconia ETLs',
#     'start_date': datetime(2021, 12, 13)
# }

with DAG(
    DAG_ID, 
    default_args=default_args,
    schedule_interval='45 5,7,9,11,13,14,15,16 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    fetch_sap_payments = PythonOperator(
        task_id = 'fetch_sap_payments',
        python_callable = fetch_sap_payments,
        provide_context = True
    )

    fetch_sap_ojdt = PythonOperator(
        task_id = 'fetch_sap_ojdt',
        python_callable = fetch_sap_ojdt,
        provide_context = True
    )

    with TaskGroup('orderscreen_details') as orderscreen_details:

        fetch_sap_orderscreendetails = PythonOperator(
            task_id = 'fetch_sap_orderscreendetails',
            python_callable = fetch_sap_orderscreendetails,
            provide_context=True
        )

        update_to_source_orderscreen = PythonOperator(
            task_id = 'update_to_source_orderscreen',
            python_callable = update_to_source_orderscreen,
            provide_context=True
        )

        fetch_sap_orderscreendetails >> update_to_source_orderscreen

    fetch_sap_discounts = PythonOperator(
        task_id = 'fetch_sap_discounts',
        python_callable = fetch_sap_discounts,
        provide_context = True
    )

    fetch_sap_orders = PythonOperator(
            task_id = 'fetch_sap_orders',
            python_callable = fetch_sap_orders,
            provide_context = True
    )

    with TaskGroup('incentives') as incentives:

        create_incentive_cash = PythonOperator(
            task_id = 'create_incentive_cash',
            python_callable = create_incentive_cash,
            provide_context = True,
        )

        create_incentive_insurance = PythonOperator(
            task_id = 'create_incentive_insurance',
            python_callable = create_incentive_insurance,
            provide_context = True
        )

        create_incentive_cash >> create_incentive_insurance

    clear_cache = PythonOperator(
            task_id = 'clear_cache',
            python_callable = clear_cache,
            provide_context = True
    )

    finish = DummyOperator(
        task_id = "finish"
    )

    start >> fetch_sap_payments >> fetch_sap_ojdt >> orderscreen_details  >> fetch_sap_orders >> fetch_sap_discounts >> incentives >> clear_cache >> finish



