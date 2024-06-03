import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.example_dags.subdags.subdag import subdag
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta


# from tmp.python_test
DAG_ID = 'BranchTargets_ETLs_Pipeline'

default_args = {
    'owner': 'Data Team',
    # 'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
    'start_date': datetime(2021, 12, 13),
    'email': ['ian.gathumbi@optica.africa','wairimu@optica.africa','douglas.kathurima@optica.africa'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    DAG_ID, 
    default_args=default_args,
    tags=['Live'], 
    schedule_interval='00 23 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    with TaskGroup('sales_targets') as sales:

        from sub_tasks.targetsETL.branch_targets import (fetch_sap_branch_targets)
        from sub_tasks.targetsETL.incentive_slab import (fetch_sap_incentive_slab)

        fetch_sap_branch_targets = PythonOperator(
            task_id = 'fetch_sap_branch_targets',
            python_callable = fetch_sap_branch_targets,
            provide_context = True
        )

        fetch_sap_incentive_slab = PythonOperator(
            task_id = 'fetch_sap_incentive_slab',
            python_callable = fetch_sap_incentive_slab,
            provide_context = True
        )

        fetch_sap_branch_targets >> fetch_sap_incentive_slab
    
    with TaskGroup('google_reviews') as google_reviews: 

        from sub_tasks.gsheets.gr_targets import (fetch_gr_targets)
        from sub_tasks.incentives.incentive_factors import(refresh_google_reviews_summary)
            
        fetch_gr_targets = PythonOperator(
            task_id = 'fetch_gr_targets',
            python_callable = fetch_gr_targets,
            provide_context = True
        )

        refresh_google_reviews_summary = PythonOperator(
            task_id = 'refresh_google_reviews_summary',
            python_callable = refresh_google_reviews_summary,
            provide_context = True
        )

        fetch_gr_targets >> refresh_google_reviews_summary
    
    with TaskGroup('sunglasses') as sunglasses:

        from sub_tasks.gsheets.sg_sales_targets import (fetch_sg_targets, create_sg_summary)
        from sub_tasks.incentives.incentive_factors import refresh_sunglass_sales_summary
    
        fetch_sg_targets = PythonOperator(
            task_id = 'fetch_sg_targets',
            python_callable = fetch_sg_targets,
            provide_context = True
        ) 

        refresh_sunglass_sales_summary = PythonOperator(
            task_id = 'refresh_sunglass_sales_summary',
            python_callable = refresh_sunglass_sales_summary,
            provide_context = True
        )

        fetch_sg_targets >> refresh_sunglass_sales_summary
    
    with TaskGroup('nps_survey') as nps_survey:

        from sub_tasks.gsheets.ded_earn import fetch_perc_det
        from sub_tasks.incentives.incentive_factors import refresh_nps_summary
                                               
        fetch_perc_det = PythonOperator(
            task_id = 'fetch_perc_det',
            python_callable = fetch_perc_det,
            provide_context = True
        )
 
        refresh_nps_summary = PythonOperator(
            task_id = 'refresh_nps_summary',
            python_callable = refresh_nps_summary,
            provide_context = True
        )

        fetch_perc_det >> refresh_nps_summary

    with TaskGroup('insurance_rejections') as insurance_rejections:

        from sub_tasks.gsheets.ded_earn import fetch_perc_ins_rej
        from sub_tasks.incentives.incentive_factors import refresh_insurance_rejections
        from sub_tasks.gsheets.insurancetodrop import fetch_insurance_errors_to_drop

        fetch_perc_ins_rej = PythonOperator(
            task_id = 'fetch_perc_ins_rej',
            python_callable = fetch_perc_ins_rej,
            provide_context = True
        )

        fetch_insurance_errors_to_drop = PythonOperator(
            task_id = 'fetch_insurance_errors_to_drop',
            python_callable = fetch_insurance_errors_to_drop,
            provide_context = True
        )

        refresh_insurance_rejections = PythonOperator(
            task_id = 'refresh_insurance_rejections',
            python_callable = refresh_insurance_rejections,
            provide_context = True
        )

        fetch_perc_ins_rej >> fetch_insurance_errors_to_drop >> refresh_insurance_rejections

    with TaskGroup('sop') as sop:

        from sub_tasks.gsheets.ded_earn import fetch_perc_sop
        from sub_tasks.gsheets.sop import (fetch_sop_branch_info, fetch_sop)
        from sub_tasks.incentives.incentive_factors import refresh_sop   

        fetch_perc_sop = PythonOperator(
            task_id = 'fetch_perc_sop',
            python_callable = fetch_perc_sop,
            provide_context = True
        )

        fetch_sop_branch_info = PythonOperator(
            task_id = 'fetch_sop_branch_info',
            python_callable=fetch_sop_branch_info,
            provide_context=True
        )

        fetch_sop = PythonOperator(
            task_id = 'fetch_sop',
            python_callable=fetch_sop,
            provide_context=True
        )

        refresh_sop = PythonOperator(
            task_id = 'refresh_sop',
            python_callable = refresh_sop,
            provide_context = True
        )

        fetch_perc_sop >> fetch_sop_branch_info >> fetch_sop >> refresh_sop 
    
    with TaskGroup('insurance_feedback_conversion') as insurance_feedback_conversion:

        from sub_tasks.incentives.incentive_factors import refresh_insurance_feedback_conversion

        refresh_insurance_feedback_conversion = PythonOperator(
            task_id = 'refresh_insurance_feedback_conversion',
            python_callable = refresh_insurance_feedback_conversion,
            provide_context = True
        )
    
    from sub_tasks.incentives.incentive_factors import refresh_all_activity
    
    refresh_all_activity = PythonOperator(
        task_id = 'refresh_all_activity',
        python_callable = refresh_all_activity,
        provide_context = True
    )

        
    from sub_tasks.incentives.incentive_factors import refresh_order_contents
    
    refresh_order_contents = PythonOperator(
        task_id = 'refresh_order_contents',
        python_callable = refresh_order_contents,
        provide_context = True
    )


    from sub_tasks.incentives.incentive_factors import refresh_lens_silh

    refresh_lens_silh = PythonOperator(
        task_id = 'refresh_lens_silh',
        python_callable = refresh_lens_silh,
        provide_context = True
    )


    finish = DummyOperator(
        task_id = "finish"
    )
    
    start >> sales >> refresh_all_activity >> refresh_order_contents >> refresh_lens_silh >> google_reviews >> sunglasses >> nps_survey  >> insurance_feedback_conversion >> insurance_rejections >> sop >> finish
    # start >> gr >> sg >> nps_det >> create_all_activity >> ins_rej >> sop >> ins_conv >>  finish
    