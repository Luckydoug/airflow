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
    
    with TaskGroup('gr_targets') as gr: 

        from sub_tasks.gsheets.gr_targets import (fetch_gr_targets, create_gr_summary)
            
        fetch_gr_targets = PythonOperator(
            task_id = 'fetch_gr_targets',
            python_callable = fetch_gr_targets,
            provide_context = True
        )

        create_gr_summary = PythonOperator(
            task_id = 'create_gr_summary',
            python_callable = create_gr_summary,
            provide_context = True
        )

        fetch_gr_targets >> create_gr_summary
    
    with TaskGroup('sg_targets') as sg:

        from sub_tasks.gsheets.sg_sales_targets import (fetch_sg_targets, create_source_sg_sales, create_sg_summary)
    
        fetch_sg_targets = PythonOperator(
            task_id = 'fetch_sg_targets',
            python_callable = fetch_sg_targets,
            provide_context = True
        ) 

        create_source_sg_sales = PythonOperator(
            task_id = 'create_source_sg_sales',
            python_callable = create_source_sg_sales,
            provide_context = True
        ) 

        create_sg_summary = PythonOperator(
            task_id = 'create_sg_summary',
            python_callable = create_sg_summary,
            provide_context = True
        )

        fetch_sg_targets >> create_source_sg_sales >> create_sg_summary
    
    with TaskGroup('nps_det') as nps_det:

        from sub_tasks.gsheets.ded_earn import fetch_perc_det
        from sub_tasks.incentives.incentive_factors import create_nps_summary
                                               
        fetch_perc_det = PythonOperator(
            task_id = 'fetch_perc_det',
            python_callable = fetch_perc_det,
            provide_context = True
        )
 
        create_nps_summary = PythonOperator(
            task_id = 'create_nps_summary',
            python_callable = create_nps_summary,
            provide_context = True
        )

        fetch_perc_det >> create_nps_summary

    with TaskGroup('ins_rej') as ins_rej:

        from sub_tasks.gsheets.ded_earn import fetch_perc_ins_rej
        from sub_tasks.incentives.incentive_factors import create_ins_rejections,create_ins_rejection_summary
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

        create_ins_rejections = PythonOperator(
            task_id = 'create_ins_rejections',
            python_callable = create_ins_rejections,
            provide_context = True
        )

        create_ins_rejection_summary = PythonOperator(
            task_id = 'create_ins_rejection_summary',
            python_callable = create_ins_rejection_summary,
            provide_context = True
        )

        fetch_perc_ins_rej >> fetch_insurance_errors_to_drop >> create_ins_rejections >>create_ins_rejection_summary

    with TaskGroup('sop') as sop:

        from sub_tasks.gsheets.ded_earn import fetch_perc_sop
        from sub_tasks.gsheets.sop import (fetch_sop_branch_info, fetch_sop)
        from sub_tasks.incentives.incentive_factors import create_sop, create_sop_summary
        
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

        create_sop = PythonOperator(
            task_id = 'create_sop',
            python_callable = create_sop,
            provide_context = True
        )

        create_sop_summary = PythonOperator(
            task_id = 'create_sop_summary',
            python_callable = create_sop_summary,
            provide_context = True
        )

        fetch_perc_sop >> fetch_sop_branch_info >> fetch_sop >> create_sop >> create_sop_summary
    
    with TaskGroup('ins_conv') as ins_conv:

        from sub_tasks.incentives.incentive_factors import (create_ins_feedback_conv, create_ins_direct_conv)

        create_ins_feedback_conv = PythonOperator(
            task_id = 'create_ins_feedback_conv',
            python_callable = create_ins_feedback_conv,
            provide_context = True
        )

        create_ins_direct_conv = PythonOperator(
            task_id = 'create_ins_direct_conv',
            python_callable = create_ins_direct_conv,
            provide_context = True
        )
    
    create_ins_feedback_conv >> create_ins_direct_conv

    from sub_tasks.incentives.incentive_factors import create_all_activity
    
    create_all_activity = PythonOperator(
        task_id = 'create_all_activity',
        python_callable = create_all_activity,
        provide_context = True
    )


    finish = DummyOperator(
        task_id = "finish"
    )
    
    start >> sales >> gr >> sg >> nps_det >> create_all_activity >> ins_rej >> sop >> ins_conv >>  finish
    # start >> gr >> sg >> nps_det >> create_all_activity >> ins_rej >> sop >> ins_conv >>  finish
    