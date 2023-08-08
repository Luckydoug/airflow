import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from airflow import DAG
# from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.example_dags.subdags.subdag import subdag
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from sub_tasks.ticketing_data.departments import (fetch_source_depts, fetch_emails, create_dept_live)
from sub_tasks.ticketing_data.tickets import (fetch_source_tickets, fetch_source_ticket_data,
fetch_source_ticket_statuses, fetch_source_ticketlist, fetch_source_ticketlist_items, create_tickets_live,
update_fact_tickets, create_mviews_tickets)
from sub_tasks.ticketing_data.ticket_threads import (fetch_source_tickets_thread, fetch_source_tickets_thread_entry, 
create_source_ticket_response, create_ticket_response)
from sub_tasks.ticketing_data.users import (fetch_source_staff, fetch_source_users, fetch_source_user_email,
create_users_live, create_staff_live)
from sub_tasks.ticketing_data.teams import (fetch_source_teams, fetch_source_team_members,
create_teams_live)
from sub_tasks.ticketing_data.tasks import (fetch_source_tasks, fetch_source_task_data, create_live_tasks)
from sub_tasks.ticketing_data.others import (fetch_source_sla)
from sub_tasks.runtimes.ticketing_time import (get_start_time)
from sub_tasks.ticketing_data.refresh_cache import (refresh_metadata, refresh_cache, cda_cache)



# from tmp.python_test
DAG_ID = 'Ticketing_ETLs_Pipeline'

default_args = {
    'owner': 'Iconia ETLs',
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
    schedule_interval='*/30 5-16 * * *',
    catchup=False
    ) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )
    
    """
    GET START TIME
    """

    with TaskGroup('start_time') as start_time:

        get_start_time = PythonOperator(
            task_id = 'get_start_time',
            python_callable=get_start_time,
            provide_context=True
        )

    with TaskGroup('depts') as depts:

        fetch_source_depts = PythonOperator(
            task_id = 'fetch_source_depts',
            python_callable=fetch_source_depts,
            provide_context=True
        )

        fetch_emails = PythonOperator(
            task_id = 'fetch_emails',
            python_callable=fetch_emails,
            provide_context=True
        )

        create_dept_live = PythonOperator(
            task_id = 'create_dept_live',
            python_callable=create_dept_live,
            provide_context=True
        )

        fetch_source_depts >> fetch_emails >> create_dept_live 
    
    with TaskGroup('tickets') as tickets:

        fetch_source_tickets = PythonOperator(
            task_id = 'fetch_source_tickets',
            python_callable=fetch_source_tickets,
            provide_context=True
        )

        fetch_source_ticket_data = PythonOperator(
            task_id = 'fetch_source_ticket_data',
            python_callable=fetch_source_ticket_data,
            provide_context=True
        )

        fetch_source_ticket_statuses = PythonOperator(
            task_id = 'fetch_source_ticket_statuses',
            python_callable=fetch_source_ticket_statuses,
            provide_context=True
        )

        fetch_source_ticketlist = PythonOperator(
            task_id = 'fetch_source_ticketlist',
            python_callable=fetch_source_ticketlist,
            provide_context=True
        )

        fetch_source_ticketlist_items = PythonOperator(
            task_id = 'fetch_source_ticketlist_items',
            python_callable=fetch_source_ticketlist_items,
            provide_context=True
        )

        create_tickets_live = PythonOperator(
            task_id = 'create_tickets_live',
            python_callable=create_tickets_live,
            provide_context=True
        )

        update_fact_tickets = PythonOperator(
            task_id = 'update_fact_tickets',
            python_callable=update_fact_tickets,
            provide_context=True
        )

        create_mviews_tickets = PythonOperator(
            task_id = 'create_mviews_tickets',
            python_callable=create_mviews_tickets,
            provide_context=True
        )

        [fetch_source_tickets, fetch_source_ticket_data, fetch_source_ticket_statuses, fetch_source_ticketlist, fetch_source_ticketlist_items] >> create_tickets_live >> update_fact_tickets >> create_mviews_tickets

    with TaskGroup('ticket_thread') as ticket_thread:

        fetch_source_tickets_thread = PythonOperator(
            task_id = 'fetch_source_tickets_thread',
            python_callable=fetch_source_tickets_thread,
            provide_context=True
        )

        fetch_source_tickets_thread_entry = PythonOperator(
            task_id = 'fetch_source_tickets_thread_entry',
            python_callable=fetch_source_tickets_thread_entry,
            provide_context=True
        )

        create_source_ticket_response = PythonOperator(
            task_id = 'create_source_ticket_response',
            python_callable=create_source_ticket_response,
            provide_context=True
        )

        create_ticket_response = PythonOperator(
            task_id = 'create_ticket_response',
            python_callable=create_ticket_response,
            provide_context=True
        )

        [fetch_source_tickets_thread, fetch_source_tickets_thread_entry] >> create_source_ticket_response >> create_ticket_response

    with TaskGroup('users') as users:

        fetch_source_staff = PythonOperator(
            task_id = 'fetch_source_staff',
            python_callable=fetch_source_staff,
            provide_context=True
        )

        fetch_source_users = PythonOperator(
            task_id = 'fetch_source_users',
            python_callable=fetch_source_users,
            provide_context=True
        )

        fetch_source_user_email = PythonOperator(
            task_id = 'fetch_source_user_email',
            python_callable=fetch_source_user_email,
            provide_context=True
        )

        create_users_live = PythonOperator(
            task_id = 'create_users_live',
            python_callable=create_users_live,
            provide_context=True
        )

        create_staff_live = PythonOperator(
            task_id = 'create_staff_live',
            python_callable=create_staff_live,
            provide_context=True
        )

        [fetch_source_staff, fetch_source_users, fetch_source_user_email] >> create_users_live >>create_staff_live

    with TaskGroup('teams') as teams:

        fetch_source_teams = PythonOperator(
            task_id = 'fetch_source_teams',
            python_callable=fetch_source_teams,
            provide_context=True
        )

        fetch_source_team_members = PythonOperator(
            task_id = 'fetch_source_team_members',
            python_callable=fetch_source_team_members,
            provide_context=True
        )

        create_teams_live = PythonOperator(
            task_id = 'create_teams_live',
            python_callable=create_teams_live,
            provide_context=True
        )

        [fetch_source_teams, fetch_source_team_members] >> create_teams_live

    with TaskGroup('tasks') as tasks:

        fetch_source_tasks = PythonOperator(
            task_id = 'fetch_source_tasks',
            python_callable=fetch_source_tasks,
            provide_context=True
        )

        fetch_source_task_data = PythonOperator(
            task_id = 'fetch_source_task_data',
            python_callable=fetch_source_task_data,
            provide_context=True
        )

        create_live_tasks = PythonOperator(
            task_id = 'create_live_tasks',
            python_callable=create_live_tasks,
            provide_context=True
        )

        [fetch_source_tasks, fetch_source_task_data] >> create_live_tasks

    with TaskGroup('otherdata') as otherdata:

        fetch_source_sla = PythonOperator(
            task_id = 'fetch_source_sla',
            python_callable=fetch_source_sla,
            provide_context=True
        )

    with TaskGroup('cache') as cache:

        cda_cache = PythonOperator(
            task_id = 'cda_cache',
            python_callable=cda_cache,
            provide_context=True
        )

        cda_cache

    finish = DummyOperator(
        task_id = "finish"
    ) 

    start >> start_time >> depts >> tickets >> ticket_thread >> users >> teams >> tasks >> otherdata >> cache >> finish