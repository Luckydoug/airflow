from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 19),
    'retries': 1,
}

def sum_func(numb):
    return 2

def sum_prev_func(**kwargs):
    ti = kwargs['ti']
    num = 2
    num_value = ti.xcom_pull(task_ids='sum_task')
    return 2 + num_value

def sum_three_func(**kwargs):
    ti = kwargs['ti']
    num_value = ti.xcom_pull(task_ids='sum_prev_task')
    return num_value + 3

with DAG('my_dag', schedule_interval=None, default_args=default_args) as dag:
    sum_task = PythonOperator(
        task_id='sum_task',
        python_callable=sum_func,
        op_args=[6],
        provide_context=True
    )

    sum_prev_task = PythonOperator(
        task_id='sum_prev_task',
        python_callable=sum_prev_func,
        provide_context=True
    )

    sum_three_task = PythonOperator(
        task_id='sum_three_task',
        python_callable=sum_three_func,
        provide_context=True
    )

    sum_task >> sum_prev_task >> sum_three_task
