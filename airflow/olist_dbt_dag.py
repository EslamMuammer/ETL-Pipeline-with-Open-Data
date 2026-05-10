from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# المسارات
DBT_PROJECT_DIR = "/home/eslam/DataEngineering/Olist"
DBT_EXECUTABLE = "/home/eslam/DataEngineering/airflow_env/bin/dbt"

default_args = {
    'owner': 'eslam',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='olist_sales_pipeline',
    default_args=default_args,
    description='Olist Sales Analysis Pipeline',
    schedule='@daily',  # تم تعديلها من schedule_interval لـ schedule
    catchup=False,
    tags=['dbt', 'olist']
) as dag:

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'{DBT_EXECUTABLE} seed --project-dir {DBT_PROJECT_DIR}'
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'{DBT_EXECUTABLE} run --project-dir {DBT_PROJECT_DIR}'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'{DBT_EXECUTABLE} test --project-dir {DBT_PROJECT_DIR}'
    )

    dbt_seed >> dbt_run >> dbt_test