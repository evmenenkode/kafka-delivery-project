from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "denis",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
}

DBT_DIR = "/opt/dbt/delivery_dwh"

with DAG(
    dag_id="dbt_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    description="dbt pipeline in Snowflake",
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test"
    )

    dbt_run >> dbt_test