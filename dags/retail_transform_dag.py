"""Airflow tasks for the ordered DuckDB medallion transformation."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"

with DAG(
    dag_id="retail_transform",
    description="Build bronze, silver, and gold layers in DuckDB",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    dagrun_timeout=timedelta(hours=2),
    tags=["retail", "transform", "duckdb", "local"],
) as dag:
    run_bronze = BashOperator(
        task_id="run_bronze",
        bash_command="PIPELINE_RUN_ID='{{ run_id }}' python transform/transform.py --step bronze",
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(hours=1),
    )
    run_silver = BashOperator(
        task_id="run_silver",
        bash_command="PIPELINE_RUN_ID='{{ run_id }}' python transform/transform.py --step silver",
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(hours=1),
    )
    run_gold = BashOperator(
        task_id="run_gold",
        bash_command="PIPELINE_RUN_ID='{{ run_id }}' python transform/transform.py --step gold",
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(hours=1),
    )

    run_bronze >> run_silver >> run_gold
