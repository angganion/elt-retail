"""Airflow tasks for the sequential retail ingestion stage."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"
DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="retail_ingestion",
    description="Load retailer, supplier, and review data into the retail landing zone",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    tags=["retail", "ingestion", "local"],
) as dag:
    load_retailer = BashOperator(
        task_id="load_retailer",
        bash_command="PIPELINE_RUN_ID='{{ run_id }}' python ingestion/retailer_to_landing.py",
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(hours=1),
    )
    load_supplier = BashOperator(
        task_id="load_supplier",
        bash_command="PIPELINE_RUN_ID='{{ run_id }}' python ingestion/supplier_to_landing.py",
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(hours=1),
    )
    load_file_sources = BashOperator(
        task_id="load_file_sources",
        bash_command="PIPELINE_RUN_ID='{{ run_id }}' python ingestion/file_sources_to_landing.py",
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(hours=1),
    )
    load_customer_reviews = BashOperator(
        task_id="load_customer_reviews",
        bash_command="PIPELINE_RUN_ID='{{ run_id }}' python ingestion/customer_reviews_to_landing.py",
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(hours=1),
        retries=1,
        retry_delay=timedelta(minutes=2),
    )

    load_retailer >> load_supplier >> load_file_sources >> load_customer_reviews
