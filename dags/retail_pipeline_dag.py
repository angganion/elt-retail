"""Scheduled parent workflow for the retail pipeline."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="retail_pipeline",
    description="Run retail ingestion then DuckDB transformations",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 0,
    },
    dagrun_timeout=timedelta(hours=4),
    tags=["retail", "pipeline", "local"],
) as dag:
    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion",
        trigger_dag_id="retail_ingestion",
        wait_for_completion=True,
        deferrable=True,
        reset_dag_run=True,
    )
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="retail_transform",
        wait_for_completion=True,
        deferrable=True,
        reset_dag_run=True,
    )

    trigger_ingestion >> trigger_transform
