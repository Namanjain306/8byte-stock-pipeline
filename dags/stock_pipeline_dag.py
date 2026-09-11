"""
stock_pipeline_dag.py
----------------------
Airflow DAG that runs the stock market data pipeline on a schedule.

Flow:
    fetch_and_store_task  ->  (calls run_pipeline() from scripts/fetch_stock_data.py)

Schedule: runs once every hour (change SCHEDULE below for daily, etc).
Retries: each run retries up to 2 times with a 5-minute delay if it fails,
so a transient API/network issue doesn't require manual intervention.
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# scripts/ is mounted into the Airflow containers at /opt/airflow/scripts
sys.path.insert(0, "/opt/airflow/scripts")

from fetch_stock_data import run_pipeline  # noqa: E402

SCHEDULE = "@hourly"  # change to "@daily" if you prefer once-a-day runs

default_args = {
    "owner": "8byte-assignment",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_market_pipeline",
    description="Fetches stock market data and upserts it into PostgreSQL",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "assignment", "8byte"],
) as dag:

    def _run_pipeline_task(**context):
        """Thin wrapper so Airflow's task logs show the pipeline's result."""
        result = run_pipeline()
        context["ti"].xcom_push(key="pipeline_result", value=result)
        return result

    fetch_and_store_task = PythonOperator(
        task_id="fetch_and_store_stock_data",
        python_callable=_run_pipeline_task,
    )

    fetch_and_store_task
