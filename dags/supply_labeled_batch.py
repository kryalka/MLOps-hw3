from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task

from pipeline.supply import append_month, plan_month, prune_history

with DAG(
    "supply_labeled_batch", schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc), catchup=False,
    max_active_runs=1, default_args={"retries": 2, "retry_delay": timedelta(seconds=15)},
    tags=["hw3", "supply"],
) as dag:
    plan = task(plan_month)(run_id="{{ run_id }}")
    appended = task(append_month)(plan)
    task(prune_history)(appended)
