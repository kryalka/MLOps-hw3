from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task

from pipeline.quality import assert_report, report_dataset

with DAG(
    "data_quality", schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc), catchup=False,
    max_active_runs=1, default_args={"retries": 1, "retry_delay": timedelta(seconds=15)},
    tags=["hw3", "dq"],
) as dag:
    report = task(report_dataset)(stamp="{{ ts_nodash }}")
    task(assert_report)(report)
