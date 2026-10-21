from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task

from pipeline.retrain import compare_models, prepare_window, preprocess, publish_model, train_candidate

with DAG(
    "retrain_catboost", schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc), catchup=False,
    max_active_runs=1, default_args={"retries": 2, "retry_delay": timedelta(seconds=15)},
    tags=["hw3", "training"],
) as dag:
    window = task(prepare_window)(run_id="{{ run_id }}")
    processed = task(preprocess)(window)
    candidate = task(train_candidate)(processed)
    decision = task(compare_models)(candidate)
    task(publish_model)(decision)
