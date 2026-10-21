import logging
import os
import time
from pathlib import Path

import pandas as pd
import psycopg2

from pipeline.db import copy_frame, lock, one, read_frame
from pipeline.files import atomic_write, write_json
from pipeline.modeling import features, fit_model
from pipeline.settings import MODELS, RAW_COLUMNS, REPORTS, WINDOW_MONTHS, WORK

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("data_loader")
CSV_PATH = Path(os.getenv("LABELED_CSV_PATH", "/data/train.csv"))


def get_db_config():
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB", "fraud_db"),
        "user": os.getenv("POSTGRES_USER", "fraud_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "fraud_password"),
    }


def connect():
    config = get_db_config()
    last_error = None
    for attempt in range(1, 31):
        try:
            return psycopg2.connect(**config)
        except psycopg2.OperationalError as exc:
            last_error = exc
            logger.info("Postgres ещё не готов (%s/30): %s", attempt, exc)
            time.sleep(2)
    raise last_error


def ensure_airflow_database():
    conn = connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'")
            if cur.fetchone() is None:
                cur.execute("CREATE DATABASE airflow")
    finally:
        conn.close()


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS labeled_transactions (
                row_id TEXT PRIMARY KEY, transaction_time TIMESTAMP NOT NULL,
                merch TEXT, cat_id TEXT, amount DOUBLE PRECISION,
                name_1 TEXT, name_2 TEXT, gender TEXT, street TEXT,
                one_city TEXT, us_state TEXT, post_code TEXT,
                lat DOUBLE PRECISION, lon DOUBLE PRECISION,
                population_city DOUBLE PRECISION, jobs TEXT,
                merchant_lat DOUBLE PRECISION, merchant_lon DOUBLE PRECISION,
                target SMALLINT
            );
            CREATE INDEX IF NOT EXISTS labeled_transactions_transaction_time_idx
                ON labeled_transactions (transaction_time);
            CREATE TABLE IF NOT EXISTS training_state (
                id INTEGER PRIMARY KEY CHECK (id = 1), next_month DATE NOT NULL,
                window_start DATE, window_end DATE, last_version TEXT
            );
            CREATE TABLE IF NOT EXISTS training_runs (
                version TEXT PRIMARY KEY, batch_month DATE UNIQUE NOT NULL,
                info JSONB NOT NULL, decision JSONB,
                committed BOOLEAN NOT NULL DEFAULT FALSE
            );
            CREATE TABLE IF NOT EXISTS training_requests (
                run_id TEXT PRIMARY KEY,
                version TEXT NOT NULL REFERENCES training_runs(version)
            );
            CREATE TABLE IF NOT EXISTS supply_runs (
                run_id TEXT PRIMARY KEY, batch_month DATE UNIQUE NOT NULL,
                path TEXT NOT NULL, applied BOOLEAN NOT NULL DEFAULT FALSE
            );
            CREATE TABLE IF NOT EXISTS supply_requests (
                run_id TEXT PRIMARY KEY,
                canonical_run_id TEXT NOT NULL REFERENCES supply_runs(run_id)
            );
        """)
    logger.info("Схема витрины проверена.")


def row_count(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM labeled_transactions")
        return cur.fetchone()[0]


def load_csv():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Не найден файл размеченных данных: {CSV_PATH}")
    logger.info("Чтение %s", CSV_PATH)
    frame = pd.read_csv(CSV_PATH, dtype={"post_code": str})
    missing = [column for column in RAW_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"В CSV нет колонок: {missing}")
    frame = frame[RAW_COLUMNS].copy()
    frame["transaction_time"] = pd.to_datetime(frame["transaction_time"])
    frame["row_id"] = [f"source-{index}" for index in range(len(frame))]
    last = frame["transaction_time"].max().to_period("M").to_timestamp()
    start = last - pd.DateOffset(months=WINDOW_MONTHS - 1)
    frame = frame.loc[frame["transaction_time"] >= start].sort_values(["transaction_time", "row_id"])
    if frame["transaction_time"].dt.to_period("M").nunique() != WINDOW_MONTHS:
        raise ValueError("train.csv must contain at least three consecutive months")
    logger.info("CSV прочитан: %s строк, время с %s по %s", len(frame),
                frame["transaction_time"].min(), frame["transaction_time"].max())
    return frame


def ensure_state(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM training_state WHERE id = 1")
        if cur.fetchone()[0]:
            logger.info("training_state уже заполнен, указатель месяца не сбрасывается.")
            return
        cur.execute("SELECT date_trunc('month', max(transaction_time)) FROM labeled_transactions")
        last = pd.Timestamp(cur.fetchone()[0])
        start = last - pd.DateOffset(months=WINDOW_MONTHS - 1)
        cur.execute("""INSERT INTO training_state (id, window_start, window_end, next_month)
                       VALUES (1, %s, %s, %s)""", (start.date(), last.date(), last.date()))
    logger.info("Стартовое окно обучения: %s — %s", start.date(), last.date())


def read_state(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT window_start, window_end FROM training_state WHERE id = 1")
        window_start, window_end = cur.fetchone()
    return window_start, window_end


def create_initial_model(conn):
    start, last = read_state(conn)
    train = read_frame(conn, start, last)
    model = fit_model(features(train), train["target"])
    atomic_write(MODELS / "model_initial.cbm", lambda p: model.save_model(str(p), format="cbm"))
    write_json(MODELS / "latest.json", {"version": "initial", "file": "model_initial.cbm",
               "trained_until": (pd.Timestamp(last) - pd.DateOffset(months=1)).date().isoformat()})


def main():
    ensure_airflow_database()
    for path in (MODELS, REPORTS, WORK, REPORTS / "dq", REPORTS / "models",
                 WORK / "training", WORK / "supply"):
        path.mkdir(parents=True, exist_ok=True)
        path.chmod(0o777)
    conn = connect()
    try:
        with conn:
            lock(conn)
            ensure_schema(conn)
            existing = one(conn, "SELECT * FROM training_state WHERE id = 1")
            if existing and not (MODELS / "latest.json").exists():
                raise RuntimeError("DB initialized but model registry missing; restore the matching volume")
            if row_count(conn) == 0:
                copy_frame(conn, load_csv())
            else:
                logger.info("Витрина уже заполнена, повторная загрузка пропущена.")
            ensure_state(conn)
            if not existing:
                create_initial_model(conn)
    finally:
        conn.close()
    logger.info("Загрузка витрины завершена.")


if __name__ == "__main__":
    main()
