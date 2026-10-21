from pathlib import Path

import pandas as pd
from psycopg2.extras import Json

from pipeline.db import connect, lock, one, read_frame
from pipeline.files import atomic_write, copy_atomic, read_json, write_frame, write_json
from pipeline.modeling import approve, features, fit_model, load_model, metrics
from pipeline.settings import MODELS, REPORTS, WINDOW_MONTHS, WORK


def prepare_window(run_id):
    """Читает три месяца витрины и сохраняет окно. Повтор берёт тот же месяц."""
    with connect() as conn:
        lock(conn)
        existing = one(conn, """SELECT r.info FROM training_runs r JOIN training_requests q
                               ON q.version = r.version WHERE q.run_id = %s""", (run_id,))
        if existing:
            return existing["info"]
        state = one(conn, "SELECT * FROM training_state WHERE id = 1")
        month = pd.Timestamp(state["next_month"])
        existing = one(conn, "SELECT version, info FROM training_runs WHERE batch_month = %s", (month,))
        if existing:
            info = existing["info"]
        else:
            start = month - pd.DateOffset(months=WINDOW_MONTHS - 1)
            end = month + pd.DateOffset(months=1)
            frame = read_frame(conn, start, end)
            expected = pd.period_range(start, month, freq="M")
            actual = pd.PeriodIndex(pd.to_datetime(frame["transaction_time"]).dt.to_period("M").unique())
            if not expected.equals(actual.sort_values()):
                from airflow.exceptions import AirflowSkipException
                raise AirflowSkipException(f"Waiting for complete months {list(map(str, expected))}")
            version = month.strftime("%Y%m")
            directory = WORK / "training" / version
            path = directory / "window.parquet"
            write_frame(path, frame)
            info = {"version": version, "batch_month": month.date().isoformat(),
                    "window_start": start.date().isoformat(), "window_end": month.date().isoformat(),
                    "window_path": str(path), "directory": str(directory)}
            with conn.cursor() as cur:
                cur.execute("INSERT INTO training_runs (version, batch_month, info) VALUES (%s, %s, %s)",
                            (version, month, Json(info)))
        with conn.cursor() as cur:
            cur.execute("INSERT INTO training_requests VALUES (%s, %s)", (run_id, info["version"]))
    return info


def preprocess(info):
    """Читает окно, делит по времени и пишет признаки train/test. Повтор заменяет те же файлы."""
    frame = pd.read_parquet(info["window_path"])
    times = pd.to_datetime(frame["transaction_time"])
    cutoff = pd.Timestamp(info["batch_month"])
    train = frame.loc[times < cutoff]
    test = frame.loc[times >= cutoff]
    if train.empty or test.empty or train["target"].nunique() != 2 or test["target"].nunique() != 2:
        raise ValueError("Train and test must both have rows and both target classes")
    directory = Path(info["directory"])
    for name, raw in (("train", train), ("test", test)):
        processed = features(raw)
        processed["target"] = raw["target"].to_numpy()
        write_frame(directory / f"{name}.parquet", processed)
    return {**info, "train_rows": len(train), "test_rows": len(test)}


def train_candidate(info):
    """Читает train.parquet и сохраняет кандидата. Повтор заменяет его файл, не трогая активную модель."""
    directory = Path(info["directory"])
    train = pd.read_parquet(directory / "train.parquet")
    model = fit_model(train.drop(columns="target"), train["target"])
    atomic_write(directory / "candidate.cbm", lambda path: model.save_model(str(path), format="cbm"))
    return info


def compare_models(info):
    """Читает test.parquet и сравнивает модели. Сохраняет метрики; повтор берёт готовое решение."""
    with connect() as conn:
        row = one(conn, "SELECT decision FROM training_runs WHERE version = %s", (info["version"],))
        if row["decision"] is not None:
            decision = row["decision"]
        else:
            current = read_json(MODELS / "latest.json")
            if pd.Timestamp(current["trained_until"]) >= pd.Timestamp(info["batch_month"]):
                raise ValueError("Current model has seen the test month; comparison would leak labels")
            directory = Path(info["directory"])
            test = pd.read_parquet(directory / "test.parquet")
            x, y = test.drop(columns="target"), test["target"]
            candidate_metrics = metrics(load_model(directory / "candidate.cbm"), x, y)
            current_metrics = metrics(load_model(MODELS / current["file"]), x, y)
            decision = {**info, "candidate_metrics": candidate_metrics, "current_metrics": current_metrics,
                        "current_version": current["version"], "approved": approve(candidate_metrics, current_metrics),
                        "trained_until": (pd.Timestamp(info["batch_month"]) - pd.DateOffset(months=1)).date().isoformat()}
            with conn.cursor() as cur:
                cur.execute("UPDATE training_runs SET decision = %s WHERE version = %s",
                            (Json(decision), info["version"]))
    write_json(REPORTS / "models" / f'{info["version"]}.json', decision)
    return decision


def publish_model(decision):
    """Читает решение и публикует одобренную модель. После сохранения двигает месяц только один раз."""
    version = decision["version"]
    candidate = Path(decision["directory"]) / "candidate.cbm"
    with connect() as conn:
        lock(conn)
        row = one(conn, "SELECT committed FROM training_runs WHERE version = %s", (version,))
        state = one(conn, "SELECT next_month FROM training_state WHERE id = 1")
        if row["committed"]:
            return version
        if state["next_month"].isoformat() != decision["batch_month"]:
            raise ValueError("Refusing to commit a month out of order")
        if not candidate.is_file():
            raise FileNotFoundError(candidate)
        if decision["approved"]:
            filename = f"model_{version}.cbm"
            copy_atomic(candidate, MODELS / filename)
            write_json(MODELS / "latest.json", {"version": version, "file": filename,
                       "trained_until": decision["trained_until"]})
        next_month = (pd.Timestamp(decision["batch_month"]) + pd.DateOffset(months=1)).date()
        with conn.cursor() as cur:
            cur.execute("""UPDATE training_state SET next_month = %s, window_start = %s,
                           window_end = %s, last_version = %s WHERE id = 1""",
                        (next_month, decision["window_start"], decision["window_end"], version))
            cur.execute("UPDATE training_runs SET committed = TRUE WHERE version = %s", (version,))
    return version
