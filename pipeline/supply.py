import pandas as pd

from pipeline.db import connect, copy_frame, lock, one, read_frame
from pipeline.files import write_frame
from pipeline.settings import BATCH_ROWS, HISTORY_MONTHS, WINDOW_MONTHS, WORK


def shift_to_month(frame, month):
    result = frame.copy()
    times = pd.to_datetime(result["transaction_time"])
    month = pd.Timestamp(month)
    days = times.dt.day.clip(upper=month.days_in_month) - 1
    result["transaction_time"] = month + pd.to_timedelta(days, unit="D") + (times - times.dt.normalize())
    result["row_id"] = [f"supply-{month:%Y%m}-{i}" for i in range(len(result))]
    return result


def plan_month(run_id):
    """Читает витрину и сохраняет батч следующего месяца. Повтор использует прежний батч."""
    with connect() as conn:
        lock(conn)
        existing = one(conn, """SELECT r.* FROM supply_runs r JOIN supply_requests q
                               ON q.canonical_run_id = r.run_id WHERE q.run_id = %s""", (run_id,))
        if existing:
            return {"run_id": existing["run_id"], "batch_month": existing["batch_month"].isoformat(), "path": existing["path"]}
        pending = one(conn, "SELECT * FROM supply_runs WHERE NOT applied ORDER BY batch_month LIMIT 1")
        if pending:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO supply_requests VALUES (%s, %s)", (run_id, pending["run_id"]))
            return {"run_id": pending["run_id"], "batch_month": pending["batch_month"].isoformat(), "path": pending["path"]}
        latest = one(conn, "SELECT date_trunc('month', max(transaction_time)) AS month FROM labeled_transactions")["month"]
        month = pd.Timestamp(latest) + pd.DateOffset(months=1)
        cutoff = month - pd.DateOffset(months=HISTORY_MONTHS - 1)
        state = one(conn, "SELECT next_month FROM training_state WHERE id = 1")
        needed = pd.Timestamp(state["next_month"]) - pd.DateOffset(months=WINDOW_MONTHS - 1)
        if cutoff > needed:
            from airflow.exceptions import AirflowSkipException
            raise AirflowSkipException("Supply waits: retention would delete a pending training month")
        if BATCH_ROWS < 2:
            raise ValueError("BATCH_ROWS must be at least 2")
        sample = read_frame(conn, limit=BATCH_ROWS)
        if sample["target"].nunique() != 2:
            raise ValueError("Monthly sample must contain both classes; increase BATCH_ROWS")
        batch = shift_to_month(sample, month)
        path = WORK / "supply" / f"{month:%Y%m}.parquet"
        write_frame(path, batch)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO supply_runs (run_id, batch_month, path) VALUES (%s, %s, %s)",
                        (run_id, month.date(), str(path)))
            cur.execute("INSERT INTO supply_requests VALUES (%s, %s)", (run_id, run_id))
    return {"run_id": run_id, "batch_month": month.date().isoformat(), "path": str(path)}


def append_month(info):
    """Читает батч и добавляет его в витрину. Отметка загрузки защищает от дублей при повторе."""
    with connect() as conn:
        lock(conn)
        row = one(conn, "SELECT applied FROM supply_runs WHERE run_id = %s", (info["run_id"],))
        if not row["applied"]:
            copy_frame(conn, pd.read_parquet(info["path"]))
            with conn.cursor() as cur:
                cur.execute("UPDATE supply_runs SET applied = TRUE WHERE run_id = %s", (info["run_id"],))
    return info


def prune_history(info):
    """Удаляет из витрины старые месяцы. Повторяет ту же очистку и сохраняет нужное обучению окно."""
    cutoff = pd.Timestamp(info["batch_month"]) - pd.DateOffset(months=HISTORY_MONTHS - 1)
    with connect() as conn:
        lock(conn)
        state = one(conn, "SELECT next_month FROM training_state WHERE id = 1")
        needed = pd.Timestamp(state["next_month"]) - pd.DateOffset(months=WINDOW_MONTHS - 1)
        if cutoff > needed:
            raise ValueError("Retention would delete a pending training window")
        with conn.cursor() as cur:
            cur.execute("DELETE FROM labeled_transactions WHERE transaction_time < %s", (cutoff,))
            removed = cur.rowcount
    return {**info, "removed_rows": removed}
