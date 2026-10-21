import numpy as np
import pandas as pd

from pipeline.db import connect, read_frame
from pipeline.files import atomic_write
from pipeline.settings import RAW_COLUMNS, REPORTS


def quality_checks(frame):
    checks = {
        "Выборка не пустая": not frame.empty,
        "Все обязательные колонки присутствуют": set(["row_id", *RAW_COLUMNS]).issubset(frame.columns),
    }
    if not all(checks.values()):
        return checks
    numeric = frame[["amount", "population_city", "lat", "lon", "merchant_lat", "merchant_lon"]].apply(
        pd.to_numeric, errors="coerce"
    )
    checks.update({
        "Обязательные поля заполнены": frame[["row_id", *RAW_COLUMNS]].notna().all().all(),
        "ID уникальны": frame["row_id"].is_unique,
        "Даты корректны": pd.to_datetime(frame["transaction_time"], errors="coerce").notna().all(),
        "Target равен 0 или 1": frame["target"].isin([0, 1]).all(),
        "Сумма конечная и положительная": (np.isfinite(numeric["amount"]) & numeric["amount"].gt(0)).all(),
        "Население конечное и неотрицательное": (
            np.isfinite(numeric["population_city"]) & numeric["population_city"].ge(0)
        ).all(),
        "Координаты допустимы": all(
            numeric[c].between(-limit, limit).all()
            for c, limit in [("lat", 90), ("lon", 180), ("merchant_lat", 90), ("merchant_lon", 180)]
        ),
    })
    return {rule: bool(passed) for rule, passed in checks.items()}


def report_dataset(stamp):
    """Читает витрину и пишет HTML-отчёт. Повтор обновляет отчёт, не меняя данные."""
    with connect() as conn:
        frame = read_frame(conn)
    checks = quality_checks(frame)
    failed = [rule for rule, passed in checks.items() if not passed]
    table = pd.DataFrame([
        {"Правило": rule, "Результат": "PASS" if passed else "FAIL"}
        for rule, passed in checks.items()
    ]).to_html(index=False, escape=True)
    html = (
        '<!doctype html><html lang="ru"><meta charset="utf-8"><title>Data Quality</title>'
        f'<h1>Data Quality: {"FAIL" if failed else "PASS"}</h1>'
        f'<p>Строк: {len(frame)}</p>{table}</html>'
    )
    path = REPORTS / "dq" / f"report_{stamp}.html"
    for destination in (path, path.with_name("latest.html")):
        atomic_write(destination, lambda p: p.write_text(html, encoding="utf-8"))
    return {"report_path": str(path), "failed": failed}


def assert_report(result):
    """Читает итог проверки и останавливает DAG при нарушениях. Повтор не меняет данные и отчёт."""
    if result["failed"]:
        raise ValueError(f"Data Quality: {result['failed']}. Отчёт: {result['report_path']}")
    return result["report_path"]
