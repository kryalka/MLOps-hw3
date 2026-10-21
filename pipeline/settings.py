import os
from pathlib import Path

SHARED = Path(os.getenv("SHARED_DIR", "/shared"))
MODELS = SHARED / "models"
REPORTS = SHARED / "reports"
WORK = SHARED / "work"
WINDOW_MONTHS = 3
HISTORY_MONTHS = 36
BATCH_ROWS = int(os.getenv("BATCH_ROWS", "5000"))
TARGET = "target"
RAW_COLUMNS = [
    "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2",
    "gender", "street", "one_city", "us_state", "post_code", "lat", "lon",
    "population_city", "jobs", "merchant_lat", "merchant_lon", TARGET,
]
