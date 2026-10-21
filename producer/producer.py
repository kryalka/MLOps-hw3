import argparse
import csv
import json
import os
import time
from itertools import cycle, islice
from uuid import uuid4
from pathlib import Path
from confluent_kafka import Producer

COLUMNS = [
    "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2",
    "gender", "street", "one_city", "us_state", "post_code", "lat", "lon",
    "population_city", "jobs", "merchant_lat", "merchant_lon"
]

parser = argparse.ArgumentParser()
parser.add_argument("--file", default="/data/train.csv")
parser.add_argument("--limit", type=int, default=0)
parser.add_argument("--repeat", action="store_true")
parser.add_argument("--interval", type=float, default=0)
args = parser.parse_args()
if args.limit < 0:
    parser.error("--limit must be non-negative")
if args.interval < 0:
    parser.error("--interval must be non-negative")

errors = []


def delivery_report(error, message):
    if error is not None:
        errors.append(str(error))


producer = Producer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9096"),
    "enable.idempotence": True,
    "acks": "all",
})
path = Path(args.file)
sent = 0

with path.open(encoding="utf-8", newline="") as file:
    reader = csv.DictReader(file)
    for column in COLUMNS:
        if column not in (reader.fieldnames or []):
            raise ValueError(f"Missing column: {column}")

    rows = islice(reader, args.limit or None)
    if args.repeat:
        rows = cycle(rows)
    for row in rows:
        transaction = {column: row[column] for column in COLUMNS}
        transaction_id = str(uuid4())
        if len(transaction["transaction_time"]) == 16:
            transaction["transaction_time"] += ":00"
        for column in ["lat", "lon", "merchant_lat", "merchant_lon"]:
            transaction[column] = float(transaction[column])
        transaction["population_city"] = int(transaction["population_city"])

        while True:
            try:
                producer.produce(
                    "transactions", key=transaction_id,
                    value=json.dumps({"transaction_id": transaction_id, "data": transaction}).encode(),
                    on_delivery=delivery_report,
                )
                break
            except BufferError:
                producer.poll(1)
        producer.poll(0)
        sent += 1
        if errors:
            raise RuntimeError(f"Kafka delivery errors: {errors}")
        if args.interval:
            time.sleep(args.interval)

remaining = producer.flush(60)
if remaining or errors:
    raise RuntimeError(f"Unsent: {remaining}, errors: {errors}")
print(f"Successfully delivered: {sent}")
