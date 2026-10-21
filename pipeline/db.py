import io
import os
from contextlib import contextmanager

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from pipeline.settings import RAW_COLUMNS


@contextmanager
def connect():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "fraud_db"),
        user=os.getenv("POSTGRES_USER", "fraud_user"),
        password=os.getenv("POSTGRES_PASSWORD", "fraud_password"),
    )
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def lock(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(3003)")


def one(conn, query, params=()):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        return dict(row) if row else None


def read_frame(conn, start=None, end=None, limit=None):
    query = "SELECT row_id, " + ", ".join(RAW_COLUMNS) + " FROM labeled_transactions"
    params = ()
    if start is not None:
        query += " WHERE transaction_time >= %s AND transaction_time < %s"
        params = (start, end)
    if limit is None:
        query += " ORDER BY transaction_time, row_id"
    else:
        query += " ORDER BY random() LIMIT %s"
        params += (limit,)
    with conn.cursor() as cur:
        cur.execute(query, params)
        return pd.DataFrame(cur.fetchall(), columns=[item.name for item in cur.description])


def copy_frame(conn, frame):
    buffer = io.StringIO()
    columns = ["row_id", *RAW_COLUMNS]
    frame[columns].to_csv(buffer, index=False, header=False, date_format="%Y-%m-%d %H:%M:%S")
    buffer.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY labeled_transactions ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV)",
            buffer,
        )
