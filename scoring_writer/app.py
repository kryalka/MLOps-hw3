import json
import logging
import os

import psycopg2
from confluent_kafka import Consumer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

SCORES_TOPIC = os.getenv(
    "KAFKA_SCORES_TOPIC",
    "scores",
)


def get_db_config():
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "database": os.getenv("POSTGRES_DB", "fraud_db"),
        "user": os.getenv("POSTGRES_USER", "fraud_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "fraud_password"),
    }


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scores (
                id SERIAL PRIMARY KEY,
                transaction_id TEXT NOT NULL UNIQUE,
                score DOUBLE PRECISION NOT NULL,
                fraud_flag INTEGER NOT NULL,
                model_version TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

    logger.info("Table 'scores' checked or created.")



def insert_score(conn, data):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO scores (
                transaction_id,
                score,
                fraud_flag,
                model_version
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (transaction_id)
            DO UPDATE SET
                score = EXCLUDED.score,
                fraud_flag = EXCLUDED.fraud_flag,
                model_version = EXCLUDED.model_version,
                created_at = CURRENT_TIMESTAMP;
            """,
            (
                data["transaction_id"],
                data["score"],
                data["fraud_flag"],
                data["model_version"],
            ),
        )

    conn.commit()


def run_consumer():
    consumer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "scoring-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    logger.info(
        "Connecting to Kafka: %s, topic: %s",
        KAFKA_BOOTSTRAP_SERVERS,
        SCORES_TOPIC,
    )

    consumer = Consumer(consumer_config)
    consumer.subscribe([SCORES_TOPIC])

    db_config = get_db_config()

    logger.info(
        "Connecting to PostgreSQL: %s:%s/%s",
        db_config["host"],
        db_config["port"],
        db_config["database"],
    )

    conn = psycopg2.connect(**db_config)
    create_table(conn)

    logger.info("Scoring writer started.")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                logger.error(
                    "Kafka error: %s",
                    msg.error(),
                )
                raise RuntimeError("Kafka consumer error")

            try:
                data = json.loads(
                    msg.value().decode("utf-8")
                )

                if not isinstance(data, dict):
                    raise ValueError("Score message must be a JSON object")

                required_keys = {
                    "transaction_id",
                    "score",
                    "fraud_flag",
                    "model_version",
                }

                if not required_keys.issubset(data):
                    raise ValueError("Invalid score message")

                insert_score(conn, data)

                committed_offsets = consumer.commit(
                    message=msg,
                    asynchronous=False,
                )
                if not committed_offsets or any(offset.error is not None for offset in committed_offsets):
                    raise RuntimeError("Kafka offset commit failed")

                logger.info(
                    "Saved transaction %s to PostgreSQL",
                    data["transaction_id"],
                )

            except json.JSONDecodeError as e:
                logger.exception(
                    "JSON decoding error: %s",
                    e,
                )
                raise

            except Exception as e:
                conn.rollback()
                logger.exception(f"Ошибка обработки сообщения: {e}")
                raise

    except KeyboardInterrupt:
        logger.info(
            "Scoring writer stopped by user."
        )

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    logger.info(
        "Starting scoring writer..."
    )

    run_consumer()
