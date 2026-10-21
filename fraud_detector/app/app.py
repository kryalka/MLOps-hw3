import json
import logging
import os

import pandas as pd
from confluent_kafka import Consumer, Producer

from fraud_detector.src.preprocessing import run_preproc
from fraud_detector.src.scorer import make_pred


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")


class ProcessingService:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        self.producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer(self.producer_config)

        logger.info(
            "Kafka service initialized. Input topic: %s, output topic: %s",
            TRANSACTIONS_TOPIC,
            SCORES_TOPIC
        )


    def process_message(self, msg):
        try:
            data = json.loads(msg.value().decode('utf-8'))

            transaction_id = data['transaction_id']
            input_df = pd.DataFrame([data['data']])

            processed_df = run_preproc(input_df)
            prediction, _ = make_pred(processed_df, "kafka_stream")

            score = float(prediction["score"].iloc[0])
            fraud_flag = int(prediction["fraud_flag"].iloc[0])

            result = {
                "transaction_id": transaction_id,
                "score": score,
                "fraud_flag": fraud_flag,
                "model_version": str(prediction["model_version"].iloc[0]),
            }

            delivery_status = {"confirmed": False, "error": None}

            def on_delivery(error, _message):
                delivery_status["confirmed"] = True
                delivery_status["error"] = error

            self.producer.produce(
                SCORES_TOPIC,
                value=json.dumps(result).encode("utf-8"),
                on_delivery=on_delivery,
            )
            remaining_messages = self.producer.flush()

            if (
                remaining_messages != 0
                or not delivery_status["confirmed"]
                or delivery_status["error"] is not None
            ):
                raise RuntimeError(
                    f"Kafka delivery was not confirmed: {delivery_status['error']}"
                )

            committed_offsets = self.consumer.commit(
                message=msg,
                asynchronous=False,
            )
            if not committed_offsets or any(offset.error is not None for offset in committed_offsets):
                raise RuntimeError("Kafka offset commit failed")

            logger.info(
                "Transaction %s processed: score=%.6f, fraud_flag=%s",
                transaction_id,
                score,
                fraud_flag,
            )

            return True

        except Exception as e:
            logger.exception(f"Error processing message: {e}")
            return False

    def process_messages(self):
        logger.info(
            "Listening to Kafka topic: %s",
            TRANSACTIONS_TOPIC,
        )

        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                raise RuntimeError("Kafka consumer error")

            if not self.process_message(msg):
                logger.error("Message processing failed; stopping without offset commit")
                raise RuntimeError("Message processing failed")

    def close(self):
        logger.info("Closing Kafka consumer...")
        self.producer.flush()
        self.consumer.close()


if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
    finally:
        service.close()
