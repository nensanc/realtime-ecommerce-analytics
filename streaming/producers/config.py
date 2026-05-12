"""
Kafka Producer configuration and factory.

This module is the single source of truth for how producers
connect to Kafka and how messages get serialized. Other modules
(transaction_generator.py, alert producers, etc.) import
`create_producer()` instead of duplicating boilerplate.

Usage:
    from kafka.producers.config import create_producer

    producer = create_producer()
    producer.send("transactions", key="user_42", value={"foo": "bar"})
    producer.flush()
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv()

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Quiet the chatty kafka-python-ng internal logs — keep only WARNING+
logging.getLogger("kafka").setLevel(logging.WARNING)



# ---------------------------------------------------------------------
# Config loaders
# ---------------------------------------------------------------------
def get_kafka_config() -> dict[str, Any]:
    """
    Read Kafka-related settings from environment variables.

    Returns a flat dict that can be unpacked into KafkaProducer().
    Centralizing this prevents config drift across producer scripts.
    """
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    return {
        "bootstrap_servers": bootstrap.split(","),
        # --- Durability & correctness ----------------------------------
        # NOTE: kafka-python-ng does not support `enable_idempotence` or
        # `delivery_timeout_ms` (Java/librdkafka-only features). For this
        # portfolio project the practical impact is negligible at our
        # throughput. If exactly-once semantics become a hard requirement,
        # migrate to `confluent-kafka-python` (wraps librdkafka in C).
        "acks": "all",                # wait for full ISR acknowledgment
        "retries": 5,
        "max_in_flight_requests_per_connection": 5,
        # --- Throughput tuning -----------------------------------------
        "linger_ms": 10,              # micro-batch window
        "batch_size": 32 * 1024,      # 32 KB per partition batch
        "compression_type": "gzip",
        # --- Timeouts --------------------------------------------------
        "request_timeout_ms": 30_000,
        # --- Client identity (shows up in broker logs / Kafka UI) -----
        "client_id": "ecommerce-analytics-producer",
    }


# ---------------------------------------------------------------------
# Serializers
# ---------------------------------------------------------------------
def _json_serializer(value: Any) -> bytes:
    """Serialize a Python object to UTF-8 JSON bytes for Kafka."""
    return json.dumps(value, default=str).encode("utf-8")


def _string_key_serializer(key: Optional[Any]) -> Optional[bytes]:
    """Serialize the message key (e.g. user_id) to UTF-8 bytes."""
    if key is None:
        return None
    return str(key).encode("utf-8")


# ---------------------------------------------------------------------
# Send callbacks (async result handlers)
# ---------------------------------------------------------------------
def on_send_success(record_metadata) -> None:
    """Callback invoked after a successful async send."""
    logger.debug(
        "Delivered → topic=%s partition=%d offset=%d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(exc: Exception) -> None:
    """Callback invoked when an async send fails permanently."""
    logger.error("Failed to deliver message: %s", exc, exc_info=True)


# ---------------------------------------------------------------------
# Producer factory
# ---------------------------------------------------------------------
def create_producer() -> KafkaProducer:
    """
    Build and return a configured KafkaProducer.

    Raises:
        KafkaError: if the producer cannot connect to the broker.
    """
    config = get_kafka_config()
    logger.info(
        "Connecting to Kafka at %s (client_id=%s)",
        config["bootstrap_servers"],
        config["client_id"],
    )

    try:
        producer = KafkaProducer(
            value_serializer=_json_serializer,
            key_serializer=_string_key_serializer,
            **config,
        )
    except KafkaError as e:
        logger.error("Could not create Kafka producer: %s", e)
        raise

    logger.info("✓ Kafka producer ready")
    return producer


# ---------------------------------------------------------------------
# Self-test: run `python -m kafka.producers.config` to sanity-check
# ---------------------------------------------------------------------
if __name__ == "__main__":
    import time
    from datetime import datetime, timezone

    logger.info("Running producer self-test...")

    producer = create_producer()
    topic = "smoke-test"

    for i in range(3):
        msg = {
            "test_id": i,
            "source": "config.py self-test",
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        future = producer.send(topic, key=f"test-{i}", value=msg)
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)
        logger.info("Queued message %d", i)
        time.sleep(0.1)

    producer.flush()
    producer.close()
    logger.info("✓ Self-test complete — sent 3 messages to '%s'", topic)
