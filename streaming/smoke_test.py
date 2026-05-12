"""
Kafka smoke test.

Goal: verify Python ↔ Kafka connectivity end-to-end.
Sends a single 'hello world' message to the smoke-test topic
and immediately consumes it back.

Usage:
    python kafka/smoke_test.py produce   # send 1 message
    python kafka/smoke_test.py consume   # read messages from beginning
"""

import json
import sys
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "smoke-test"


def produce_one() -> None:
    """Send a single JSON message to Kafka and confirm delivery."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    message = {
        "msg": "hello kafka",
        "from": "python smoke test",
        "ts": datetime.now(timezone.utc).isoformat(),
    }

    print(f"→ Sending to topic '{TOPIC}': {message}")
    future = producer.send(TOPIC, value=message)

    # Block until the broker acknowledges, so we *know* it arrived.
    metadata = future.get(timeout=10)
    print(
        f"✓ Delivered to partition {metadata.partition} "
        f"at offset {metadata.offset}"
    )

    producer.flush()
    producer.close()


def consume_all() -> None:
    """Read every message in the topic from the beginning, then exit."""
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,   # exit after 5s of silence
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="smoke-test-reader",
    )

    print(f"← Reading from topic '{TOPIC}' (5s timeout)...")
    count = 0
    for msg in consumer:
        count += 1
        print(
            f"  [partition={msg.partition} offset={msg.offset}] "
            f"{msg.value}"
        )

    print(f"✓ Read {count} message(s). Done.")
    consumer.close()


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in {"produce", "consume"}:
        print("Usage: python kafka/smoke_test.py [produce|consume]")
        sys.exit(1)

    if sys.argv[1] == "produce":
        produce_one()
    else:
        consume_all()
