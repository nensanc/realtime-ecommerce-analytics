"""
Real-time e-commerce transaction generator.

Emits a continuous stream of realistic order events to the Kafka
`transactions` topic. The catalog and user pool are built once at
startup (deterministic via seed); the stream of transactions
referencing them is what flows into Kafka.

Usage:
    python -m streaming.producers.transaction_generator

Environment variables (from .env):
    KAFKA_BOOTSTRAP_SERVERS   default: localhost:9092
    KAFKA_TOPIC_TRANSACTIONS  default: transactions
    TRANSACTION_RATE          transactions per second (default: 10)
    NUM_PRODUCTS              catalog size (default: 100)
    NUM_USERS                 user pool size (default: 1000)
"""

from __future__ import annotations

import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv

from streaming.producers.catalog import (
    Product,
    User,
    build_product_catalog,
    build_user_pool,
)
from streaming.producers.config import (
    create_producer,
    on_send_error,
    on_send_success,
)

load_dotenv()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Tunables (with .env overrides)
# ---------------------------------------------------------------------
TOPIC = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions")
RATE = int(os.getenv("TRANSACTION_RATE", "10"))
NUM_PRODUCTS = int(os.getenv("NUM_PRODUCTS", "100"))
NUM_USERS = int(os.getenv("NUM_USERS", "1000"))

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "cash"]
PAYMENT_WEIGHTS = [0.55,         0.25,          0.15,    0.05]   # sum=1.0

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.65,    0.30,      0.05]                       # sum=1.0

# Quantity distribution: most orders are 1-2 items
QUANTITY_CHOICES = [1, 1, 1, 1, 2, 2, 2, 3, 4, 5]


# ---------------------------------------------------------------------
# Transaction builder
# ---------------------------------------------------------------------
def build_transaction(user: User, product: Product) -> dict[str, Any]:
    """Build a single transaction event matching the target schema."""
    quantity = random.choice(QUANTITY_CHOICES)
    total = round(product.unit_price * quantity, 2)

    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user.user_id,
        "user_name": user.user_name,
        "product_id": product.product_id,
        "product_name": product.product_name,
        "category": product.category,
        "quantity": quantity,
        "unit_price": product.unit_price,
        "total_amount": total,
        "payment_method": random.choices(PAYMENT_METHODS, PAYMENT_WEIGHTS)[0],
        "location": user.location.to_dict(),
        "session_id": f"sess_{uuid.uuid4().hex[:8]}",
        "device_type": random.choices(DEVICE_TYPES, DEVICE_WEIGHTS)[0],
    }


# ---------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------
class TransactionGenerator:
    """Encapsulates the producer + catalog + run loop with clean shutdown."""

    def __init__(self, rate: int, topic: str) -> None:
        self.rate = rate
        self.topic = topic
        self.interval = 1.0 / rate if rate > 0 else 0.1
        self.sent = 0
        self.errors = 0
        self.running = True

        logger.info("Building catalog (%d products)...", NUM_PRODUCTS)
        self.products = build_product_catalog(n=NUM_PRODUCTS)

        logger.info("Building user pool (%d users)...", NUM_USERS)
        self.users = build_user_pool(n=NUM_USERS)

        logger.info("Connecting to Kafka...")
        self.producer = create_producer()

    def _on_success(self, record_metadata) -> None:
        self.sent += 1
        on_send_success(record_metadata)

    def _on_error(self, exc: Exception) -> None:
        self.errors += 1
        on_send_error(exc)

    def stop(self, *_: Any) -> None:
        """Signal handler for Ctrl+C — let the loop exit cleanly."""
        logger.info("Shutdown signal received, draining producer...")
        self.running = False

    def run(self) -> None:
        """Main producer loop. Runs until self.running is False."""
        logger.info(
            "▶ Generating transactions → topic='%s' rate=%d TPS (Ctrl+C to stop)",
            self.topic, self.rate,
        )
        start = time.monotonic()
        next_log_at = 50  # log progress every N messages

        while self.running:
            tick_start = time.monotonic()

            user = random.choice(self.users)
            product = random.choice(self.products)
            event = build_transaction(user, product)

            future = self.producer.send(
                self.topic,
                key=user.user_id,
                value=event,
            )
            future.add_callback(self._on_success)
            future.add_errback(self._on_error)

            # Periodic progress
            if self.sent >= next_log_at:
                elapsed = time.monotonic() - start
                actual_tps = self.sent / elapsed if elapsed > 0 else 0
                logger.info(
                    "Sent=%d errors=%d elapsed=%.1fs actual_tps=%.1f",
                    self.sent, self.errors, elapsed, actual_tps,
                )
                next_log_at += 50

            # Throttle to target rate
            sleep_for = self.interval - (time.monotonic() - tick_start)
            if sleep_for > 0:
                time.sleep(sleep_for)

        self._shutdown()

    def _shutdown(self) -> None:
        """Flush in-flight messages and close cleanly."""
        logger.info("Flushing pending messages...")
        self.producer.flush(timeout=10)
        self.producer.close(timeout=5)
        logger.info(
            "✓ Stopped. Total sent=%d, errors=%d",
            self.sent, self.errors,
        )


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------
def main() -> int:
    gen = TransactionGenerator(rate=RATE, topic=TOPIC)
    signal.signal(signal.SIGINT, gen.stop)
    signal.signal(signal.SIGTERM, gen.stop)
    try:
        gen.run()
    except Exception:
        logger.exception("Unexpected error in generator loop")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
