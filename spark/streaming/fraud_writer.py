"""
PostgreSQL writer for fraud events.

Sibling to postgres_writer.py (which writes sales_metrics).
Both use the same psycopg2 + execute_values + ON CONFLICT pattern.

The `fraud_events` table has a UNIQUE (transaction_id, alert_type)
constraint, so the same transaction can be flagged by multiple
distinct rules but not duplicated by the same rule on replay.
"""

from __future__ import annotations

import logging

from psycopg2.extras import execute_values
from pyspark.sql import DataFrame

from spark.streaming.postgres_writer import pg_connection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# SQL — idempotent UPSERT
# ---------------------------------------------------------------------
# Note: we use DO UPDATE rather than DO NOTHING so that the latest
# fraud_score / reason wins if the same (tx, type) is re-flagged
# (which shouldn't happen, but defensive).
UPSERT_SQL = """
INSERT INTO fraud_events
    (transaction_id, alert_type, user_id, amount, fraud_score,
     reason, location_city, location_country)
VALUES %s
ON CONFLICT ON CONSTRAINT uq_fraud_events_tx_type
DO UPDATE SET
    fraud_score      = EXCLUDED.fraud_score,
    reason           = EXCLUDED.reason,
    amount           = EXCLUDED.amount,
    location_city    = EXCLUDED.location_city,
    location_country = EXCLUDED.location_country,
    detected_at      = CURRENT_TIMESTAMP
"""


def write_fraud_events(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch writer for the fraud_events table.

    Expects `batch_df` with columns:
        transaction_id, alert_type, user_id, amount, fraud_score,
        reason, location (struct with city, country)
    """
    rows = batch_df.collect()
    if not rows:
        logger.debug("batch_id=%d (fraud_events): no rows", batch_id)
        return

    payload = [
        (
            r["transaction_id"],
            r["alert_type"],
            int(r["user_id"]),
            float(r["amount"]),
            float(r["fraud_score"]),
            r["reason"],
            r["location"]["city"]    if r["location"] else None,
            r["location"]["country"] if r["location"] else None,
        )
        for r in rows
    ]

    with pg_connection() as conn, conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, payload)

    logger.info(
        "batch_id=%d (fraud_events): persisted %d row(s)", batch_id, len(payload),
    )


# ---------------------------------------------------------------------
# Self-test: insert one fake fraud event
# ---------------------------------------------------------------------
if __name__ == "__main__":
    import uuid
    from datetime import datetime, timezone

    from spark.streaming.spark_session import create_spark_session

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info("Running fraud_writer self-test...")
    spark = create_spark_session(app_name="fraud-writer-test")

    test_events = spark.createDataFrame(
        [
            (
                str(uuid.uuid4()),
                "high_value",
                12345,
                5420.0,
                0.85,
                "Self-test: amount $5,420.00 exceeds threshold of $1,000",
                ("Colombia", "Bogotá", 4.711, -74.0721),
                datetime.now(timezone.utc),
            )
        ],
        schema="""
            transaction_id  string,
            alert_type      string,
            user_id         int,
            amount          double,
            fraud_score     double,
            reason          string,
            location        struct<country:string,city:string,latitude:double,longitude:double>,
            detected_at     timestamp
        """,
    )

    write_fraud_events(test_events, batch_id=0)
    logger.info("✓ Self-test complete — check sales table:")
    logger.info("  docker exec -it postgres psql -U ecommerce_user -d ecommerce -c 'SELECT * FROM fraud_events;'")

    spark.stop()
