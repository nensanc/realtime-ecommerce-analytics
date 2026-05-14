"""
Kafka alert writer for fraud detection.

Publishes detected fraud events to the `fraud-alerts` Kafka topic so
downstream consumers (dashboards, external alerting, audit pipelines)
can react in real time.

Design notes:
    - Spark has a native Kafka sink, but for fraud alerts we want full
      control over the payload shape, so we use foreachBatch + the
      DataFrame's writer with a manual value column.
    - Alerts include EVERYTHING the consumer needs — no joins required.
"""

from __future__ import annotations

import logging
import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, struct, to_json

load_dotenv()
logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FRAUD_ALERTS_TOPIC = os.getenv("KAFKA_TOPIC_FRAUD_ALERTS", "fraud-alerts")


def write_fraud_alerts(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch writer: publish fraud-detection rows to Kafka.

    Expects `batch_df` to have the following columns (the schema produced
    by the fraud detector):
        alert_id, alert_type, fraud_score, reason, user_id,
        transaction_id, amount, location (struct), detected_at

    Each row becomes one Kafka message:
        key   = user_id (string)
        value = JSON-encoded row
    """
    if batch_df.rdd.isEmpty():
        logger.debug("batch_id=%d (fraud-alerts): no rows", batch_id)
        return

    # Build the Kafka-compatible DataFrame: (key STRING, value STRING)
    payload = batch_df.select(
        col("user_id").cast("string").alias("key"),
        # to_json over a struct → serialize the entire row as one JSON object
        to_json(struct(
            "alert_id",
            "alert_type",
            "fraud_score",
            "reason",
            "user_id",
            "transaction_id",
            "amount",
            "location",
            "detected_at",
        )).alias("value"),
    )

    # Write as a normal batch DataFrame to Kafka (not a streaming write —
    # we're inside foreachBatch, which gives us a batch DataFrame).
    (
        payload.write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", FRAUD_ALERTS_TOPIC)
        .save()
    )

    count = batch_df.count()
    logger.info(
        "batch_id=%d (fraud-alerts): published %d alert(s) to topic '%s'",
        batch_id, count, FRAUD_ALERTS_TOPIC,
    )


# ---------------------------------------------------------------------
# Self-test: requires a Spark session, so we mock one
# ---------------------------------------------------------------------
if __name__ == "__main__":
    import uuid
    from datetime import datetime, timezone

    from spark.streaming.spark_session import create_spark_session

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info("Running kafka_writer self-test...")
    spark = create_spark_session(app_name="kafka-writer-test")

    # Build a fake fraud-alert DataFrame to publish
    now = datetime.now(timezone.utc)
    test_alerts = spark.createDataFrame(
        [
            (
                str(uuid.uuid4()),
                "high_value",
                0.85,
                "Transaction amount $5,420.00 exceeds threshold of $1,000",
                12345,
                str(uuid.uuid4()),
                5420.0,
                ("Colombia", "Bogotá", 4.711, -74.0721),
                now,
            )
        ],
        schema="""
            alert_id        string,
            alert_type      string,
            fraud_score     double,
            reason          string,
            user_id         int,
            transaction_id  string,
            amount          double,
            location        struct<country:string,city:string,latitude:double,longitude:double>,
            detected_at     timestamp
        """,
    )

    write_fraud_alerts(test_alerts, batch_id=0)
    logger.info("✓ Self-test complete — check Kafka UI for the message")

    spark.stop()
