"""
Spark Structured Streaming job: real-time fraud detection.

Rules implemented:
    1. HIGH_VALUE              (stateless): order > FRAUD_THRESHOLD
    2. RAPID_PURCHASES         (stateful):  ≥3 orders from same user / 60s
    3. GEOGRAPHIC_IMPOSSIBILITY (stateful):
       same user in 2+ cities > 500 km apart, within a 60s window

All rules fan out to:
    - PostgreSQL fraud_events  (historical record, idempotent UPSERT)
    - Kafka fraud-alerts       (real-time consumers)

Run:
    python -m spark.streaming.fraud_detector
"""

from __future__ import annotations

import logging
import math
import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array_max,
    col,
    collect_list,
    count,
    current_timestamp,
    expr,
    first,
    format_string,
    from_json,
    lit,
    max as max_,
    min as min_,
    struct,
    udf,
    window,
)
from pyspark.sql.types import DoubleType

from spark.streaming.fraud_writer import write_fraud_events
from spark.streaming.kafka_writer import write_fraud_alerts
from spark.streaming.schemas import TRANSACTION_SCHEMA
from spark.streaming.spark_session import create_spark_session

load_dotenv()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions")
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "1000"))

# Rule 2
RAPID_PURCHASE_WINDOW = "1 minute"
RAPID_PURCHASE_THRESHOLD = 3

# Rule 3
GEO_WINDOW = "1 minute"
GEO_DISTANCE_THRESHOLD_KM = 500.0   # >500 km in <1 min = impossible

STARTING_OFFSETS = "latest"
TRIGGER_INTERVAL = "10 seconds"
# 30 seconds is aggressive — tuned for fast feedback while developing.
# Production would set this based on observed event lag (typically 1-5 min).
WATERMARK_DELAY = "5 minutes"

CHECKPOINT_ROOT = os.getenv("SPARK_CHECKPOINT_DIR", "data/checkpoints")
CHECKPOINT_HIGH_VALUE = f"{CHECKPOINT_ROOT}/fraud_high_value"
CHECKPOINT_RAPID      = f"{CHECKPOINT_ROOT}/fraud_rapid_purchases"
CHECKPOINT_GEO        = f"{CHECKPOINT_ROOT}/fraud_geographic"


# ---------------------------------------------------------------------
# Haversine distance UDF
# ---------------------------------------------------------------------
def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two lat/long points in km."""
    if None in (lat1, lon1, lat2, lon2):
        return 0.0
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _max_pairwise_distance(coords: list) -> float:
    """
    Given a list of [lat, lon] pairs, return the maximum pairwise distance.
    For N points the brute-force comparison is O(N²) but N is tiny here
    (the count of cities for one user in one minute).
    """
    if not coords or len(coords) < 2:
        return 0.0
    max_d = 0.0
    for i in range(len(coords)):
        for j in range(i + 1, len(coords)):
            d = _haversine_km(coords[i][0], coords[i][1],
                              coords[j][0], coords[j][1])
            if d > max_d:
                max_d = d
    return float(max_d)


max_pairwise_distance_udf = udf(_max_pairwise_distance, DoubleType())


# ---------------------------------------------------------------------
# Rule 1: high-value detection (stateless)
# ---------------------------------------------------------------------
def apply_high_value_rule(transactions: DataFrame) -> DataFrame:
    """Flag any transaction whose total_amount exceeds FRAUD_THRESHOLD."""
    return (
        transactions
        .filter(col("total_amount") > FRAUD_THRESHOLD)
        .withColumn("alert_id",   expr("uuid()"))
        .withColumn("alert_type", lit("high_value"))
        .withColumn(
            "fraud_score",
            expr(
                f"least(1.0, 0.5 + (total_amount - {FRAUD_THRESHOLD}) "
                f"/ ({FRAUD_THRESHOLD} * 4) * 0.5)"
            ),
        )
        .withColumn(
            "reason",
            format_string(
                "High-value order: $%.2f exceeds threshold $%.2f",
                col("total_amount"),
                lit(FRAUD_THRESHOLD),
            ),
        )
        .withColumn("amount",      col("total_amount"))
        .withColumn("detected_at", current_timestamp())
        .select(
            "alert_id", "alert_type", "fraud_score", "reason",
            "user_id", "transaction_id", "amount",
            "location", "detected_at",
        )
    )


# ---------------------------------------------------------------------
# Rule 2: rapid purchases (stateful, windowed)
# ---------------------------------------------------------------------
def apply_rapid_purchase_rule(transactions: DataFrame) -> DataFrame:
    """Flag users with ≥RAPID_PURCHASE_THRESHOLD orders in 60s."""
    aggregated = (
        transactions
        .groupBy(
            window(col("timestamp"), RAPID_PURCHASE_WINDOW).alias("w"),
            col("user_id"),
        )
        .agg(
            count("transaction_id").alias("order_count"),
            first("transaction_id").alias("transaction_id"),
            first("total_amount").alias("amount"),
            first("location").alias("location"),
        )
        .filter(col("order_count") >= RAPID_PURCHASE_THRESHOLD)
    )

    return (
        aggregated
        .withColumn("alert_id",   expr("uuid()"))
        .withColumn("alert_type", lit("rapid_purchases"))
        .withColumn(
            "fraud_score",
            expr("least(1.0, 0.4 + order_count * 0.08)"),
        )
        .withColumn(
            "reason",
            format_string(
                "Rapid purchases: %d orders by user %d in 60s window starting %s",
                col("order_count").cast("int"),
                col("user_id").cast("int"),
                col("w.start").cast("string"),
            ),
        )
        .withColumn("detected_at", current_timestamp())
        .select(
            "alert_id", "alert_type", "fraud_score", "reason",
            "user_id", "transaction_id", "amount",
            "location", "detected_at",
        )
    )


# ---------------------------------------------------------------------
# Rule 3: geographic impossibility (stateful, windowed + Haversine UDF)
# ---------------------------------------------------------------------
def apply_geographic_impossibility_rule(transactions: DataFrame) -> DataFrame:
    """
    Flag users whose orders span 2+ cities > GEO_DISTANCE_THRESHOLD_KM
    apart within a single window.

    Approach:
      1. Group by (window, user_id)
      2. Collect all distinct (latitude, longitude) pairs the user touched
      3. Compute max pairwise Haversine distance via UDF
      4. Filter to users whose max distance exceeds the threshold
    """
    aggregated = (
        transactions
        .groupBy(
            window(col("timestamp"), GEO_WINDOW).alias("w"),
            col("user_id"),
        )
        .agg(
            # collect_list of [lat, lon] arrays — duplicates are fine; UDF handles it
            collect_list(
                expr("array(location.latitude, location.longitude)")
            ).alias("coords"),
            first("transaction_id").alias("transaction_id"),
            first("total_amount").alias("amount"),
            first("location").alias("location"),
        )
        .withColumn(
            "max_distance_km",
            max_pairwise_distance_udf(col("coords")),
        )
        .filter(col("max_distance_km") > GEO_DISTANCE_THRESHOLD_KM)
    )

    return (
        aggregated
        .withColumn("alert_id",   expr("uuid()"))
        .withColumn("alert_type", lit("geographic_impossibility"))
        # Score scales with distance: 500 km → 0.55, 5000 km → 0.95
        .withColumn(
            "fraud_score",
            expr(
                "least(1.0, 0.5 + max_distance_km / 10000.0 * 0.5)"
            ),
        )
        .withColumn(
            "reason",
            format_string(
                "Geographic impossibility: user %d in cities %.0f km apart within 60s",
                col("user_id").cast("int"),
                col("max_distance_km"),
            ),
        )
        .withColumn("detected_at", current_timestamp())
        .select(
            "alert_id", "alert_type", "fraud_score", "reason",
            "user_id", "transaction_id", "amount",
            "location", "detected_at",
        )
    )


# ---------------------------------------------------------------------
# Combined sink — Postgres + Kafka in one foreachBatch
# ---------------------------------------------------------------------
def write_alerts_fanout(batch_df: DataFrame, batch_id: int) -> None:
    """Single foreachBatch fans out to both sinks; persist() avoids recomputation."""
    if batch_df.rdd.isEmpty():
        logger.debug("batch_id=%d: no fraud alerts", batch_id)
        return

    batch_df.persist()
    try:
        write_fraud_events(batch_df, batch_id)
        write_fraud_alerts(batch_df, batch_id)
    finally:
        batch_df.unpersist()


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    spark = create_spark_session(app_name="fraud-detector")

    logger.info(
        "Reading stream (threshold=$%.2f, rapid≥%d/%s, geo>%.0fkm/%s)",
        FRAUD_THRESHOLD,
        RAPID_PURCHASE_THRESHOLD, RAPID_PURCHASE_WINDOW,
        GEO_DISTANCE_THRESHOLD_KM, GEO_WINDOW,
    )

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    transactions = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_str")
        .withColumn("data", from_json(col("json_str"), TRANSACTION_SCHEMA))
        .select("data.*")
        .withWatermark("timestamp", WATERMARK_DELAY)
    )

    high_value_alerts = apply_high_value_rule(transactions)
    rapid_alerts      = apply_rapid_purchase_rule(transactions)
    geo_alerts        = apply_geographic_impossibility_rule(transactions)

    logger.info("Starting fraud-detection sinks (Ctrl+C to stop)")

    q_high = (
        high_value_alerts.writeStream
        .queryName("fraud_high_value")
        .foreachBatch(write_alerts_fanout)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_HIGH_VALUE)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    q_rapid = (
        rapid_alerts.writeStream
        .queryName("fraud_rapid_purchases")
        .foreachBatch(write_alerts_fanout)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_RAPID)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    q_geo = (
        geo_alerts.writeStream
        .queryName("fraud_geographic")
        .foreachBatch(write_alerts_fanout)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_GEO)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
