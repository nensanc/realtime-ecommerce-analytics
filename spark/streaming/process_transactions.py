"""
Spark Structured Streaming job: process e-commerce transactions.

Pipeline:
    Kafka → JSON parse → 1-min windowed aggregations → PostgreSQL UPSERT

Two parallel queries:
    1. Overall sales metrics per window (category=NULL in the table)
    2. Per-category breakdown (category populated)

Both write to the same `sales_metrics` table, distinguished by the
`category` column. The unique constraint (window_start, category)
NULLS NOT DISTINCT makes UPSERT idempotent on replay.

Run:
    python -m spark.streaming.process_transactions

Stop with Ctrl+C.
"""

from __future__ import annotations

import logging
import os

from dotenv import load_dotenv
from pyspark.sql.functions import (
    approx_count_distinct,
    avg,
    col,
    count,
    from_json,
    sum as sum_,
    window,
)

from spark.streaming.postgres_writer import (
    write_category_metrics,
    write_overall_metrics,
)
from spark.streaming.schemas import TRANSACTION_SCHEMA
from spark.streaming.spark_session import create_spark_session

load_dotenv()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions")
STARTING_OFFSETS = "latest"

# Window semantics
WINDOW_DURATION = "1 minute"
WATERMARK_DELAY = "5 minutes"
TRIGGER_INTERVAL = "10 seconds"

# Checkpoint paths (one per query — never share!)
CHECKPOINT_ROOT = os.getenv("SPARK_CHECKPOINT_DIR", "data/checkpoints")
CHECKPOINT_OVERALL = f"{CHECKPOINT_ROOT}/sales_metrics_overall"
CHECKPOINT_CATEGORY = f"{CHECKPOINT_ROOT}/sales_metrics_by_category"


def main() -> None:
    spark = create_spark_session(app_name="process-transactions")

    # ----------------------------------------------------------------
    # Read Kafka and parse JSON (Layers 1-3, same as before)
    # ----------------------------------------------------------------
    logger.info(
        "Reading stream from topic '%s' (bootstrap=%s, offsets=%s)",
        KAFKA_TOPIC, KAFKA_BOOTSTRAP, STARTING_OFFSETS,
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

    parsed = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_str")
        .withColumn("data", from_json(col("json_str"), TRANSACTION_SCHEMA))
        .select("data.*")
        .withWatermark("timestamp", WATERMARK_DELAY)
    )

    # ----------------------------------------------------------------
    # Aggregation 1: overall sales metrics per window
    # ----------------------------------------------------------------
    sales_overall = (
        parsed
        .groupBy(window(col("timestamp"), WINDOW_DURATION).alias("w"))
        .agg(
            sum_("total_amount").alias("total_sales"),
            count("transaction_id").alias("order_count"),
            avg("total_amount").alias("avg_order_value"),
            approx_count_distinct("user_id").alias("unique_customers"),
        )
        .select(
            col("w.start").alias("window_start"),
            col("w.end").alias("window_end"),
            col("total_sales"),
            col("order_count"),
            col("avg_order_value"),
            col("unique_customers"),
        )
    )

    # ----------------------------------------------------------------
    # Aggregation 2: per-category breakdown
    # ----------------------------------------------------------------
    sales_by_category = (
        parsed
        .groupBy(
            window(col("timestamp"), WINDOW_DURATION).alias("w"),
            col("category"),
        )
        .agg(
            sum_("total_amount").alias("category_sales"),
            count("transaction_id").alias("category_orders"),
        )
        .select(
            col("w.start").alias("window_start"),
            col("category"),
            col("category_sales"),
            col("category_orders"),
        )
    )

    # ----------------------------------------------------------------
    # Postgres sinks — foreachBatch with UPSERT
    # ----------------------------------------------------------------
    logger.info("Starting Postgres sinks (Ctrl+C to stop)")
    logger.info("  Checkpoint overall : %s", CHECKPOINT_OVERALL)
    logger.info("  Checkpoint category: %s", CHECKPOINT_CATEGORY)

    q_overall = (
        sales_overall.writeStream
        .queryName("sales_metrics_overall")
        .foreachBatch(write_overall_metrics)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_OVERALL)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    q_category = (
        sales_by_category.writeStream
        .queryName("sales_metrics_by_category")
        .foreachBatch(write_category_metrics)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_CATEGORY)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
