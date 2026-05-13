"""
Schema definitions for streaming data sources.

Why explicit schemas:
    Spark Structured Streaming cannot infer schemas from Kafka
    (the source is unbounded — there's nothing to "sample" up front).
    Every streaming JSON parse must be given an explicit schema.

These schemas are also the single source of truth for the shape
of our event data. If the producer's JSON shape changes, this
file MUST change too.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------
# Nested: location
# ---------------------------------------------------------------------
LOCATION_SCHEMA = StructType([
    StructField("country",   StringType(), nullable=False),
    StructField("city",      StringType(), nullable=False),
    StructField("latitude",  DoubleType(), nullable=True),
    StructField("longitude", DoubleType(), nullable=True),
])


# ---------------------------------------------------------------------
# Top-level: transaction event
# ---------------------------------------------------------------------
# IMPORTANT: field order and names must match the JSON emitted by
# streaming/producers/transaction_generator.py:build_transaction().
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(),    nullable=False),
    StructField("timestamp",      TimestampType(), nullable=False),
    StructField("user_id",        IntegerType(),   nullable=False),
    StructField("user_name",      StringType(),    nullable=True),
    StructField("product_id",     IntegerType(),   nullable=False),
    StructField("product_name",   StringType(),    nullable=True),
    StructField("category",       StringType(),    nullable=False),
    StructField("quantity",       IntegerType(),   nullable=False),
    StructField("unit_price",     DoubleType(),    nullable=False),
    StructField("total_amount",   DoubleType(),    nullable=False),
    StructField("payment_method", StringType(),    nullable=False),
    StructField("location",       LOCATION_SCHEMA, nullable=True),
    StructField("session_id",     StringType(),    nullable=True),
    StructField("device_type",    StringType(),    nullable=True),
])


# ---------------------------------------------------------------------
# Self-test: print the schema tree
# ---------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 60)
    print("TRANSACTION SCHEMA")
    print("=" * 60)
    print(TRANSACTION_SCHEMA.simpleString())
    print()

    # Use an empty DataFrame to get the tree-formatted schema view.
    # StructType itself has no tree-printer in PySpark (only Scala).
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("schema-self-test")
        .master("local[1]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.createDataFrame([], TRANSACTION_SCHEMA).printSchema()
    spark.stop()

    print(f"Total fields: {len(TRANSACTION_SCHEMA.fields)}")
