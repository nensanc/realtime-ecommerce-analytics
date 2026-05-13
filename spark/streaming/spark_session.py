"""
Spark Session factory for streaming jobs.

Centralizes Spark configuration so every streaming job in this
project starts from the same well-tuned base. Other modules
(process_transactions.py, fraud_detector.py) import
`create_spark_session()` instead of duplicating boilerplate.

Usage:
    from spark.streaming.spark_session import create_spark_session

    spark = create_spark_session(app_name="my-streaming-job")
    df = spark.readStream.format("kafka")...
"""

from __future__ import annotations

import logging
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Maven coordinates for required JARs
# ---------------------------------------------------------------------
# Spark 3.5.0 is built against Scala 2.12 → that's why the _2.12 suffix.
# These get auto-downloaded from Maven Central on first run, cached in ~/.ivy2/.
SPARK_VERSION = "3.5.0"
SCALA_VERSION = "2.12"
POSTGRES_JDBC_VERSION = "42.7.1"

KAFKA_PACKAGE = (
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"
)
POSTGRES_PACKAGE = f"org.postgresql:postgresql:{POSTGRES_JDBC_VERSION}"

REQUIRED_PACKAGES = ",".join([KAFKA_PACKAGE, POSTGRES_PACKAGE])


# ---------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------
DEFAULT_MASTER = os.getenv("SPARK_MASTER", "local[*]")
DEFAULT_APP_NAME = os.getenv("SPARK_APP_NAME", "ecommerce-analytics")


# ---------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------
def create_spark_session(
    app_name: str = DEFAULT_APP_NAME,
    master: str = DEFAULT_MASTER,
    log_level: str = "WARN",
) -> SparkSession:
    """
    Build and return a configured SparkSession.

    Args:
        app_name: appears in the Spark UI and logs
        master: cluster URL or `local[*]` for development
        log_level: Spark's own log level (use WARN to stay sane)
    """
    logger.info(
        "Creating Spark session: app=%s master=%s",
        app_name, master,
    )

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # --- Required JARs (Kafka connector + Postgres JDBC) -----------
        .config("spark.jars.packages", REQUIRED_PACKAGES)
        # --- Sensible defaults for local development -------------------
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        # --- Streaming-specific ----------------------------------------
        # Each query will set its own checkpointLocation; we keep this as
        # a fallback root in case someone forgets.
        .config(
            "spark.sql.streaming.checkpointLocation",
            os.getenv("SPARK_CHECKPOINT_DIR", "data/checkpoints"),
        )
        # --- UI ---------------------------------------------------------
        # Spark UI on http://localhost:4040 (auto-increments if taken)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    # Tame Spark's own logging — INFO is unreadable, WARN is usable.
    spark.sparkContext.setLogLevel(log_level)

    logger.info("✓ Spark session ready (version=%s)", spark.version)
    return spark


# ---------------------------------------------------------------------
# Self-test: run `python -m spark.streaming.spark_session`
# ---------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Running Spark session self-test...")

    spark = create_spark_session(app_name="self-test")

    # Trivial DataFrame operation to confirm the session works end-to-end
    df = spark.createDataFrame(
        [(1, "hello"), (2, "spark"), (3, "world")],
        schema=["id", "word"],
    )
    logger.info("Created DataFrame with %d rows", df.count())
    df.show()

    spark.stop()
    logger.info("✓ Self-test complete")
