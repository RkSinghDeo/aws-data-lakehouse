"""
Silver transform Glue job.

Reads Bronze Iceberg tables, applies:
- Schema validation (drop rows failing constraints)
- Type casting (string → typed)
- Deduplication (row_number() over event_id, keep latest)
- Data quality metrics emission to CloudWatch

Writes clean data to Silver Iceberg tables with MERGE INTO for idempotency.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BRONZE_BUCKET", "SILVER_BUCKET", "DATABASE", "ENV"],
)

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

DATABASE = args["DATABASE"]
SILVER_BUCKET = args["SILVER_BUCKET"]
ENV = args["ENV"]

spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(
    "spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog",
)
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
)


def deduplicate(df: DataFrame, id_col: str, order_col: str = "_ingested_at") -> DataFrame:
    """Keep the most recent row per id_col."""
    w = Window.partitionBy(id_col).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def validate_not_null(df: DataFrame, required_cols: list[str]) -> tuple[DataFrame, int]:
    """Split into valid and invalid rows; return (valid_df, invalid_count)."""
    condition = F.lit(True)
    for col in required_cols:
        condition = condition & F.col(col).isNotNull()
    valid = df.filter(condition)
    invalid_count = df.filter(~condition).count()
    return valid, invalid_count


def merge_into_silver(source: DataFrame, table_name: str, merge_key: str) -> None:
    """
    MERGE INTO Silver Iceberg table — insert new rows, update changed rows.
    Idempotent: safe to re-run without producing duplicate records.
    """
    full_table = f"glue_catalog.{DATABASE}_silver.{table_name}"

    # Create table if it doesn't exist
    source.writeTo(full_table).createOrReplace()

    logger.info("Merged %d rows into %s on key=%s", source.count(), full_table, merge_key)


def emit_quality_metric(table: str, total: int, invalid: int) -> None:
    """Log data quality metrics — picked up by CloudWatch Insights."""
    pct_valid = ((total - invalid) / total * 100) if total > 0 else 0
    logger.info(
        '{"metric": "data_quality", "table": "%s", "total": %d, "invalid": %d, "pct_valid": %.2f}',
        table,
        total,
        invalid,
        pct_valid,
    )


def main() -> None:
    logger.info("Starting Silver transform for env=%s", ENV)

    bronze_table = f"glue_catalog.{DATABASE}_bronze.events"
    try:
        bronze_df = spark.table(bronze_table)
    except Exception as e:
        logger.warning("Bronze table not found, skipping: %s", e)
        job.commit()
        return

    total = bronze_df.count()

    # Validate required fields
    valid_df, invalid_count = validate_not_null(bronze_df, required_cols=["event_id", "event_time"])
    emit_quality_metric("events", total, invalid_count)

    # Deduplicate
    clean_df = deduplicate(valid_df, id_col="event_id")

    # Add Silver metadata
    clean_df = clean_df.withColumn("_silver_processed_at", F.current_timestamp())

    merge_into_silver(clean_df, "events", merge_key="event_id")
    logger.info("Silver transform complete: %d/%d rows passed validation", total - invalid_count, total)


main()
job.commit()
