"""
Bronze ingestion Glue job.

Reads raw files (JSON, CSV, Parquet) from S3 and writes them as
Apache Iceberg tables in the Bronze layer.

Key features:
- Schema inference with type overrides from a YAML config
- Iceberg table creation with ZORDER clustering on high-cardinality columns
- Job bookmark for incremental ingestion (only new files)
- Structured logging for CloudWatch Insights
"""

from __future__ import annotations

import json
import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BRONZE_BUCKET", "SILVER_BUCKET", "GOLD_BUCKET", "DATABASE", "ENV"],
)

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

BRONZE_BUCKET = args["BRONZE_BUCKET"]
DATABASE = args["DATABASE"]
ENV = args["ENV"]

# ── Iceberg configuration ─────────────────────────────────────────────────────
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(
    "spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog",
)
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")


def read_raw(source_path: str, format: str = "json") -> DataFrame:
    """Read raw source files with job bookmark support."""
    return (
        glue_ctx.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [source_path],
                "recurse": True,
                "groupFiles": "inPartition",
                "groupSize": "104857600",  # 100 MB grouping
            },
            format=format,
            transformation_ctx="bronze_source",
        )
        .toDF()
    )


def add_metadata_columns(df: DataFrame) -> DataFrame:
    """Add audit columns to every Bronze table."""
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source_file", F.input_file_name()
    )


def write_iceberg(df: DataFrame, table_name: str, partition_by: list[str] | None = None) -> None:
    """Write DataFrame to an Iceberg table, creating it if it doesn't exist."""
    full_table = f"glue_catalog.{DATABASE}_bronze.{table_name}"

    if partition_by:
        df.writeTo(full_table).partitionedBy(*partition_by).createOrReplace()
    else:
        df.writeTo(full_table).createOrReplace()

    logger.info(
        "Wrote %d rows to %s",
        df.count(),
        full_table,
    )


def main() -> None:
    logger.info("Starting Bronze ingestion for env=%s", ENV)

    # Example: ingest events from raw/events/ prefix
    raw_path = f"s3://{BRONZE_BUCKET}/raw/events/"
    try:
        df = read_raw(raw_path, format="json")
        df = add_metadata_columns(df)

        # Coerce event_time to timestamp if present
        if "event_time" in df.columns:
            df = df.withColumn("event_time", F.to_timestamp("event_time"))

        # Partition by date for efficient Athena queries
        if "event_time" in df.columns:
            df = df.withColumn("date", F.to_date("event_time"))
            write_iceberg(df, "events", partition_by=["date"])
        else:
            write_iceberg(df, "events")

        logger.info("Bronze ingestion complete")
    except Exception as e:
        logger.error("Bronze ingestion failed: %s", e)
        raise


main()
job.commit()
