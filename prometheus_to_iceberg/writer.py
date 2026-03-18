import logging

from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)


def ensure_table(spark: SparkSession, database: str, table_name: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {database}.{table_name} (
            timestamp   TIMESTAMP,
            metric_name STRING,
            labels      MAP<STRING, STRING>,
            value       DOUBLE,
            dt          STRING,
            hour        INT
        ) USING iceberg PARTITIONED BY (dt, hour)
    """)


def write(df: DataFrame, database: str, table_name: str) -> None:
    full_table = f"{database}.{table_name}"
    df.writeTo(full_table).overwritePartitions()
    logger.info("Wrote %d rows to %s", df.count(), full_table)
