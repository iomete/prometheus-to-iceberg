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
            cluster     STRING,
            namespace   STRING
        ) USING iceberg PARTITIONED BY (cluster, namespace, dt)
    """)


def write(df: DataFrame, database: str, table_name: str) -> None:
    full_table = f"{database}.{table_name}"
    count = df.count()
    df.createOrReplaceTempView("_incoming")
    df.sparkSession.sql(f"""
        MERGE INTO {full_table} t
        USING _incoming s
        ON  t.dt          = s.dt
        AND t.cluster     = s.cluster
        AND t.namespace   = s.namespace
        AND t.timestamp   = s.timestamp
        AND t.metric_name = s.metric_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    logger.info("----------------------------------------------------------")
    logger.info(" ")
    logger.info("Wrote %d rows to %s", count, full_table)
    logger.info(" ")
    logger.info("----------------------------------------------------------")
