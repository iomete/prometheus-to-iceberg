import logging

from pyspark.sql import SparkSession

from prometheus_to_iceberg.config import load_config, parse_args, get_time_window
from prometheus_to_iceberg.prometheus import query_range
from prometheus_to_iceberg.templating import substitute
from prometheus_to_iceberg.transformer import to_dataframe
from prometheus_to_iceberg.writer import ensure_table, write

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("prometheus_to_iceberg")


def main():
    args = parse_args()
    config = load_config(args.config)
    start, end = get_time_window(args)

    logger.info(
        "Time window: %s -> %s",
        start.isoformat(),
        end.isoformat(),
    )

    spark_remote = args.spark_remote or config.spark.remote
    builder = SparkSession.builder.appName("prometheus-to-iceberg")
    if spark_remote:
        builder = builder.remote(spark_remote)
    spark = builder.getOrCreate()

    # Phase 1: fetch and transform all metrics before writing anything.
    # If any metric fails here, we abort without writing any data.
    pending = []
    for metric in config.metrics:
        table_name = metric.table_name
        logger.info("Processing metric: %s -> %s.%s", metric.name, config.database, table_name)

        resolved_query = substitute(metric.query, config.variables)
        logger.info("Resolved query: %s", resolved_query)

        results = query_range(
            base_url=config.prometheus.url,
            query=resolved_query,
            start=start.timestamp(),
            end=end.timestamp(),
            step=config.step,
            timeout=config.prometheus.timeout_seconds,
            headers=config.prometheus.headers or None,
            tls_verify=config.prometheus.tls_verify,
        )

        if not results:
            logger.warning("No data returned for metric %s, skipping write", metric.name)
            continue

        df = to_dataframe(spark, results, metric.name)
        logger.info("DataFrame preview:\n%s", df.show(5))
        pending.append((metric, table_name, df))

    # Phase 2: all fetches succeeded — now write everything.
    for metric, table_name, df in pending:
        ensure_table(spark, config.database, table_name)
        write(df, config.database, table_name)
        logger.info("Successfully wrote metric %s", metric.name)

    spark.stop()
    logger.info("All metrics processed successfully")


if __name__ == "__main__":
    main()
