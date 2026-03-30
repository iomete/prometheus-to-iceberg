from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    StringType,
    MapType,
    DoubleType,
)

SCHEMA = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("metric_name", StringType(), False),
    StructField("labels", MapType(StringType(), StringType()), False),
    StructField("value", DoubleType(), False),
    StructField("dt", StringType(), False),
    StructField("cluster", StringType(), False),
    StructField("namespace", StringType(), False),
])


def to_dataframe(
    spark: SparkSession,
    results: list[dict],
    metric_name: str,
) -> DataFrame:
    rows = []
    for series in results:
        labels = dict(series.get("metric", {}))
        labels.pop("__name__", None)

        for ts_val in series.get("values", []):
            ts_epoch, val_str = ts_val[0], ts_val[1]
            dt_obj = datetime.fromtimestamp(float(ts_epoch), tz=timezone.utc)
            rows.append((
                dt_obj,
                metric_name,
                labels,
                float(val_str),
                dt_obj.strftime("%Y-%m-%d"),
                labels.get("cluster", "unknown"),
                labels.get("namespace", "unknown"),
            ))

    return spark.createDataFrame(rows, schema=SCHEMA)
