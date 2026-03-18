import argparse
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

import yaml


@dataclass
class PrometheusConfig:
    url: str
    timeout_seconds: int = 30
    headers: dict[str, str] = field(default_factory=dict)
    tls_verify: bool = True


@dataclass
class MetricConfig:
    name: str
    query: str
    table: Optional[str] = None

    @property
    def table_name(self) -> str:
        if self.table:
            return self.table
        return self.name.replace(".", "_").replace("-", "_").replace(":", "_")


@dataclass
class SparkConfig:
    remote: Optional[str] = None  # Spark Connect URL, e.g. "sc://localhost:15002"


@dataclass
class AppConfig:
    prometheus: PrometheusConfig
    spark: SparkConfig
    database: str
    step: str
    metrics: list[MetricConfig] = field(default_factory=list)


def load_config(path: str) -> AppConfig:
    with open(path) as f:
        raw = yaml.safe_load(f)

    prom = PrometheusConfig(
        url=raw["prometheus"]["url"].rstrip("/"),
        timeout_seconds=raw["prometheus"].get("timeout_seconds", 30),
        headers=raw["prometheus"].get("headers", {}),
        tls_verify=raw["prometheus"].get("tls_verify", True),
    )

    spark_raw = raw.get("spark", {})
    spark_config = SparkConfig(
        remote=spark_raw.get("remote"),
    )

    defaults = raw.get("defaults", {})
    database = defaults.get("database", "default")
    step = defaults.get("step", "60s")

    metrics = []
    for m in raw.get("metrics", []):
        metrics.append(
            MetricConfig(
                name=m["name"],
                query=m["query"],
                table=m.get("table"),
            )
        )

    return AppConfig(prometheus=prom, spark=spark_config, database=database, step=step, metrics=metrics)


def parse_args(args=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prometheus to Iceberg scraper")
    parser.add_argument("--config", required=True, help="Path to metrics YAML config")
    parser.add_argument("--start", default=None, help="Start time (ISO 8601)")
    parser.add_argument("--end", default=None, help="End time (ISO 8601)")
    parser.add_argument("--spark-remote", default=None, help="Spark Connect URL (e.g. sc://localhost:15002), overrides config")
    return parser.parse_args(args)


def get_time_window(args: argparse.Namespace) -> tuple[datetime, datetime]:
    if args.start and args.end:
        start = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
        end = datetime.fromisoformat(args.end.replace("Z", "+00:00"))
        return start, end

    now = datetime.now(timezone.utc)
    end = now.replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=1)
    return start, end
