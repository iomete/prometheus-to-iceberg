from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone, timedelta

from prometheus_to_iceberg.config import AppConfig, PrometheusConfig, SparkConfig, MetricConfig


def make_config(*metric_names):
    return AppConfig(
        prometheus=PrometheusConfig(url="http://prometheus:9090"),
        spark=SparkConfig(),
        database="db",
        step="60s",
        metrics=[MetricConfig(name=n, query=f"metric_{n}") for n in metric_names],
        variables={},
    )


FAKE_RESULTS = [{"metric": {"__name__": "m"}, "values": [[1700000000, "1.0"]]}]
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 1, 1, 1, tzinfo=timezone.utc)


@patch("job.write")
@patch("job.ensure_table")
@patch("job.to_dataframe")
@patch("job.query_range")
@patch("job.substitute", side_effect=lambda q, v: q)
@patch("job.SparkSession")
@patch("job.get_time_window", return_value=(START, END))
@patch("job.load_config")
@patch("job.parse_args")
def test_all_metrics_succeed_writes_all(
    mock_parse_args, mock_load_config, mock_time_window,
    mock_spark_session, mock_substitute, mock_query_range,
    mock_to_dataframe, mock_ensure_table, mock_write,
):
    mock_parse_args.return_value = MagicMock(config="config.yaml", spark_remote=None)
    mock_load_config.return_value = make_config("cpu", "memory")
    mock_query_range.return_value = FAKE_RESULTS
    mock_to_dataframe.return_value = MagicMock()

    from job import main
    main()

    assert mock_write.call_count == 2


@patch("job.write")
@patch("job.ensure_table")
@patch("job.to_dataframe")
@patch("job.query_range")
@patch("job.substitute", side_effect=lambda q, v: q)
@patch("job.SparkSession")
@patch("job.get_time_window", return_value=(START, END))
@patch("job.load_config")
@patch("job.parse_args")
def test_first_metric_fails_nothing_written(
    mock_parse_args, mock_load_config, mock_time_window,
    mock_spark_session, mock_substitute, mock_query_range,
    mock_to_dataframe, mock_ensure_table, mock_write,
):
    mock_parse_args.return_value = MagicMock(config="config.yaml", spark_remote=None)
    mock_load_config.return_value = make_config("cpu", "memory")
    mock_query_range.side_effect = [RuntimeError("connection refused"), FAKE_RESULTS]

    import pytest
    from job import main
    with pytest.raises(RuntimeError):
        main()

    mock_write.assert_not_called()


@patch("job.write")
@patch("job.ensure_table")
@patch("job.to_dataframe")
@patch("job.query_range")
@patch("job.substitute", side_effect=lambda q, v: q)
@patch("job.SparkSession")
@patch("job.get_time_window", return_value=(START, END))
@patch("job.load_config")
@patch("job.parse_args")
def test_second_metric_fails_nothing_written(
    mock_parse_args, mock_load_config, mock_time_window,
    mock_spark_session, mock_substitute, mock_query_range,
    mock_to_dataframe, mock_ensure_table, mock_write,
):
    mock_parse_args.return_value = MagicMock(config="config.yaml", spark_remote=None)
    mock_load_config.return_value = make_config("cpu", "memory")
    mock_query_range.side_effect = [FAKE_RESULTS, RuntimeError("timeout")]
    mock_to_dataframe.return_value = MagicMock()

    import pytest
    from job import main
    with pytest.raises(RuntimeError):
        main()

    mock_write.assert_not_called()


@patch("job.write")
@patch("job.ensure_table")
@patch("job.to_dataframe")
@patch("job.query_range")
@patch("job.substitute", side_effect=lambda q, v: q)
@patch("job.SparkSession")
@patch("job.get_time_window", return_value=(START, END))
@patch("job.load_config")
@patch("job.parse_args")
def test_query_range_called_with_end_minus_one_second(
    mock_parse_args, mock_load_config, mock_time_window,
    mock_spark_session, mock_substitute, mock_query_range,
    mock_to_dataframe, mock_ensure_table, mock_write,
):
    mock_parse_args.return_value = MagicMock(config="config.yaml", spark_remote=None)
    mock_load_config.return_value = make_config("cpu")
    mock_query_range.return_value = FAKE_RESULTS
    mock_to_dataframe.return_value = MagicMock()

    from job import main
    main()

    _, kwargs = mock_query_range.call_args
    expected_end = (END - timedelta(seconds=1)).timestamp()
    assert kwargs["end"] == expected_end
