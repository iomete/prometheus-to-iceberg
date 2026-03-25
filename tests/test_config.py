from argparse import Namespace
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from prometheus_to_iceberg.config import get_time_window


def make_args(start=None, end=None):
    return Namespace(start=start, end=end)


def test_default_window_is_utc():
    fixed_now = datetime(2026, 3, 25, 14, 35, 22, tzinfo=timezone.utc)
    with patch("prometheus_to_iceberg.config.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_now
        mock_dt.fromisoformat.side_effect = datetime.fromisoformat
        start, end = get_time_window(make_args())

    assert start.tzinfo == timezone.utc
    assert end.tzinfo == timezone.utc


def test_default_window_end_truncated_to_hour():
    fixed_now = datetime(2026, 3, 25, 14, 35, 22, tzinfo=timezone.utc)
    with patch("prometheus_to_iceberg.config.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_now
        mock_dt.fromisoformat.side_effect = datetime.fromisoformat
        start, end = get_time_window(make_args())

    assert end == datetime(2026, 3, 25, 14, 0, 0, tzinfo=timezone.utc)


def test_default_window_start_is_one_hour_before_end():
    fixed_now = datetime(2026, 3, 25, 14, 35, 22, tzinfo=timezone.utc)
    with patch("prometheus_to_iceberg.config.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_now
        mock_dt.fromisoformat.side_effect = datetime.fromisoformat
        start, end = get_time_window(make_args())

    assert end - start == timedelta(hours=1)


def test_explicit_window_parsed_correctly():
    start, end = get_time_window(make_args(
        start="2026-03-25T12:00:00Z",
        end="2026-03-25T18:00:00Z",
    ))

    assert start == datetime(2026, 3, 25, 12, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 3, 25, 18, 0, 0, tzinfo=timezone.utc)


def test_explicit_window_strips_whitespace():
    start, end = get_time_window(make_args(
        start="  2026-03-25T12:00:00Z  ",
        end="  2026-03-25T18:00:00Z  ",
    ))

    assert start == datetime(2026, 3, 25, 12, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 3, 25, 18, 0, 0, tzinfo=timezone.utc)


def test_parse_args_strips_whitespace_from_flags():
    from prometheus_to_iceberg.config import parse_args
    args = parse_args(["--config ", " metrics.yaml", "--start ", " 2026-03-25T12:00:00Z", "--end ", " 2026-03-25T18:00:00Z"])

    assert args.config == "metrics.yaml"
    assert args.start == "2026-03-25T12:00:00Z"
    assert args.end == "2026-03-25T18:00:00Z"
