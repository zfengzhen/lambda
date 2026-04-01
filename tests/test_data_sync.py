import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock, call
import pytest
import data_sync
import data_store


def test_date_range_from_years():
    from_date, to_date = data_sync.date_range_from_years(2)
    today = datetime.date.today()
    assert to_date == str(today - datetime.timedelta(days=1))
    start = datetime.date.fromisoformat(from_date)
    assert (today - start).days >= 365 * 2 - 1


def test_full_sync_calls_both_downloaders(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path), \
         patch("data_sync.s3_downloader.sync_options") as mock_s3, \
         patch("data_sync.rest_downloader.sync_equity") as mock_rest:
        data_sync.full_sync(
            years=1, tickers=["TQQQ"], api_key="key"
        )
    mock_s3.assert_called_once()
    mock_rest.assert_called_once()
    # equity 调用包含 tickers 列表
    assert mock_rest.call_args.args[0] == ["TQQQ"]


def test_incremental_sync_starts_from_next_day(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        data_store.upsert_option_bars([{
            "date": "2025-06-01", "symbol": "O:TQQQ250131P00038500",
            "open": 0.85, "high": 0.90, "low": 0.80, "close": 0.87,
            "volume": 10, "transactions": 3,
        }])
        with patch("data_sync.s3_downloader.sync_options") as mock_s3, \
             patch("data_sync.rest_downloader.sync_equity") as mock_rest:
            data_sync.incremental_sync(tickers=["TQQQ"], api_key="key")

    # 起始日期是已有最新日期的次日
    mock_s3.assert_called_once()
    s3_from = mock_s3.call_args.args[0]
    assert s3_from == "2025-06-02"


def test_incremental_sync_when_no_existing_data(tmp_path):
    """无历史数据时增量同步默认拉最近 30 天。"""
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("data_sync.s3_downloader.sync_options") as mock_s3, \
             patch("data_sync.rest_downloader.sync_equity"):
            data_sync.incremental_sync(tickers=["TQQQ"], api_key="key")

    today = datetime.date.today()
    s3_from = datetime.date.fromisoformat(mock_s3.call_args.args[0])
    assert (today - s3_from).days >= 29
