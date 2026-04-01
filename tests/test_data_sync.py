import datetime
from pathlib import Path
from unittest.mock import patch
import pytest
import data_sync
import data_store


def test_ensure_synced_empty_db(tmp_path):
    """空库时从近 2 年开始同步。"""
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path), \
         patch("data_sync.s3_downloader.sync_options") as mock_s3, \
         patch("data_sync.rest_downloader.sync_equity") as mock_rest:
        data_store.init_db()
        data_sync.ensure_synced(tickers=["TQQQ"], api_key="key")

    mock_s3.assert_called_once()
    mock_rest.assert_called_once()
    s3_from = datetime.date.fromisoformat(mock_s3.call_args.args[0])
    today = datetime.date.today()
    # 起始日期应在近 2 年范围内
    assert (today - s3_from).days >= 365 * 2 - 1


def test_ensure_synced_incremental(tmp_path):
    """有历史数据时从次日开始增量同步。"""
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        data_store.upsert_equity_bars([{
            "date": "2025-06-01", "ticker": "TQQQ",
            "open": 50.0, "high": 52.0, "low": 49.0, "close": 51.0,
            "volume": 100000, "vwap": 50.5, "transactions": 500,
        }])
        with patch("data_sync.s3_downloader.sync_options") as mock_s3, \
             patch("data_sync.rest_downloader.sync_equity") as mock_rest:
            data_sync.ensure_synced(tickers=["TQQQ"], api_key="key")

    mock_s3.assert_called_once()
    s3_from = mock_s3.call_args.args[0]
    assert s3_from == "2025-06-02"


def test_ensure_synced_already_latest(tmp_path):
    """数据已是昨天时直接返回，不调用同步。"""
    db_path = tmp_path / "test.duckdb"
    yesterday = str(datetime.date.today() - datetime.timedelta(days=1))
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        data_store.upsert_equity_bars([{
            "date": yesterday, "ticker": "TQQQ",
            "open": 50.0, "high": 52.0, "low": 49.0, "close": 51.0,
            "volume": 100000, "vwap": 50.5, "transactions": 500,
        }])
        with patch("data_sync.s3_downloader.sync_options") as mock_s3, \
             patch("data_sync.rest_downloader.sync_equity") as mock_rest:
            data_sync.ensure_synced(tickers=["TQQQ"], api_key="key")

    mock_s3.assert_not_called()
    mock_rest.assert_not_called()
