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
         patch("data_sync.rest_downloader.download_splits", return_value=[]), \
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
        with patch("data_sync.rest_downloader.download_splits", return_value=[]), \
             patch("data_sync.s3_downloader.sync_options") as mock_s3, \
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
        with patch("data_sync.rest_downloader.download_splits", return_value=[]), \
             patch("data_sync.s3_downloader.sync_options") as mock_s3, \
             patch("data_sync.rest_downloader.sync_equity") as mock_rest:
            data_sync.ensure_synced(tickers=["TQQQ"], api_key="key")

    mock_s3.assert_not_called()
    mock_rest.assert_not_called()


def test_ensure_synced_no_new_splits(tmp_path):
    """无新拆股事件时，正常同步，不清空数据"""
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("data_sync.rest_downloader.download_splits", return_value=[]) as mock_splits, \
             patch("data_sync.s3_downloader.sync_options") as mock_s3, \
             patch("data_sync.rest_downloader.sync_equity") as mock_rest, \
             patch("data_sync.data_store.delete_ticker_data") as mock_delete:
            data_sync.ensure_synced(tickers=["TQQQ"], api_key="key")

    mock_splits.assert_called_once_with("TQQQ", "key")
    mock_delete.assert_not_called()
    mock_s3.assert_called_once()
    mock_rest.assert_called_once()


def test_ensure_synced_new_split_triggers_purge(tmp_path):
    """发现新拆股事件时，清空该 ticker 数据并全量重拉"""
    db_path = tmp_path / "test.duckdb"
    yesterday = str(datetime.date.today() - datetime.timedelta(days=1))

    new_split = [{"ticker": "TQQQ", "exec_date": "2025-11-20",
                  "split_from": 1, "split_to": 2}]

    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        # 预置已有数据（本来不需要同步）
        data_store.upsert_equity_bars([{
            "date": yesterday, "ticker": "TQQQ",
            "open": 50.0, "high": 52.0, "low": 49.0, "close": 51.0,
            "volume": 100000, "vwap": 50.5, "transactions": 500,
        }])

        with patch("data_sync.rest_downloader.download_splits",
                   return_value=new_split), \
             patch("data_sync.s3_downloader.sync_options") as mock_s3, \
             patch("data_sync.rest_downloader.sync_equity") as mock_rest, \
             patch("data_sync.data_store.delete_ticker_data") as mock_delete:
            data_sync.ensure_synced(tickers=["TQQQ"], api_key="key")

    # 应该清空数据
    mock_delete.assert_called_once_with("TQQQ")
    # 应该全量重拉（from_date 应是近 2 年前，而非增量）
    mock_s3.assert_called_once()
    s3_from = mock_s3.call_args.args[0]
    today = datetime.date.today()
    from_date = datetime.date.fromisoformat(s3_from)
    assert (today - from_date).days >= 365 * 2 - 1
