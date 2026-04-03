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
         patch("data_sync.rest_downloader.download_and_store_equity") as mock_eq:
        data_store.init_db()
        data_sync.ensure_synced(tickers=["TQQQ"], api_key="key")

    mock_s3.assert_called_once()
    mock_eq.assert_called_once()
    eq_from = datetime.date.fromisoformat(mock_eq.call_args.args[1])
    today = datetime.date.today()
    # 起始日期应在近 2 年范围内
    assert (today - eq_from).days >= 365 * 2 - 1


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
             patch("data_sync.rest_downloader.download_and_store_equity") as mock_eq:
            data_sync.ensure_synced(tickers=["TQQQ"], api_key="key")

    mock_s3.assert_called_once()
    mock_eq.assert_called_once()
    # equity 同步起始日应为 2025-06-02（TQQQ 最新日期次日）
    assert mock_eq.call_args.args[0] == "TQQQ"
    assert mock_eq.call_args.args[1] == "2025-06-02"


def test_ensure_synced_already_latest(tmp_path):
    """数据已是昨天时不调用 equity 同步（option 同步仍会运行）。"""
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
             patch("data_sync.rest_downloader.download_and_store_equity") as mock_eq:
            data_sync.ensure_synced(tickers=["TQQQ"], api_key="key")

    mock_eq.assert_not_called()


def test_ensure_synced_no_new_splits(tmp_path):
    """无新拆股事件时，正常同步，不清空数据"""
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("data_sync.rest_downloader.download_splits", return_value=[]) as mock_splits, \
             patch("data_sync.s3_downloader.sync_options") as mock_s3, \
             patch("data_sync.rest_downloader.download_and_store_equity") as mock_eq, \
             patch("data_sync.data_store.delete_ticker_data") as mock_delete:
            data_sync.ensure_synced(tickers=["TQQQ"], api_key="key")

    mock_splits.assert_called_once_with("TQQQ", "key")
    mock_delete.assert_not_called()
    mock_s3.assert_called_once()
    mock_eq.assert_called_once()


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
             patch("data_sync.rest_downloader.download_and_store_equity") as mock_eq, \
             patch("data_sync.data_store.delete_ticker_data") as mock_delete:
            data_sync.ensure_synced(tickers=["TQQQ"], api_key="key")

    # 应该清空数据
    mock_delete.assert_called_once_with("TQQQ")
    # equity 应从全量起始日重拉（2 年前）
    mock_eq.assert_called_once()
    today = datetime.date.today()
    eq_from = datetime.date.fromisoformat(mock_eq.call_args.args[1])
    assert (today - eq_from).days >= 365 * 2 - 1


def test_ensure_synced_calls_sync_ticker_iv(tmp_path):
    """ensure_synced 应在同步完成后调用 sync_ticker_iv"""
    db_path = tmp_path / "test.duckdb"
    yesterday = str(datetime.date.today() - datetime.timedelta(days=1))

    with patch.object(data_store, "DB_PATH", db_path), \
         patch.object(data_sync, "sync_ticker_iv") as mock_iv, \
         patch("data_sync.rest_downloader.download_splits", return_value=[]), \
         patch("data_sync.rest_downloader.download_and_store_equity"), \
         patch("data_sync.s3_downloader.sync_options"):
        data_store.init_db()
        # 预置数据使同步被跳过
        data_store.upsert_equity_bars([{
            "date": yesterday, "ticker": "TQQQ",
            "open": 50.0, "high": 51.0, "low": 49.0, "close": 50.0,
            "volume": 1000, "vwap": 50.0, "transactions": 100,
        }])
        data_sync.ensure_synced(["TQQQ"], "test_key")
        mock_iv.assert_called_once_with(["TQQQ"])


def test_ensure_synced_calls_iv_even_when_synced(tmp_path):
    """即使数据已最新，IV 也应被计算（可能有历史数据未算 IV）"""
    db_path = tmp_path / "test.duckdb"
    yesterday = str(datetime.date.today() - datetime.timedelta(days=1))

    with patch.object(data_store, "DB_PATH", db_path), \
         patch.object(data_sync, "sync_ticker_iv") as mock_iv, \
         patch("data_sync.rest_downloader.download_splits", return_value=[]), \
         patch("data_sync.s3_downloader.sync_options") as mock_s3, \
         patch("data_sync.rest_downloader.download_and_store_equity") as mock_eq:
        data_store.init_db()
        data_store.upsert_equity_bars([{
            "date": yesterday, "ticker": "TQQQ",
            "open": 50.0, "high": 51.0, "low": 49.0, "close": 50.0,
            "volume": 1000, "vwap": 50.0, "transactions": 100,
        }])
        data_sync.ensure_synced(["TQQQ"], "test_key")

    # equity sync should be skipped (data already latest)
    mock_eq.assert_not_called()
    # but IV should still run
    mock_iv.assert_called_once_with(["TQQQ"])


def test_ensure_synced_new_ticker_full_sync(tmp_path):
    """已有 TQQQ 数据时，新增 QQQ 应独立从全量开始同步。"""
    db_path = tmp_path / "test.duckdb"
    yesterday = str(datetime.date.today() - datetime.timedelta(days=1))

    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        # TQQQ 已有最新数据
        data_store.upsert_equity_bars([{
            "date": yesterday, "ticker": "TQQQ",
            "open": 50.0, "high": 52.0, "low": 49.0, "close": 51.0,
            "volume": 100000, "vwap": 50.5, "transactions": 500,
        }])
        with patch("data_sync.rest_downloader.download_splits", return_value=[]), \
             patch("data_sync.s3_downloader.sync_options") as mock_s3, \
             patch("data_sync.rest_downloader.download_and_store_equity") as mock_eq:
            data_sync.ensure_synced(tickers=["TQQQ", "QQQ"], api_key="key")

    # 应该只调用 QQQ 的 equity 同步（TQQQ 已最新）
    assert mock_eq.call_count == 1
    qqq_call = mock_eq.call_args_list[0]
    assert qqq_call.args[0] == "QQQ"
    qqq_from = datetime.date.fromisoformat(qqq_call.args[1])
    today = datetime.date.today()
    assert (today - qqq_from).days >= 365 * 2 - 1
