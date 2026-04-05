# tests/data/sync/test_orchestrator.py
"""data.sync.orchestrator 单元测试。"""
import datetime
from unittest.mock import patch
import pytest

from data.store import init_db
from data.writers import upsert_equity_bars
from data.sync.orchestrator import ensure_synced


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch("config.DB_PATH", db_path), \
         patch("data.store.DB_PATH", db_path):
        init_db()
        yield db_path


def test_ensure_synced_empty_db(tmp_db):
    """空库时从近 2 年开始同步。"""
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db), \
         patch("data.sync.orchestrator.download_splits", return_value=[]), \
         patch("data.sync.orchestrator.sync_options") as mock_s3, \
         patch("data.sync.orchestrator.download_and_store") as mock_eq:
        ensure_synced(api_key="key")

    mock_s3.assert_called_once()
    mock_eq.assert_called_once()
    eq_from = datetime.date.fromisoformat(mock_eq.call_args.args[0])
    today = datetime.date.today()
    # 起始日期应在近 2 年范围内
    assert (today - eq_from).days >= 365 * 2 - 1


def test_ensure_synced_incremental(tmp_db):
    """有历史数据时从次日开始增量同步。"""
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_equity_bars([{
            "date": "2025-06-01", "ticker": "TQQQ",
            "open": 50.0, "high": 52.0, "low": 49.0, "close": 51.0,
            "volume": 100000, "vwap": 50.5, "transactions": 500,
        }])
        with patch("data.sync.orchestrator.download_splits", return_value=[]), \
             patch("data.sync.orchestrator.sync_options") as mock_s3, \
             patch("data.sync.orchestrator.download_and_store") as mock_eq:
            ensure_synced(api_key="key")

    mock_s3.assert_called_once()
    mock_eq.assert_called_once()
    # equity 同步起始日应为 2025-06-02（TQQQ 最新日期次日）
    assert mock_eq.call_args.args[0] == "2025-06-02"


def test_ensure_synced_already_latest(tmp_db):
    """数据已是昨天时不调用 equity 同步（option 同步仍会运行）。"""
    yesterday = str(datetime.date.today() - datetime.timedelta(days=1))
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_equity_bars([{
            "date": yesterday, "ticker": "TQQQ",
            "open": 50.0, "high": 52.0, "low": 49.0, "close": 51.0,
            "volume": 100000, "vwap": 50.5, "transactions": 500,
        }])
        with patch("data.sync.orchestrator.download_splits", return_value=[]), \
             patch("data.sync.orchestrator.sync_options") as mock_s3, \
             patch("data.sync.orchestrator.download_and_store") as mock_eq:
            ensure_synced(api_key="key")

    mock_eq.assert_not_called()


def test_ensure_synced_no_new_splits(tmp_db):
    """无新拆股事件时，正常同步，不清空数据"""
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db), \
         patch("data.sync.orchestrator.download_splits", return_value=[]) as mock_splits, \
         patch("data.sync.orchestrator.sync_options") as mock_s3, \
         patch("data.sync.orchestrator.download_and_store") as mock_eq, \
         patch("data.sync.orchestrator.delete_all_data") as mock_delete:
        ensure_synced(api_key="key")

    mock_splits.assert_called_once_with("key")
    mock_delete.assert_not_called()
    mock_s3.assert_called_once()
    mock_eq.assert_called_once()


def test_ensure_synced_new_split_triggers_purge(tmp_db):
    """发现新拆股事件时，清空数据并全量重拉"""
    yesterday = str(datetime.date.today() - datetime.timedelta(days=1))

    new_split = [{"ticker": "TQQQ", "exec_date": "2025-11-20",
                  "split_from": 1, "split_to": 2}]

    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        # 预置已有数据（本来不需要同步）
        upsert_equity_bars([{
            "date": yesterday, "ticker": "TQQQ",
            "open": 50.0, "high": 52.0, "low": 49.0, "close": 51.0,
            "volume": 100000, "vwap": 50.5, "transactions": 500,
        }])

        with patch("data.sync.orchestrator.download_splits",
                   return_value=new_split), \
             patch("data.sync.orchestrator.sync_options") as mock_s3, \
             patch("data.sync.orchestrator.download_and_store") as mock_eq, \
             patch("data.sync.orchestrator.delete_all_data") as mock_delete:
            ensure_synced(api_key="key")

    # 应该清空数据
    mock_delete.assert_called_once()
    # equity 应从全量起始日重拉（2 年前）
    mock_eq.assert_called_once()
    today = datetime.date.today()
    eq_from = datetime.date.fromisoformat(mock_eq.call_args.args[0])
    assert (today - eq_from).days >= 365 * 2 - 1


def test_ensure_synced_calls_sync_ticker_iv(tmp_db):
    """ensure_synced 应在同步完成后调用 sync_ticker_iv"""
    yesterday = str(datetime.date.today() - datetime.timedelta(days=1))

    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db), \
         patch("data.sync.orchestrator.sync_ticker_iv") as mock_iv, \
         patch("data.sync.orchestrator.download_splits", return_value=[]), \
         patch("data.sync.orchestrator.download_and_store"), \
         patch("data.sync.orchestrator.sync_options"):
        # 预置数据使同步被跳过
        upsert_equity_bars([{
            "date": yesterday, "ticker": "TQQQ",
            "open": 50.0, "high": 51.0, "low": 49.0, "close": 50.0,
            "volume": 1000, "vwap": 50.0, "transactions": 100,
        }])
        ensure_synced("test_key")
        mock_iv.assert_called_once()


def test_ensure_synced_calls_iv_even_when_synced(tmp_db):
    """即使数据已最新，IV 也应被计算（可能有历史数据未算 IV）"""
    yesterday = str(datetime.date.today() - datetime.timedelta(days=1))

    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db), \
         patch("data.sync.orchestrator.sync_ticker_iv") as mock_iv, \
         patch("data.sync.orchestrator.download_splits", return_value=[]), \
         patch("data.sync.orchestrator.sync_options") as mock_s3, \
         patch("data.sync.orchestrator.download_and_store") as mock_eq:
        upsert_equity_bars([{
            "date": yesterday, "ticker": "TQQQ",
            "open": 50.0, "high": 51.0, "low": 49.0, "close": 50.0,
            "volume": 1000, "vwap": 50.0, "transactions": 100,
        }])
        ensure_synced("test_key")

    # equity sync should be skipped (data already latest)
    mock_eq.assert_not_called()
    # but IV should still run
    mock_iv.assert_called_once()
