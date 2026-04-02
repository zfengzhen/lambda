# tests/test_rest_downloader.py
from unittest.mock import patch, MagicMock
from pathlib import Path
import pytest
import rest_downloader
import data_store


def _mock_response(results: list[dict], status=200):
    mock = MagicMock()
    mock.status_code = status
    mock.json.return_value = {"results": results, "status": "OK"}
    mock.raise_for_status = MagicMock()
    return mock


SAMPLE_BARS = [
    {"t": 1736139600000, "o": 42.0, "h": 43.0, "l": 41.0,
     "c": 42.5, "v": 1000000, "vw": 42.3, "n": 5000},
    {"t": 1736226000000, "o": 42.5, "h": 44.0, "l": 42.0,
     "c": 43.8, "v": 900000, "vw": 43.1, "n": 4500},
]


def test_download_and_store_equity_writes_to_db(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("rest_downloader.requests.get",
                   return_value=_mock_response(SAMPLE_BARS)):
            count = rest_downloader.download_and_store_equity(
                "TQQQ", "2025-01-06", "2025-01-07", "test_api_key"
            )
    assert count == 2
    import duckdb
    con = duckdb.connect(str(db_path))
    rows = con.execute("SELECT COUNT(*) FROM equity_bars").fetchone()[0]
    close = con.execute(
        "SELECT close FROM equity_bars WHERE date='2025-01-06'"
    ).fetchone()[0]
    con.close()
    assert rows == 2
    assert close == 42.5


def test_download_and_store_equity_returns_zero_on_empty(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("rest_downloader.requests.get",
                   return_value=_mock_response([])):
            count = rest_downloader.download_and_store_equity(
                "TQQQ", "2025-01-06", "2025-01-07", "test_api_key"
            )
    assert count == 0


def test_download_and_store_equity_handles_429(tmp_path):
    db_path = tmp_path / "test.duckdb"
    mock_429 = MagicMock()
    mock_429.status_code = 429
    ok_resp = _mock_response(SAMPLE_BARS)

    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("rest_downloader.requests.get",
                   side_effect=[mock_429, ok_resp]), \
             patch("rest_downloader.time.sleep"):
            count = rest_downloader.download_and_store_equity(
                "TQQQ", "2025-01-06", "2025-01-07", "test_api_key"
            )
    assert count == 2
    import duckdb
    con = duckdb.connect(str(db_path))
    rows_in_db = con.execute("SELECT COUNT(*) FROM equity_bars").fetchone()[0]
    con.close()
    assert rows_in_db == 2


SAMPLE_SPLITS = [
    {"id": "abc123", "ticker": "TQQQ", "adjustment_type": "forward_split",
     "execution_date": "2025-11-20", "split_from": 1, "split_to": 2,
     "historical_adjustment_factor": 0.5},
]


def test_download_splits(tmp_path):
    db_path = tmp_path / "test.duckdb"
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"results": SAMPLE_SPLITS, "status": "OK"}
    mock_resp.raise_for_status = MagicMock()

    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("rest_downloader.requests.get", return_value=mock_resp):
            new_splits = rest_downloader.download_splits("TQQQ", "test_key")

    assert len(new_splits) == 1
    assert new_splits[0]["exec_date"] == "2025-11-20"
    assert new_splits[0]["split_from"] == 1
    assert new_splits[0]["split_to"] == 2

    # 验证写入 DB
    import duckdb
    con = duckdb.connect(str(db_path))
    row = con.execute("SELECT * FROM splits WHERE ticker = 'TQQQ'").fetchone()
    con.close()
    assert row is not None


def test_download_splits_detects_new(tmp_path):
    """已有记录时，只返回新增的拆股事件"""
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])

    api_results = SAMPLE_SPLITS + [
        {"id": "def456", "ticker": "TQQQ", "adjustment_type": "forward_split",
         "execution_date": "2026-06-01", "split_from": 1, "split_to": 3,
         "historical_adjustment_factor": 0.333},
    ]
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"results": api_results, "status": "OK"}
    mock_resp.raise_for_status = MagicMock()

    with patch.object(data_store, "DB_PATH", db_path):
        with patch("rest_downloader.requests.get", return_value=mock_resp):
            new_splits = rest_downloader.download_splits("TQQQ", "test_key")

    assert len(new_splits) == 1
    assert new_splits[0]["exec_date"] == "2026-06-01"


def test_download_splits_empty(tmp_path):
    """无拆股记录时返回空列表"""
    db_path = tmp_path / "test.duckdb"
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"results": [], "status": "OK"}
    mock_resp.raise_for_status = MagicMock()

    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("rest_downloader.requests.get", return_value=mock_resp):
            new_splits = rest_downloader.download_splits("TQQQ", "test_key")

    assert new_splits == []


def test_download_equity_uses_adjusted_true(tmp_path):
    """确认 API 请求使用 adjusted=true"""
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("rest_downloader.requests.get",
                   return_value=_mock_response(SAMPLE_BARS)) as mock_get:
            rest_downloader.download_and_store_equity(
                "TQQQ", "2025-01-06", "2025-01-07", "test_key"
            )
    call_params = mock_get.call_args[1]["params"]
    assert call_params["adjusted"] == "true"


def test_sync_equity_calls_each_ticker(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        with patch("rest_downloader.download_and_store_equity",
                   return_value=2) as mock_fetch:
            rest_downloader.sync_equity(
                ["TQQQ", "QQQ"], "2025-01-06", "2025-01-07", "test_key"
            )
    assert mock_fetch.call_count == 2
    calls_tickers = {c.args[0] for c in mock_fetch.call_args_list}
    assert calls_tickers == {"TQQQ", "QQQ"}
