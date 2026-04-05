# tests/data/sync/test_equity.py
"""data.sync.equity 单元测试。"""
from unittest.mock import patch, MagicMock
import pytest
import duckdb

from data.store import init_db
from data.sync.equity import download_and_store


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


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch("config.DB_PATH", db_path), \
         patch("data.store.DB_PATH", db_path):
        init_db()
        yield db_path


def test_download_and_store_writes_to_db(tmp_db):
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db), \
         patch("data.sync.equity.requests.get",
               return_value=_mock_response(SAMPLE_BARS)):
        count = download_and_store("2025-01-06", "2025-01-07", "test_api_key")
    assert count == 2
    con = duckdb.connect(str(tmp_db))
    rows = con.execute("SELECT COUNT(*) FROM equity_bars").fetchone()[0]
    close = con.execute(
        "SELECT close FROM equity_bars WHERE date='2025-01-06'"
    ).fetchone()[0]
    con.close()
    assert rows == 2
    assert close == 42.5


def test_download_and_store_returns_zero_on_empty(tmp_db):
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db), \
         patch("data.sync.equity.requests.get",
               return_value=_mock_response([])):
        count = download_and_store("2025-01-06", "2025-01-07", "test_api_key")
    assert count == 0


def test_download_and_store_handles_429(tmp_db):
    mock_429 = MagicMock()
    mock_429.status_code = 429
    ok_resp = _mock_response(SAMPLE_BARS)

    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db), \
         patch("data.sync.equity.requests.get",
               side_effect=[mock_429, ok_resp]), \
         patch("data.sync.equity.time.sleep"):
        count = download_and_store("2025-01-06", "2025-01-07", "test_api_key")
    assert count == 2
    con = duckdb.connect(str(tmp_db))
    rows_in_db = con.execute("SELECT COUNT(*) FROM equity_bars").fetchone()[0]
    con.close()
    assert rows_in_db == 2


def test_download_equity_uses_adjusted_true(tmp_db):
    """确认 API 请求使用 adjusted=true"""
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db), \
         patch("data.sync.equity.requests.get",
               return_value=_mock_response(SAMPLE_BARS)) as mock_get:
        download_and_store("2025-01-06", "2025-01-07", "test_key")
    call_params = mock_get.call_args[1]["params"]
    assert call_params["adjusted"] == "true"
