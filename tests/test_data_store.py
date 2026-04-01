# tests/test_data_store.py
import datetime
import pytest
import duckdb
from unittest.mock import patch
from pathlib import Path
import data_store


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch.object(data_store, "DB_PATH", db_path):
        data_store.init_db()
        yield db_path


def test_init_creates_tables(tmp_db):
    con = duckdb.connect(str(tmp_db))
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert "equity_bars" in tables
    assert "option_bars" in tables
    assert "sync_log" in tables
    con.close()


def test_upsert_equity_bars(tmp_db):
    rows = [
        {"date": "2025-01-06", "ticker": "TQQQ", "open": 42.0,
         "high": 43.0, "low": 41.0, "close": 42.5,
         "volume": 1000000, "vwap": 42.3, "transactions": 5000},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_equity_bars(rows)
    con = duckdb.connect(str(tmp_db))
    result = con.execute("SELECT close FROM equity_bars WHERE ticker='TQQQ'").fetchone()
    con.close()
    assert result[0] == 42.5


def test_upsert_equity_bars_deduplicates(tmp_db):
    row = {"date": "2025-01-06", "ticker": "TQQQ", "open": 42.0,
           "high": 43.0, "low": 41.0, "close": 42.5,
           "volume": 1000000, "vwap": 42.3, "transactions": 5000}
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_equity_bars([row])
        row["close"] = 99.0  # 更新值
        data_store.upsert_equity_bars([row])
    con = duckdb.connect(str(tmp_db))
    count = con.execute("SELECT COUNT(*) FROM equity_bars").fetchone()[0]
    close = con.execute("SELECT close FROM equity_bars").fetchone()[0]
    con.close()
    assert count == 1
    assert close == 99.0


def test_upsert_option_bars(tmp_db):
    rows = [
        {"date": "2025-01-06", "symbol": "O:TQQQ250131P00038500",
         "open": 0.85, "high": 0.90, "low": 0.80, "close": 0.87,
         "volume": 10, "transactions": 3},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_option_bars(rows)
    con = duckdb.connect(str(tmp_db))
    result = con.execute(
        "SELECT close FROM option_bars WHERE symbol='O:TQQQ250131P00038500'"
    ).fetchone()
    con.close()
    assert result[0] == 0.87


def test_query_option_bars_returns_sorted(tmp_db):
    rows = [
        {"date": "2025-01-07", "symbol": "O:TQQQ250131P00038500",
         "open": 0.87, "high": 0.95, "low": 0.85, "close": 0.92,
         "volume": 5, "transactions": 2},
        {"date": "2025-01-06", "symbol": "O:TQQQ250131P00038500",
         "open": 0.85, "high": 0.90, "low": 0.80, "close": 0.87,
         "volume": 10, "transactions": 3},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_option_bars(rows)
        bars = data_store.query_option_bars(
            "O:TQQQ250131P00038500", "2025-01-06", "2025-01-07"
        )
    assert len(bars) == 2
    assert bars[0]["date"] == "2025-01-06"
    assert bars[1]["date"] == "2025-01-07"


def test_query_option_bars_filters_by_symbol(tmp_db):
    rows = [
        {"date": "2025-01-06", "symbol": "O:TQQQ250131P00038500",
         "open": 0.85, "high": 0.90, "low": 0.80, "close": 0.87,
         "volume": 10, "transactions": 3},
        {"date": "2025-01-06", "symbol": "O:QQQ250131P00400000",
         "open": 1.0, "high": 1.5, "low": 0.9, "close": 1.2,
         "volume": 5, "transactions": 2},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_option_bars(rows)
        bars = data_store.query_option_bars(
            "O:TQQQ250131P00038500", "2025-01-06", "2025-01-06"
        )
    assert len(bars) == 1
    assert bars[0]["symbol"] == "O:TQQQ250131P00038500"


def test_query_equity_bars(tmp_db):
    rows = [
        {"date": "2025-01-06", "ticker": "TQQQ", "open": 42.0,
         "high": 43.0, "low": 41.0, "close": 42.5,
         "volume": 1000000, "vwap": 42.3, "transactions": 5000},
        {"date": "2025-01-07", "ticker": "TQQQ", "open": 42.5,
         "high": 44.0, "low": 42.0, "close": 43.8,
         "volume": 900000, "vwap": 43.1, "transactions": 4500},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_equity_bars(rows)
        bars = data_store.query_equity_bars("TQQQ", "2025-01-06", "2025-01-07")
    assert len(bars) == 2
    assert bars[0]["date"] == "2025-01-06"
    assert bars[0]["close"] == 42.5


def test_get_latest_option_date_returns_none_when_empty(tmp_db):
    with patch.object(data_store, "DB_PATH", tmp_db):
        result = data_store.get_latest_synced_date("option")
    assert result is None


def test_get_latest_option_date(tmp_db):
    rows = [
        {"date": "2025-01-06", "symbol": "O:TQQQ250131P00038500",
         "open": 0.85, "high": 0.90, "low": 0.80, "close": 0.87,
         "volume": 10, "transactions": 3},
        {"date": "2025-01-07", "symbol": "O:TQQQ250131P00038500",
         "open": 0.87, "high": 0.95, "low": 0.85, "close": 0.92,
         "volume": 5, "transactions": 2},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_option_bars(rows)
        result = data_store.get_latest_synced_date("option")
    assert result == "2025-01-07"


def test_write_sync_log(tmp_db):
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.write_sync_log("2025-01-06", "option", 260000, "ok")
    con = duckdb.connect(str(tmp_db))
    row = con.execute("SELECT date FROM sync_log").fetchone()
    con.close()
    assert row is not None
    assert row[0] == datetime.date(2025, 1, 6)


def test_get_latest_equity_date(tmp_db):
    rows = [
        {"date": "2025-01-06", "ticker": "TQQQ", "open": 42.0,
         "high": 43.0, "low": 41.0, "close": 42.5,
         "volume": 1000000, "vwap": 42.3, "transactions": 5000},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_equity_bars(rows)
        result = data_store.get_latest_synced_date("equity")
    assert result == "2025-01-06"


def test_get_latest_synced_date_invalid_type(tmp_db):
    with patch.object(data_store, "DB_PATH", tmp_db):
        with pytest.raises(ValueError, match="Unknown data_type"):
            data_store.get_latest_synced_date("invalid")
