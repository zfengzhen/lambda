# tests/data/test_queries.py
"""data.queries 单元测试：查询操作。"""
import datetime
import gzip
import io
import csv as csv_mod

import pytest
import duckdb
from unittest.mock import patch

from data.store import init_db
from data.writers import (
    upsert_equity_bars, upsert_splits, upsert_ticker_iv,
    insert_option_bars_from_csv, write_sync_log,
)
from data.queries import (
    query_equity_bars, query_option_bars, query_splits,
    get_latest_equity_date, get_latest_iv_date,
    is_synced, compute_split_factor, query_ticker_iv,
)


def _make_csv_gz(rows):
    buf = io.StringIO()
    writer = csv_mod.DictWriter(buf, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode="w") as gz:
        gz.write(buf.getvalue().encode())
    return out.getvalue()


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch("config.DB_PATH", db_path), \
         patch("data.store.DB_PATH", db_path):
        init_db()
        yield db_path


def test_query_equity_bars(tmp_db):
    rows = [
        {"date": "2025-01-06", "ticker": "TQQQ", "open": 42.0,
         "high": 43.0, "low": 41.0, "close": 42.5,
         "volume": 1000000, "vwap": 42.3, "transactions": 5000},
        {"date": "2025-01-07", "ticker": "TQQQ", "open": 42.5,
         "high": 44.0, "low": 42.0, "close": 43.8,
         "volume": 900000, "vwap": 43.1, "transactions": 4500},
    ]
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_equity_bars(rows)
        bars = query_equity_bars("2025-01-06", "2025-01-07")
    assert len(bars) == 2
    assert bars[0]["date"] == "2025-01-06"
    assert bars[0]["close"] == 42.5


def test_query_option_bars_returns_sorted(tmp_db, tmp_path):
    row1 = [{"ticker": "O:TQQQ250131P00038500", "volume": "5", "open": "0.87",
             "close": "0.92", "high": "0.95", "low": "0.85",
             "window_start": "1000", "transactions": "2"}]
    row2 = [{"ticker": "O:TQQQ250131P00038500", "volume": "10", "open": "0.85",
             "close": "0.87", "high": "0.90", "low": "0.80",
             "window_start": "1000", "transactions": "3"}]
    f1 = tmp_path / "2025-01-07.csv.gz"
    f2 = tmp_path / "2025-01-06.csv.gz"
    f1.write_bytes(_make_csv_gz(row1))
    f2.write_bytes(_make_csv_gz(row2))
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        insert_option_bars_from_csv(f2, "2025-01-06")
        insert_option_bars_from_csv(f1, "2025-01-07")
        bars = query_option_bars(
            "O:TQQQ250131P00038500", "2025-01-06", "2025-01-07"
        )
    assert len(bars) == 2
    assert bars[0]["date"] == "2025-01-06"
    assert bars[1]["date"] == "2025-01-07"


def test_query_option_bars_filters_by_symbol(tmp_db, tmp_path):
    rows = [
        {"ticker": "O:TQQQ250131P00038500", "volume": "10", "open": "0.85",
         "close": "0.87", "high": "0.90", "low": "0.80",
         "window_start": "1000", "transactions": "3"},
        {"ticker": "O:QQQ250131P00400000", "volume": "5", "open": "1.0",
         "close": "1.2", "high": "1.5", "low": "0.9",
         "window_start": "1000", "transactions": "2"},
    ]
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz(rows))
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        insert_option_bars_from_csv(f, "2025-01-06")
        bars = query_option_bars(
            "O:TQQQ250131P00038500", "2025-01-06", "2025-01-06"
        )
    assert len(bars) == 1
    assert bars[0]["symbol"] == "O:TQQQ250131P00038500"


def test_get_latest_equity_date(tmp_db):
    rows = [
        {"date": "2025-01-06", "ticker": "TQQQ", "open": 42.0,
         "high": 43.0, "low": 41.0, "close": 42.5,
         "volume": 1000000, "vwap": 42.3, "transactions": 5000},
    ]
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_equity_bars(rows)
        result = get_latest_equity_date()
    assert result == "2025-01-06"


def test_get_latest_equity_date_empty(tmp_db):
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        result = get_latest_equity_date()
    assert result is None


def test_query_splits(tmp_db):
    rows = [
        {"ticker": "TQQQ", "exec_date": "2025-11-20",
         "split_from": 1, "split_to": 2},
    ]
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_splits(rows)
        result = query_splits()
    assert len(result) == 1
    assert result[0]["ticker"] == "TQQQ"
    assert result[0]["exec_date"] == "2025-11-20"
    assert result[0]["split_from"] == 1
    assert result[0]["split_to"] == 2


def test_query_splits_empty(tmp_db):
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        result = query_splits()
    assert result == []


def test_query_ticker_iv(tmp_db):
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_ticker_iv([
            {"date": "2026-04-01", "ticker": "TQQQ", "iv": 0.65},
            {"date": "2026-04-02", "ticker": "TQQQ", "iv": 0.70},
            {"date": "2026-04-01", "ticker": "QQQ", "iv": 0.20},
        ])
        rows = query_ticker_iv("2026-04-01", "2026-04-02")
    assert len(rows) == 2
    assert rows[0]["ticker"] == "TQQQ"
    assert rows[0]["date"] == "2026-04-01"


def test_get_latest_iv_date(tmp_db):
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        assert get_latest_iv_date() is None
        upsert_ticker_iv([
            {"date": "2026-04-01", "ticker": "TQQQ", "iv": 0.65},
            {"date": "2026-04-03", "ticker": "TQQQ", "iv": 0.70},
        ])
        assert get_latest_iv_date() == "2026-04-03"


def test_is_synced_option_month(tmp_db):
    """sync_log option_month 自动按 TQQQ 过滤。"""
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        write_sync_log("2026-03-01", "option_month", 1000, "ok")
        assert is_synced("2026-03-01", "option_month")


def test_is_synced_equity(tmp_db):
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        write_sync_log("2025-01-06", "equity", 1, "ok")
        assert is_synced("2025-01-06", "equity")


# ── compute_split_factor ────────────────────────────────────

def test_compute_split_factor_before_split(tmp_db):
    """拆股前日期，因子 = split_from/split_to = 0.5"""
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        factor = compute_split_factor("2025-11-19")
    assert factor == 0.5


def test_compute_split_factor_on_split_date(tmp_db):
    """拆股当天，不需要调整（已是新价格），因子 = 1.0"""
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        factor = compute_split_factor("2025-11-20")
    assert factor == 1.0


def test_compute_split_factor_after_split(tmp_db):
    """拆股后日期，因子 = 1.0"""
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        factor = compute_split_factor("2025-12-01")
    assert factor == 1.0


def test_compute_split_factor_multiple_splits(tmp_db):
    """多次拆股累乘：1:2 再 1:3，最早期因子 = (1/2)*(1/3) = 1/6"""
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-06-01",
             "split_from": 1, "split_to": 2},
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 3},
        ])
        factor = compute_split_factor("2025-05-01")
    assert abs(factor - 1/6) < 1e-9


def test_compute_split_factor_between_splits(tmp_db):
    """两次拆股之间，只受后面那次影响：因子 = 1/3"""
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-06-01",
             "split_from": 1, "split_to": 2},
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 3},
        ])
        factor = compute_split_factor("2025-07-01")
    assert abs(factor - 1/3) < 1e-9


def test_compute_split_factor_no_splits(tmp_db):
    """无拆股记录，因子 = 1.0"""
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        factor = compute_split_factor("2025-11-19")
    assert factor == 1.0
