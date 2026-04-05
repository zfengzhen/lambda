# tests/data/test_writers.py
"""data.writers 单元测试：写入、upsert、拆股调整、backfill。"""
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
    insert_option_bars_from_csv, write_sync_log, delete_all_data,
    backfill_option_bars_columns,
)
from data.queries import (
    query_equity_bars, query_option_bars, get_latest_iv_date,
    is_synced,
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


SAMPLE_CSV_ROWS = [
    {"ticker": "O:TQQQ250131P00038500", "volume": "10", "open": "0.85",
     "close": "0.87", "high": "0.90", "low": "0.80",
     "window_start": "1000", "transactions": "3"},
    {"ticker": "O:QQQ250131P00400000", "volume": "5", "open": "1.0",
     "close": "1.2", "high": "1.5", "low": "0.9",
     "window_start": "1000", "transactions": "2"},
    {"ticker": "O:SPY250131P00500000", "volume": "8", "open": "2.0",
     "close": "2.1", "high": "2.2", "low": "1.9",
     "window_start": "1000", "transactions": "4"},
]


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch("config.DB_PATH", db_path), \
         patch("data.store.DB_PATH", db_path):
        init_db()
        yield db_path


# ── upsert_equity_bars ──────────────────────────────────────

def test_upsert_equity_bars(tmp_db):
    rows = [
        {"date": "2025-01-06", "ticker": "TQQQ", "open": 42.0,
         "high": 43.0, "low": 41.0, "close": 42.5,
         "volume": 1000000, "vwap": 42.3, "transactions": 5000},
    ]
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_equity_bars(rows)
    con = duckdb.connect(str(tmp_db))
    result = con.execute("SELECT close FROM equity_bars WHERE ticker='TQQQ'").fetchone()
    con.close()
    assert result[0] == 42.5


def test_upsert_equity_bars_deduplicates(tmp_db):
    row = {"date": "2025-01-06", "ticker": "TQQQ", "open": 42.0,
           "high": 43.0, "low": 41.0, "close": 42.5,
           "volume": 1000000, "vwap": 42.3, "transactions": 5000}
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_equity_bars([row])
        row["close"] = 99.0  # 更新值
        upsert_equity_bars([row])
    con = duckdb.connect(str(tmp_db))
    count = con.execute("SELECT COUNT(*) FROM equity_bars").fetchone()[0]
    close = con.execute("SELECT close FROM equity_bars").fetchone()[0]
    con.close()
    assert count == 1
    assert close == 99.0


# ── upsert_splits ──────────────────────────────────────────

def test_upsert_splits(tmp_db):
    rows = [{"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2}]
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_splits(rows)
    con = duckdb.connect(str(tmp_db))
    result = con.execute("SELECT * FROM splits").fetchall()
    con.close()
    assert len(result) == 1
    assert result[0][0] == "TQQQ"
    assert result[0][2] == 1  # split_from
    assert result[0][3] == 2  # split_to


def test_upsert_splits_idempotent(tmp_db):
    rows = [{"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2}]
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_splits(rows)
        upsert_splits(rows)  # 重复写入
    con = duckdb.connect(str(tmp_db))
    count = con.execute("SELECT COUNT(*) FROM splits").fetchone()[0]
    con.close()
    assert count == 1


# ── write_sync_log ──────────────────────────────────────────

def test_write_sync_log(tmp_db):
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        write_sync_log("2025-01-06", "option", 260000, "ok")
    con = duckdb.connect(str(tmp_db))
    row = con.execute("SELECT date FROM sync_log").fetchone()
    con.close()
    assert row is not None
    assert row[0] == datetime.date(2025, 1, 6)


# ── upsert_ticker_iv ──────────────────────────────────────

def test_upsert_ticker_iv(tmp_db):
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_ticker_iv([
            {"date": "2026-04-01", "ticker": "TQQQ", "iv": 0.65},
            {"date": "2026-04-02", "ticker": "TQQQ", "iv": 0.70},
        ])
    con = duckdb.connect(str(tmp_db))
    rows = con.execute(
        "SELECT date, iv FROM ticker_iv WHERE ticker='TQQQ' ORDER BY date"
    ).fetchall()
    con.close()
    assert len(rows) == 2
    assert abs(rows[0][1] - 0.65) < 1e-9
    assert abs(rows[1][1] - 0.70) < 1e-9


def test_upsert_ticker_iv_overwrites(tmp_db):
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_ticker_iv([
            {"date": "2026-04-01", "ticker": "TQQQ", "iv": 0.65},
        ])
        upsert_ticker_iv([
            {"date": "2026-04-01", "ticker": "TQQQ", "iv": 0.80},
        ])
    con = duckdb.connect(str(tmp_db))
    row = con.execute("SELECT iv FROM ticker_iv WHERE ticker='TQQQ'").fetchone()
    con.close()
    assert abs(row[0] - 0.80) < 1e-9


# ── insert_option_bars_from_csv ──────────────────────────────

def test_insert_csv_all_rows(tmp_db, tmp_path):
    """TQQQ-only：只写入 TQQQ 合约"""
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz(SAMPLE_CSV_ROWS))
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        count = insert_option_bars_from_csv(f, "2025-01-06")
    # 只写入 TQQQ（新模块按 TICKER 过滤）
    assert count == 1


def test_insert_csv_blank_volume_null(tmp_db, tmp_path):
    rows = [{"ticker": "O:TQQQ250131P00038500", "volume": "", "open": "0.85",
             "close": "0.87", "high": "0.90", "low": "0.80",
             "window_start": "1000", "transactions": ""}]
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz(rows))
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        count = insert_option_bars_from_csv(f, "2025-01-06")
    assert count == 1
    con = duckdb.connect(str(tmp_db))
    row = con.execute("SELECT volume, transactions FROM option_bars").fetchone()
    con.close()
    assert row[0] is None
    assert row[1] is None


# ── insert_option_bars_from_csv 拆股调整 ─────────────────────

def test_insert_option_bars_with_split_factor(tmp_db, tmp_path):
    """1:2 拆股，拆股前日期：价格减半，volume 翻倍，symbol strike 减半"""
    rows = [{"ticker": "O:TQQQ250131P00038500", "volume": "100", "open": "4.00",
             "close": "3.80", "high": "4.20", "low": "3.60",
             "window_start": "1000", "transactions": "10"}]
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz(rows))

    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        count = insert_option_bars_from_csv(f, "2025-01-06")

    assert count == 1
    con = duckdb.connect(str(tmp_db))
    row = con.execute("SELECT symbol, open, high, low, close, volume FROM option_bars").fetchone()
    con.close()
    assert row[0] == "O:TQQQ250131P00019250"
    assert abs(row[1] - 2.00) < 0.01  # open: 4.00 * 0.5
    assert abs(row[2] - 2.10) < 0.01  # high: 4.20 * 0.5
    assert abs(row[3] - 1.80) < 0.01  # low:  3.60 * 0.5
    assert abs(row[4] - 1.90) < 0.01  # close: 3.80 * 0.5
    assert row[5] == 200              # volume: 100 * 2


def test_insert_option_bars_no_split(tmp_db, tmp_path):
    """无拆股时，数据不变"""
    rows = [{"ticker": "O:TQQQ250131P00038500", "volume": "100", "open": "4.00",
             "close": "3.80", "high": "4.20", "low": "3.60",
             "window_start": "1000", "transactions": "10"}]
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz(rows))

    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        count = insert_option_bars_from_csv(f, "2025-01-06")

    con = duckdb.connect(str(tmp_db))
    row = con.execute("SELECT symbol, open, close, volume FROM option_bars").fetchone()
    con.close()
    assert row[0] == "O:TQQQ250131P00038500"
    assert abs(row[1] - 4.00) < 0.01
    assert abs(row[2] - 3.80) < 0.01
    assert row[3] == 100


def test_insert_option_bars_after_split_date(tmp_db, tmp_path):
    """拆股后日期，数据不变"""
    rows = [{"ticker": "O:TQQQ260131P00019250", "volume": "100", "open": "2.00",
             "close": "1.90", "high": "2.10", "low": "1.80",
             "window_start": "1000", "transactions": "10"}]
    f = tmp_path / "2025-12-01.csv.gz"
    f.write_bytes(_make_csv_gz(rows))

    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        count = insert_option_bars_from_csv(f, "2025-12-01")

    con = duckdb.connect(str(tmp_db))
    row = con.execute("SELECT symbol, open, close, volume FROM option_bars").fetchone()
    con.close()
    assert row[0] == "O:TQQQ260131P00019250"
    assert abs(row[1] - 2.00) < 0.01
    assert row[3] == 100


# ── strike/expiration/option_type 填充 ───────────────────────

def test_insert_csv_populates_new_columns(tmp_db, tmp_path):
    """入库后 strike/expiration/option_type 应被填充"""
    rows = [
        {"ticker": "O:TQQQ260424P00030000", "volume": "10", "open": "0.85",
         "close": "0.87", "high": "0.90", "low": "0.80",
         "window_start": "1000", "transactions": "3"},
    ]
    f = tmp_path / "2026-04-01.csv.gz"
    f.write_bytes(_make_csv_gz(rows))
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        insert_option_bars_from_csv(f, "2026-04-01")
    con = duckdb.connect(str(tmp_db))
    row = con.execute(
        "SELECT strike, expiration, option_type FROM option_bars"
    ).fetchone()
    con.close()
    assert abs(row[0] - 30.0) < 0.01
    assert str(row[1]) == "2026-04-24"
    assert row[2] == "P"


def test_insert_csv_new_columns_with_split(tmp_db, tmp_path):
    """拆股调整后，新列 strike 应反映调整后的值"""
    rows = [
        {"ticker": "O:TQQQ250131P00038500", "volume": "100", "open": "4.00",
         "close": "3.80", "high": "4.20", "low": "3.60",
         "window_start": "1000", "transactions": "10"},
    ]
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz(rows))
    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        insert_option_bars_from_csv(f, "2025-01-06")
    con = duckdb.connect(str(tmp_db))
    row = con.execute(
        "SELECT symbol, strike, expiration, option_type FROM option_bars"
    ).fetchone()
    con.close()
    assert row[0] == "O:TQQQ250131P00019250"
    assert abs(row[1] - 19.25) < 0.01
    assert str(row[2]) == "2025-01-31"
    assert row[3] == "P"


# ── backfill_option_bars_columns ────────────────────────────

def test_backfill_option_bars_columns(tmp_db, tmp_path):
    """存量数据 strike=NULL，回填后应有值"""
    con = duckdb.connect(str(tmp_db))
    con.execute(
        "INSERT INTO option_bars (date, symbol, open, high, low, close, volume, transactions) "
        "VALUES ('2025-01-06', 'O:TQQQ250131P00038500', 0.85, 0.90, 0.80, 0.87, 10, 3)"
    )
    con.execute(
        "INSERT INTO option_bars (date, symbol, open, high, low, close, volume, transactions) "
        "VALUES ('2025-01-06', 'O:QQQ260515C00450000', 1.0, 1.5, 0.9, 1.2, 5, 2)"
    )
    row = con.execute("SELECT strike FROM option_bars WHERE symbol='O:TQQQ250131P00038500'").fetchone()
    assert row[0] is None
    con.close()

    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        backfill_option_bars_columns()

    con = duckdb.connect(str(tmp_db))
    r1 = con.execute(
        "SELECT strike, expiration, option_type FROM option_bars "
        "WHERE symbol='O:TQQQ250131P00038500'"
    ).fetchone()
    r2 = con.execute(
        "SELECT strike, expiration, option_type FROM option_bars "
        "WHERE symbol='O:QQQ260515C00450000'"
    ).fetchone()
    con.close()

    assert abs(r1[0] - 38.5) < 0.01
    assert str(r1[1]) == "2025-01-31"
    assert r1[2] == "P"

    assert abs(r2[0] - 450.0) < 0.01
    assert str(r2[1]) == "2026-05-15"
    assert r2[2] == "C"


def test_backfill_idempotent(tmp_db, tmp_path):
    """已回填的数据再次调用不报错"""
    con = duckdb.connect(str(tmp_db))
    con.execute(
        "INSERT INTO option_bars (date, symbol, open, high, low, close, volume, transactions) "
        "VALUES ('2025-01-06', 'O:TQQQ250131P00038500', 0.85, 0.90, 0.80, 0.87, 10, 3)"
    )
    con.close()

    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        backfill_option_bars_columns()
        backfill_option_bars_columns()  # 再次调用

    con = duckdb.connect(str(tmp_db))
    count = con.execute("SELECT COUNT(*) FROM option_bars").fetchone()[0]
    con.close()
    assert count == 1


# ── delete_all_data ──────────────────────────────────────────

def test_delete_all_data(tmp_db, tmp_path):
    """清空 TQQQ 的 equity_bars + option_bars + sync_log + ticker_iv"""
    rows_eq = [
        {"date": "2025-01-06", "ticker": "TQQQ", "open": 42.0,
         "high": 43.0, "low": 41.0, "close": 42.5,
         "volume": 1000000, "vwap": 42.3, "transactions": 5000},
    ]
    opt_rows = [
        {"ticker": "O:TQQQ250131P00038500", "volume": "10", "open": "0.85",
         "close": "0.87", "high": "0.90", "low": "0.80",
         "window_start": "1000", "transactions": "3"},
    ]
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz(opt_rows))

    with patch("config.DB_PATH", tmp_db), \
         patch("data.store.DB_PATH", tmp_db):
        upsert_equity_bars(rows_eq)
        insert_option_bars_from_csv(f, "2025-01-06")
        write_sync_log("2025-01-06", "option_month", 100, "ok")
        upsert_ticker_iv([
            {"date": "2025-01-06", "ticker": "TQQQ", "iv": 0.65},
        ])

        delete_all_data()

        # equity_bars 被清空
        eq = query_equity_bars("1900-01-01", "2099-12-31")
        assert eq == []
        # ticker_iv 被清空
        assert get_latest_iv_date() is None
        # option_month sync_log 被清空
        assert not is_synced("2025-01-06", "option_month")
