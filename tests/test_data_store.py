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


def test_query_option_bars_returns_sorted(tmp_db, tmp_path):
    row1 = [{"ticker": "O:TQQQ250131P00038500", "volume": "5", "open": "0.87",
             "close": "0.92", "high": "0.95", "low": "0.85",
             "window_start": "1000", "transactions": "2"}]
    row2 = [{"ticker": "O:TQQQ250131P00038500", "volume": "10", "open": "0.85",
             "close": "0.87", "high": "0.90", "low": "0.80",
             "window_start": "1000", "transactions": "3"}]
    f1 = tmp_path / "2025-01-07.csv.gz"
    f2 = tmp_path / "2025-01-06.csv.gz"
    f1.write_bytes(_make_csv_gz_ds(row1))
    f2.write_bytes(_make_csv_gz_ds(row2))
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.insert_option_bars_from_csv(f2, "2025-01-06")
        data_store.insert_option_bars_from_csv(f1, "2025-01-07")
        bars = data_store.query_option_bars(
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
    f.write_bytes(_make_csv_gz_ds(rows))
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.insert_option_bars_from_csv(f, "2025-01-06")
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


def test_get_latest_option_date(tmp_db, tmp_path):
    rows = [
        {"ticker": "O:TQQQ250131P00038500", "volume": "10", "open": "0.85",
         "close": "0.87", "high": "0.90", "low": "0.80",
         "window_start": "1000", "transactions": "3"},
    ]
    f1 = tmp_path / "2025-01-06.csv.gz"
    f2 = tmp_path / "2025-01-07.csv.gz"
    f1.write_bytes(_make_csv_gz_ds(rows))
    f2.write_bytes(_make_csv_gz_ds(rows))
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.insert_option_bars_from_csv(f1, "2025-01-06")
        data_store.insert_option_bars_from_csv(f2, "2025-01-07")
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


# ── insert_option_bars_from_csv ────────────────────────────

import gzip, io, csv as csv_mod

def _make_csv_gz_ds(rows):
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


def test_insert_csv_all_rows(tmp_db, tmp_path):
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz_ds(SAMPLE_CSV_ROWS))
    with patch.object(data_store, "DB_PATH", tmp_db):
        count = data_store.insert_option_bars_from_csv(f, "2025-01-06")
    assert count == 3


def test_insert_csv_ticker_filter_single(tmp_db, tmp_path):
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz_ds(SAMPLE_CSV_ROWS))
    with patch.object(data_store, "DB_PATH", tmp_db):
        count = data_store.insert_option_bars_from_csv(f, "2025-01-06", tickers=["TQQQ"])
    assert count == 1


def test_insert_csv_ticker_filter_multiple(tmp_db, tmp_path):
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz_ds(SAMPLE_CSV_ROWS))
    with patch.object(data_store, "DB_PATH", tmp_db):
        count = data_store.insert_option_bars_from_csv(f, "2025-01-06", tickers=["TQQQ", "QQQ"])
    assert count == 2


def test_insert_csv_blank_volume_null(tmp_db, tmp_path):
    rows = [{"ticker": "O:TQQQ250131P00038500", "volume": "", "open": "0.85",
             "close": "0.87", "high": "0.90", "low": "0.80",
             "window_start": "1000", "transactions": ""}]
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz_ds(rows))
    with patch.object(data_store, "DB_PATH", tmp_db):
        count = data_store.insert_option_bars_from_csv(f, "2025-01-06")
    assert count == 1
    import duckdb
    con = duckdb.connect(str(tmp_db))
    row = con.execute("SELECT volume, transactions FROM option_bars").fetchone()
    con.close()
    assert row[0] is None
    assert row[1] is None


# ── splits 表 CRUD ──────────────────────────────────────────

def test_init_creates_splits_table(tmp_db):
    con = duckdb.connect(str(tmp_db))
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert "splits" in tables
    con.close()


def test_upsert_splits(tmp_db):
    rows = [{"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2}]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits(rows)
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
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits(rows)
        data_store.upsert_splits(rows)  # 重复写入
    con = duckdb.connect(str(tmp_db))
    count = con.execute("SELECT COUNT(*) FROM splits").fetchone()[0]
    con.close()
    assert count == 1


def test_query_splits(tmp_db):
    rows = [
        {"ticker": "TQQQ", "exec_date": "2025-11-20",
         "split_from": 1, "split_to": 2},
        {"ticker": "QQQ", "exec_date": "2025-06-01",
         "split_from": 1, "split_to": 3},
    ]
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits(rows)
        result = data_store.query_splits("TQQQ")
    assert len(result) == 1
    assert result[0]["ticker"] == "TQQQ"
    assert result[0]["exec_date"] == "2025-11-20"
    assert result[0]["split_from"] == 1
    assert result[0]["split_to"] == 2


def test_query_splits_empty(tmp_db):
    with patch.object(data_store, "DB_PATH", tmp_db):
        result = data_store.query_splits("TQQQ")
    assert result == []


# ── compute_split_factor ────────────────────────────────────

def test_compute_split_factor_before_split(tmp_db):
    """拆股前日期，因子 = split_from/split_to = 0.5"""
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        factor = data_store.compute_split_factor("TQQQ", "2025-11-19")
    assert factor == 0.5


def test_compute_split_factor_on_split_date(tmp_db):
    """拆股当天，不需要调整（已是新价格），因子 = 1.0"""
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        factor = data_store.compute_split_factor("TQQQ", "2025-11-20")
    assert factor == 1.0


def test_compute_split_factor_after_split(tmp_db):
    """拆股后日期，因子 = 1.0"""
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        factor = data_store.compute_split_factor("TQQQ", "2025-12-01")
    assert factor == 1.0


def test_compute_split_factor_multiple_splits(tmp_db):
    """多次拆股累乘：1:2 再 1:3，最早期因子 = (1/2)*(1/3) = 1/6"""
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-06-01",
             "split_from": 1, "split_to": 2},
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 3},
        ])
        factor = data_store.compute_split_factor("TQQQ", "2025-05-01")
    assert abs(factor - 1/6) < 1e-9


def test_compute_split_factor_between_splits(tmp_db):
    """两次拆股之间，只受后面那次影响：因子 = 1/3"""
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-06-01",
             "split_from": 1, "split_to": 2},
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 3},
        ])
        factor = data_store.compute_split_factor("TQQQ", "2025-07-01")
    assert abs(factor - 1/3) < 1e-9


def test_compute_split_factor_no_splits(tmp_db):
    """无拆股记录，因子 = 1.0"""
    with patch.object(data_store, "DB_PATH", tmp_db):
        factor = data_store.compute_split_factor("TQQQ", "2025-11-19")
    assert factor == 1.0


# ── delete_ticker_data ──────────────────────────────────────

def test_delete_ticker_data(tmp_db, tmp_path):
    """清空指定 ticker 的 equity_bars + option_bars + sync_log"""
    rows_eq = [
        {"date": "2025-01-06", "ticker": "TQQQ", "open": 42.0,
         "high": 43.0, "low": 41.0, "close": 42.5,
         "volume": 1000000, "vwap": 42.3, "transactions": 5000},
    ]
    # 准备期权 CSV
    opt_rows = [
        {"ticker": "O:TQQQ250131P00038500", "volume": "10", "open": "0.85",
         "close": "0.87", "high": "0.90", "low": "0.80",
         "window_start": "1000", "transactions": "3"},
        {"ticker": "O:QQQ250131P00400000", "volume": "5", "open": "1.0",
         "close": "1.2", "high": "1.5", "low": "0.9",
         "window_start": "1000", "transactions": "2"},
    ]
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz_ds(opt_rows))

    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_equity_bars(rows_eq)
        data_store.insert_option_bars_from_csv(f, "2025-01-06")
        data_store.write_sync_log("2025-01-06", "option_month", 100, "ok")
        data_store.write_sync_log("2025-01-06", "equity", 1, "ok")

        data_store.delete_ticker_data("TQQQ")

        # equity_bars 被清空
        eq = data_store.query_equity_bars("TQQQ", "1900-01-01", "2099-12-31")
        assert eq == []
        # option_bars: TQQQ 被删除，QQQ 保留
        tqqq_opts = data_store.query_option_bars(
            "O:TQQQ250131P00038500", "2025-01-06", "2025-01-06")
        assert tqqq_opts == []
        qqq_opts = data_store.query_option_bars(
            "O:QQQ250131P00400000", "2025-01-06", "2025-01-06")
        assert len(qqq_opts) == 1
        # option_month sync_log 被清空，equity sync_log 保留
        assert not data_store.is_synced("2025-01-06", "option_month")
        assert data_store.is_synced("2025-01-06", "equity")


# ── insert_option_bars_from_csv 拆股调整 ─────────────────────

def test_insert_option_bars_with_split_factor(tmp_db, tmp_path):
    """1:2 拆股，拆股前日期：价格减半，volume 翻倍，symbol strike 减半"""
    rows = [{"ticker": "O:TQQQ250131P00038500", "volume": "100", "open": "4.00",
             "close": "3.80", "high": "4.20", "low": "3.60",
             "window_start": "1000", "transactions": "10"}]
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz_ds(rows))

    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        count = data_store.insert_option_bars_from_csv(
            f, "2025-01-06", tickers=["TQQQ"],
        )

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
    f.write_bytes(_make_csv_gz_ds(rows))

    with patch.object(data_store, "DB_PATH", tmp_db):
        count = data_store.insert_option_bars_from_csv(f, "2025-01-06", tickers=["TQQQ"])

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
    f.write_bytes(_make_csv_gz_ds(rows))

    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        count = data_store.insert_option_bars_from_csv(f, "2025-12-01", tickers=["TQQQ"])

    con = duckdb.connect(str(tmp_db))
    row = con.execute("SELECT symbol, open, close, volume FROM option_bars").fetchone()
    con.close()
    assert row[0] == "O:TQQQ260131P00019250"
    assert abs(row[1] - 2.00) < 0.01
    assert row[3] == 100


# ── ticker_iv 表 CRUD ────────────────────────────────────────

def test_init_creates_ticker_iv_table(tmp_db):
    con = duckdb.connect(str(tmp_db))
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert "ticker_iv" in tables
    con.close()


def test_option_bars_has_new_columns(tmp_db):
    """option_bars 表应含 strike, expiration, option_type 列"""
    con = duckdb.connect(str(tmp_db))
    cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'option_bars'"
    ).fetchall()}
    con.close()
    assert "strike" in cols
    assert "expiration" in cols
    assert "option_type" in cols


def test_upsert_ticker_iv(tmp_db):
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_ticker_iv([
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
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_ticker_iv([
            {"date": "2026-04-01", "ticker": "TQQQ", "iv": 0.65},
        ])
        data_store.upsert_ticker_iv([
            {"date": "2026-04-01", "ticker": "TQQQ", "iv": 0.80},
        ])
    con = duckdb.connect(str(tmp_db))
    row = con.execute("SELECT iv FROM ticker_iv WHERE ticker='TQQQ'").fetchone()
    con.close()
    assert abs(row[0] - 0.80) < 1e-9


def test_query_ticker_iv(tmp_db):
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_ticker_iv([
            {"date": "2026-04-01", "ticker": "TQQQ", "iv": 0.65},
            {"date": "2026-04-02", "ticker": "TQQQ", "iv": 0.70},
            {"date": "2026-04-01", "ticker": "QQQ", "iv": 0.20},
        ])
        rows = data_store.query_ticker_iv("TQQQ", "2026-04-01", "2026-04-02")
    assert len(rows) == 2
    assert rows[0]["ticker"] == "TQQQ"
    assert rows[0]["date"] == "2026-04-01"


def test_get_latest_iv_date(tmp_db):
    with patch.object(data_store, "DB_PATH", tmp_db):
        assert data_store.get_latest_iv_date("TQQQ") is None
        data_store.upsert_ticker_iv([
            {"date": "2026-04-01", "ticker": "TQQQ", "iv": 0.65},
            {"date": "2026-04-03", "ticker": "TQQQ", "iv": 0.70},
        ])
        assert data_store.get_latest_iv_date("TQQQ") == "2026-04-03"


def test_delete_ticker_iv(tmp_db):
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_ticker_iv([
            {"date": "2026-04-01", "ticker": "TQQQ", "iv": 0.65},
            {"date": "2026-04-01", "ticker": "QQQ", "iv": 0.20},
        ])
        data_store.delete_ticker_iv("TQQQ")
        assert data_store.get_latest_iv_date("TQQQ") is None
        assert data_store.get_latest_iv_date("QQQ") == "2026-04-01"


def test_delete_ticker_data_clears_ticker_iv(tmp_db, tmp_path):
    """delete_ticker_data 应同步清空 ticker_iv"""
    opt_rows = [
        {"ticker": "O:TQQQ250131P00038500", "volume": "10", "open": "0.85",
         "close": "0.87", "high": "0.90", "low": "0.80",
         "window_start": "1000", "transactions": "3"},
    ]
    f = tmp_path / "2025-01-06.csv.gz"
    f.write_bytes(_make_csv_gz_ds(opt_rows))

    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_equity_bars([
            {"date": "2025-01-06", "ticker": "TQQQ", "open": 42.0,
             "high": 43.0, "low": 41.0, "close": 42.5,
             "volume": 1000000, "vwap": 42.3, "transactions": 5000},
        ])
        data_store.insert_option_bars_from_csv(f, "2025-01-06")
        data_store.upsert_ticker_iv([
            {"date": "2025-01-06", "ticker": "TQQQ", "iv": 0.65},
        ])
        data_store.delete_ticker_data("TQQQ")
        assert data_store.get_latest_iv_date("TQQQ") is None


# ── strike/expiration/option_type 填充 ───────────────────────

def test_insert_csv_populates_new_columns(tmp_db, tmp_path):
    """入库后 strike/expiration/option_type 应被填充"""
    rows = [
        {"ticker": "O:TQQQ260424P00030000", "volume": "10", "open": "0.85",
         "close": "0.87", "high": "0.90", "low": "0.80",
         "window_start": "1000", "transactions": "3"},
    ]
    f = tmp_path / "2026-04-01.csv.gz"
    f.write_bytes(_make_csv_gz_ds(rows))
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.insert_option_bars_from_csv(f, "2026-04-01")
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
    f.write_bytes(_make_csv_gz_ds(rows))
    with patch.object(data_store, "DB_PATH", tmp_db):
        data_store.upsert_splits([
            {"ticker": "TQQQ", "exec_date": "2025-11-20",
             "split_from": 1, "split_to": 2},
        ])
        data_store.insert_option_bars_from_csv(f, "2025-01-06", tickers=["TQQQ"])
    con = duckdb.connect(str(tmp_db))
    row = con.execute(
        "SELECT symbol, strike, expiration, option_type FROM option_bars"
    ).fetchone()
    con.close()
    assert row[0] == "O:TQQQ250131P00019250"
    assert abs(row[1] - 19.25) < 0.01
    assert str(row[2]) == "2025-01-31"
    assert row[3] == "P"
