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
