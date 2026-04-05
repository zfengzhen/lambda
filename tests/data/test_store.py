# tests/data/test_store.py
"""data.store 单元测试：init_db、连接、建表。"""
import pytest
import duckdb
from unittest.mock import patch

from config import DB_PATH
from data.store import init_db, get_connection


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with patch("config.DB_PATH", db_path), \
         patch("data.store.DB_PATH", db_path):
        init_db()
        yield db_path


def test_init_creates_tables(tmp_db):
    con = duckdb.connect(str(tmp_db))
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert "equity_bars" in tables
    assert "option_bars" in tables
    assert "sync_log" in tables
    con.close()


def test_init_creates_splits_table(tmp_db):
    con = duckdb.connect(str(tmp_db))
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert "splits" in tables
    con.close()


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
