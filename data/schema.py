"""DuckDB 表结构定义与迁移。"""
import logging

import duckdb

logger = logging.getLogger(__name__)

_CREATE_EQUITY = """
CREATE TABLE IF NOT EXISTS equity_bars (
    date         DATE     NOT NULL,
    ticker       VARCHAR  NOT NULL,
    open         DOUBLE   NOT NULL,
    high         DOUBLE   NOT NULL,
    low          DOUBLE   NOT NULL,
    close        DOUBLE   NOT NULL,
    volume       BIGINT,
    vwap         DOUBLE,
    transactions INTEGER,
    PRIMARY KEY (date, ticker)
)
"""

_CREATE_OPTION = """
CREATE TABLE IF NOT EXISTS option_bars (
    date         DATE     NOT NULL,
    symbol       VARCHAR  NOT NULL,
    open         DOUBLE   NOT NULL,
    high         DOUBLE   NOT NULL,
    low          DOUBLE   NOT NULL,
    close        DOUBLE   NOT NULL,
    volume       BIGINT,
    transactions INTEGER,
    strike       DOUBLE,
    expiration   DATE,
    option_type  VARCHAR(1),
    PRIMARY KEY (date, symbol)
)
"""

_CREATE_SYNC_LOG = """
CREATE SEQUENCE IF NOT EXISTS sync_log_seq START 1;
CREATE TABLE IF NOT EXISTS sync_log (
    id           INTEGER   DEFAULT nextval('sync_log_seq'),
    ts           TIMESTAMP NOT NULL,
    date         DATE      NOT NULL,
    data_type    VARCHAR   NOT NULL,
    ticker       VARCHAR,
    rows_written INTEGER   NOT NULL,
    status       VARCHAR   NOT NULL,
    message      VARCHAR
)
"""

_CREATE_SPLITS = """
CREATE TABLE IF NOT EXISTS splits (
    ticker       VARCHAR  NOT NULL,
    exec_date    DATE     NOT NULL,
    split_from   INTEGER  NOT NULL,
    split_to     INTEGER  NOT NULL,
    PRIMARY KEY (ticker, exec_date)
)
"""

_CREATE_TICKER_IV = """
CREATE TABLE IF NOT EXISTS ticker_iv (
    date    DATE     NOT NULL,
    ticker  VARCHAR  NOT NULL,
    iv      DOUBLE   NOT NULL,
    PRIMARY KEY (date, ticker)
)
"""


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    """建表（幂等，已存在则跳过）。"""
    con.execute(_CREATE_EQUITY)
    con.execute(_CREATE_OPTION)
    con.execute(_CREATE_SYNC_LOG)
    con.execute(_CREATE_SPLITS)
    con.execute(_CREATE_TICKER_IV)


def run_migrations(con: duckdb.DuckDBPyConnection) -> None:
    """执行数据库迁移（幂等）。"""
    _migrate_option_bars(con)
    _migrate_sync_log_ticker(con)


def _migrate_option_bars(con: duckdb.DuckDBPyConnection) -> None:
    """为存量 option_bars 添加 strike/expiration/option_type 列。"""
    cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'option_bars'"
    ).fetchall()}
    if "strike" not in cols:
        con.execute("ALTER TABLE option_bars ADD COLUMN strike DOUBLE")
        con.execute("ALTER TABLE option_bars ADD COLUMN expiration DATE")
        con.execute("ALTER TABLE option_bars ADD COLUMN option_type VARCHAR(1)")
        logger.info("[schema] option_bars 迁移：添加 strike/expiration/option_type 列")


def _migrate_sync_log_ticker(con: duckdb.DuckDBPyConnection) -> None:
    """为存量 sync_log 添加 ticker 列。"""
    try:
        con.execute("ALTER TABLE sync_log ADD COLUMN ticker VARCHAR")
    except duckdb.CatalogException:
        pass
