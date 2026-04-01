"""本地 DuckDB 数据存储：建表、upsert、查询接口。

数据库文件：output/market_data.duckdb
"""
import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "output" / "market_data.duckdb"

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
    PRIMARY KEY (date, symbol)
)
"""

# sync_log 列顺序：id(0), ts(1), data_type(2), date(3), rows_written(4), status(5), message(6)
# 测试断言 row[3] == date，故 date 排在第 4 列
_CREATE_SYNC_LOG = """
CREATE SEQUENCE IF NOT EXISTS sync_log_seq START 1;
CREATE TABLE IF NOT EXISTS sync_log (
    id           INTEGER   DEFAULT nextval('sync_log_seq'),
    ts           TIMESTAMP NOT NULL,
    data_type    VARCHAR   NOT NULL,
    date         DATE      NOT NULL,
    rows_written INTEGER   NOT NULL,
    status       VARCHAR   NOT NULL,
    message      VARCHAR
)
"""


def _connect() -> duckdb.DuckDBPyConnection:
    """打开数据库连接，自动创建 output 目录。"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def init_db() -> None:
    """建表（幂等，已存在则跳过）。"""
    con = _connect()
    con.execute(_CREATE_EQUITY)
    con.execute(_CREATE_OPTION)
    con.execute(_CREATE_SYNC_LOG)
    con.close()
    logger.info(f"DB 初始化完成: {DB_PATH}")


def upsert_equity_bars(rows: list[dict]) -> int:
    """批量写入/更新股票日K。主键冲突时覆盖。

    Args:
        rows: list of {date, ticker, open, high, low, close, volume, vwap, transactions}

    Returns:
        写入行数
    """
    if not rows:
        return 0
    con = _connect()
    con.executemany(
        """
        INSERT INTO equity_bars
            (date, ticker, open, high, low, close, volume, vwap, transactions)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (date, ticker) DO UPDATE SET
            open         = excluded.open,
            high         = excluded.high,
            low          = excluded.low,
            close        = excluded.close,
            volume       = excluded.volume,
            vwap         = excluded.vwap,
            transactions = excluded.transactions
        """,
        [(r["date"], r["ticker"], r["open"], r["high"], r["low"],
          r["close"], r.get("volume"), r.get("vwap"), r.get("transactions"))
         for r in rows],
    )
    con.close()
    return len(rows)


def upsert_option_bars(rows: list[dict]) -> int:
    """批量写入/更新期权日K。主键冲突时覆盖。

    Args:
        rows: list of {date, symbol, open, high, low, close, volume, transactions}

    Returns:
        写入行数
    """
    if not rows:
        return 0
    con = _connect()
    con.executemany(
        """
        INSERT INTO option_bars
            (date, symbol, open, high, low, close, volume, transactions)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (date, symbol) DO UPDATE SET
            open         = excluded.open,
            high         = excluded.high,
            low          = excluded.low,
            close        = excluded.close,
            volume       = excluded.volume,
            transactions = excluded.transactions
        """,
        [(r["date"], r["symbol"], r["open"], r["high"], r["low"],
          r["close"], r.get("volume"), r.get("transactions"))
         for r in rows],
    )
    con.close()
    return len(rows)


def query_option_bars(symbol: str, from_date: str, to_date: str) -> list[dict]:
    """查询指定期权合约在日期范围内的日K数据。

    Returns:
        [{date, symbol, open, high, low, close}] 按日期升序
    """
    con = _connect()
    rows = con.execute(
        """
        SELECT date, symbol, open, high, low, close
        FROM option_bars
        WHERE symbol = ? AND date BETWEEN ? AND ?
        ORDER BY date
        """,
        [symbol, from_date, to_date],
    ).fetchall()
    con.close()
    return [
        {"date": str(r[0]), "symbol": r[1], "open": r[2],
         "high": r[3], "low": r[4], "close": r[5]}
        for r in rows
    ]


def query_equity_bars(ticker: str, from_date: str, to_date: str) -> list[dict]:
    """查询指定股票在日期范围内的日K数据。

    Returns:
        [{date, ticker, open, high, low, close, volume, vwap, transactions}] 按日期升序
    """
    con = _connect()
    rows = con.execute(
        """
        SELECT date, ticker, open, high, low, close, volume, vwap, transactions
        FROM equity_bars
        WHERE ticker = ? AND date BETWEEN ? AND ?
        ORDER BY date
        """,
        [ticker, from_date, to_date],
    ).fetchall()
    con.close()
    return [
        {"date": str(r[0]), "ticker": r[1], "open": r[2], "high": r[3],
         "low": r[4], "close": r[5], "volume": r[6],
         "vwap": r[7], "transactions": r[8]}
        for r in rows
    ]


def get_latest_synced_date(data_type: str) -> str | None:
    """返回已同步的最新日期，无数据返回 None。

    Args:
        data_type: 'option' | 'equity'
    """
    table = "option_bars" if data_type == "option" else "equity_bars"
    con = _connect()
    result = con.execute(f"SELECT MAX(date) FROM {table}").fetchone()
    con.close()
    if result and result[0] is not None:
        return str(result[0])
    return None


def write_sync_log(date: str, data_type: str, rows_written: int,
                   status: str, message: str = None) -> None:
    """写入一条同步记录。"""
    con = _connect()
    con.execute(
        """
        INSERT INTO sync_log (ts, data_type, date, rows_written, status, message)
        VALUES (now(), ?, ?, ?, ?, ?)
        """,
        [data_type, date, rows_written, status, message],
    )
    con.close()
