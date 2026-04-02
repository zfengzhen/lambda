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

_CREATE_SYNC_LOG = """
CREATE SEQUENCE IF NOT EXISTS sync_log_seq START 1;
CREATE TABLE IF NOT EXISTS sync_log (
    id           INTEGER   DEFAULT nextval('sync_log_seq'),
    ts           TIMESTAMP NOT NULL,
    date         DATE      NOT NULL,
    data_type    VARCHAR   NOT NULL,
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
    try:
        con.execute(_CREATE_EQUITY)
        con.execute(_CREATE_OPTION)
        con.execute(_CREATE_SYNC_LOG)
    finally:
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
    try:
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
    finally:
        con.close()
    return len(rows)


def insert_option_bars_from_csv(
    csv_path: "Path",
    date_str: str,
    tickers: list[str] | None = None,
) -> int:
    """从 gzip CSV 文件批量写入 option_bars（使用 DuckDB read_csv，速度远快于 executemany）。

    Args:
        csv_path:  本地 .csv.gz 文件路径
        date_str:  交易日期 "YYYY-MM-DD"
        tickers:   标的代码列表，如 ["TQQQ", "QQQ"]；None 或空列表则写入全部合约

    Returns:
        写入行数

    Raises:
        Exception: 写入失败时回滚并重新抛出
    """
    if tickers:
        where_sql = "WHERE " + " OR ".join(
            f"ticker LIKE 'O:{t.upper()}%'" for t in tickers
            if t.isalpha() and len(t) <= 10
        )
    else:
        where_sql = ""

    sql = f"""
        INSERT OR IGNORE INTO option_bars
            (date, symbol, open, high, low, close, volume, transactions)
        SELECT
            CAST('{date_str}' AS DATE),
            ticker,
            CAST(open AS DOUBLE),
            CAST(high AS DOUBLE),
            CAST(low  AS DOUBLE),
            CAST(close AS DOUBLE),
            TRY_CAST(CAST(volume AS VARCHAR) AS BIGINT),
            TRY_CAST(CAST(transactions AS VARCHAR) AS BIGINT)
        FROM read_csv('{str(csv_path)}', compression='gzip', header=true,
            auto_detect=true)
        {where_sql}
    """

    con = _connect()
    try:
        con.execute("BEGIN")
        con.execute(sql)
        written = con.execute(
            "SELECT COUNT(*) FROM option_bars WHERE date = CAST(? AS DATE)",
            [date_str],
        ).fetchone()[0]
        con.execute("COMMIT")
        logger.info(f"[data_store] {date_str}: {written:,} 行写入 option_bars")
        return written
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def query_option_bars(symbol: str, from_date: str, to_date: str) -> list[dict]:
    """查询指定期权合约在日期范围内的日K数据。

    Returns:
        [{date, symbol, open, high, low, close}] 按日期升序
    """
    con = _connect()
    try:
        rows = con.execute(
            """
            SELECT date, symbol, open, high, low, close
            FROM option_bars
            WHERE symbol = ? AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            [symbol, from_date, to_date],
        ).fetchall()
    finally:
        con.close()
    return [
        {"date": str(r[0]), "symbol": r[1], "open": r[2],
         "high": r[3], "low": r[4], "close": r[5]}
        for r in rows
    ]


def build_occ_symbol(ticker: str, expiry_date: str, strike: float,
                     option_type: str = "P") -> str:
    """构建 OCC 期权合约代码。

    Args:
        ticker: 标的代码，如 "TQQQ"
        expiry_date: 到期日 "YYYY-MM-DD"
        strike: 行权价（美元），如 30.0
        option_type: "P" 或 "C"

    Returns:
        OCC 格式代码，如 "O:TQQQ260424P00030000"
    """
    yy = expiry_date[2:4]
    mm = expiry_date[5:7]
    dd = expiry_date[8:10]
    strike_int = int(round(strike * 1000))
    return f"O:{ticker}{yy}{mm}{dd}{option_type}{strike_int:08d}"


def query_option_on_date(ticker: str, entry_date: str, expiry_date: str,
                         strike: float) -> dict | None:
    """查询最接近目标行权价的 Put 期权在入场日的价格。

    在候选到期日（精确日 ±3 天）中查找 DB 里所有可用 Put 合约，
    取 strike 最接近目标值的那个，返回入场日的 OHLCV。

    Returns:
        {symbol, date, open, high, low, close, volume, vwap} 或 None
    """
    from datetime import datetime, timedelta

    # 生成候选到期日：精确日 → ±1 → ±2 → ±3
    base = datetime.strptime(expiry_date, "%Y-%m-%d")
    candidates = [expiry_date]
    for offset in range(1, 4):
        candidates.append((base - timedelta(days=offset)).strftime("%Y-%m-%d"))
        candidates.append((base + timedelta(days=offset)).strftime("%Y-%m-%d"))

    # 构建 symbol LIKE 模式：O:TQQQ260402P%
    patterns = []
    for exp in candidates:
        yy, mm, dd = exp[2:4], exp[5:7], exp[8:10]
        patterns.append(f"O:{ticker}{yy}{mm}{dd}P%")

    con = _connect()
    try:
        # 查出入场日所有候选到期日的 Put 合约
        placeholders = " OR ".join(["symbol LIKE ?"] * len(patterns))
        rows = con.execute(
            f"""
            SELECT symbol, date, open, high, low, close, volume
            FROM option_bars
            WHERE ({placeholders}) AND date = ?
            """,
            patterns + [entry_date],
        ).fetchall()
    finally:
        con.close()

    if not rows:
        return None

    # 从 OCC symbol 提取 strike（末 8 位，单位千分之一美元）
    def _extract_strike(symbol: str) -> float:
        return int(symbol[-8:]) / 1000.0

    # 取最接近目标 strike 的合约
    best = min(rows, key=lambda r: abs(_extract_strike(r[0]) - strike))

    vol = best[6] or 0
    vwap = round((best[4] + best[5] + best[3]) / 3, 4) if vol > 0 else best[5]
    return {
        "symbol": best[0], "date": str(best[1]),
        "open": best[2], "high": best[3], "low": best[4], "close": best[5],
        "volume": vol, "vwap": round(vwap, 4),
    }


def query_equity_bars(ticker: str, from_date: str, to_date: str) -> list[dict]:
    """查询指定股票在日期范围内的日K数据。

    Returns:
        [{date, ticker, open, high, low, close, volume, vwap, transactions}] 按日期升序
    """
    con = _connect()
    try:
        rows = con.execute(
            """
            SELECT date, ticker, open, high, low, close, volume, vwap, transactions
            FROM equity_bars
            WHERE ticker = ? AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            [ticker, from_date, to_date],
        ).fetchall()
    finally:
        con.close()
    return [
        {"date": str(r[0]), "ticker": r[1], "open": r[2], "high": r[3],
         "low": r[4], "close": r[5], "volume": r[6],
         "vwap": r[7], "transactions": r[8]}
        for r in rows
    ]


_TABLE_MAP = {"option": "option_bars", "equity": "equity_bars"}


def get_latest_synced_date(data_type: str) -> str | None:
    """返回已同步的最新日期，无数据返回 None。

    Args:
        data_type: 'option' | 'equity'

    Raises:
        ValueError: data_type 不在允许列表中时抛出
    """
    if data_type not in _TABLE_MAP:
        raise ValueError(f"Unknown data_type: {data_type!r}")
    table = _TABLE_MAP[data_type]
    con = _connect()
    try:
        result = con.execute(f"SELECT MAX(date) FROM {table}").fetchone()
    finally:
        con.close()
    if result and result[0] is not None:
        return str(result[0])
    return None


def write_sync_log(date: str, data_type: str, rows_written: int,
                   status: str, message: str = None) -> None:
    """写入一条同步记录。"""
    con = _connect()
    try:
        con.execute(
            """
            INSERT INTO sync_log (ts, data_type, date, rows_written, status, message)
            VALUES (now(), ?, ?, ?, ?, ?)
            """,
            [data_type, date, rows_written, status, message],
        )
    finally:
        con.close()


def is_synced(date_str: str, data_type: str) -> bool:
    """检查指定日期和类型是否已在 sync_log 中有 ok 记录。DB 或表不存在时返回 False。"""
    if not DB_PATH.exists():
        return False
    con = _connect()
    try:
        result = con.execute(
            "SELECT COUNT(*) FROM sync_log WHERE date=? AND data_type=? AND status='ok'",
            [date_str, data_type],
        ).fetchone()[0]
    except duckdb.CatalogException:
        return False
    finally:
        con.close()
    return result > 0
