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

_CREATE_SPLITS = """
CREATE TABLE IF NOT EXISTS splits (
    ticker       VARCHAR  NOT NULL,
    exec_date    DATE     NOT NULL,
    split_from   INTEGER  NOT NULL,
    split_to     INTEGER  NOT NULL,
    PRIMARY KEY (ticker, exec_date)
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
        con.execute(_CREATE_SPLITS)
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


def upsert_splits(rows: list[dict]) -> int:
    """批量写入拆股记录（主键冲突时忽略）。

    Args:
        rows: list of {ticker, exec_date, split_from, split_to}

    Returns:
        写入行数
    """
    if not rows:
        return 0
    con = _connect()
    try:
        con.executemany(
            """
            INSERT OR IGNORE INTO splits (ticker, exec_date, split_from, split_to)
            VALUES (?, ?, ?, ?)
            """,
            [(r["ticker"], r["exec_date"], r["split_from"], r["split_to"])
             for r in rows],
        )
    finally:
        con.close()
    return len(rows)


def query_splits(ticker: str) -> list[dict]:
    """查询指定 ticker 的所有拆股记录，按执行日期升序。

    Returns:
        [{ticker, exec_date, split_from, split_to}]
    """
    con = _connect()
    try:
        rows = con.execute(
            "SELECT ticker, exec_date, split_from, split_to "
            "FROM splits WHERE ticker = ? ORDER BY exec_date",
            [ticker],
        ).fetchall()
    finally:
        con.close()
    return [
        {"ticker": r[0], "exec_date": str(r[1]),
         "split_from": r[2], "split_to": r[3]}
        for r in rows
    ]


def insert_option_bars_from_csv(
    csv_path: "Path",
    date_str: str,
    tickers: list[str] | None = None,
) -> int:
    """从 gzip CSV 文件批量写入 option_bars（使用 DuckDB read_csv，速度远快于 executemany）。

    自动检测拆股记录，对拆股前的历史数据做前复权调整：
    - 价格字段乘以 price_factor，ROUND 到 2 位小数
    - volume 乘以 1/price_factor，转 BIGINT
    - OCC symbol 末 8 位 strike 编码同步调整

    Args:
        csv_path:  本地 .csv.gz 文件路径
        date_str:  交易日期 "YYYY-MM-DD"
        tickers:   标的代码列表，如 ["TQQQ", "QQQ"]；None 或空列表则写入全部合约

    Returns:
        写入行数

    Raises:
        Exception: 写入失败时回滚并重新抛出
    """
    safe_tickers = [t.upper() for t in (tickers or [])
                     if t.isalpha() and len(t) <= 10]
    if safe_tickers:
        where_sql = "WHERE " + " OR ".join(
            f"ticker LIKE 'O:{t}%'" for t in safe_tickers
        )
    else:
        where_sql = ""

    # 计算每个 ticker 的拆股因子（同一 CSV 文件同一日期）
    # 注意：tickers=None 时不做拆股调整（生产环境总是传 tickers）
    factors = {}
    if safe_tickers:
        for t in safe_tickers:
            f = compute_split_factor(t, date_str)
            if f != 1.0:
                factors[t] = f

    # 无拆股调整时保持原始 SQL 不变
    if not factors:
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
    else:
        # 构建 CASE WHEN 表达式，为每个需要调整的 ticker 应用不同因子
        # symbol 改写：替换末尾 8 位 strike 编码
        symbol_cases = []
        open_cases = []
        high_cases = []
        low_cases = []
        close_cases = []
        vol_cases = []
        for t, pf in factors.items():
            vf = 1.0 / pf
            like = f"ticker LIKE 'O:{t}%'"
            # OCC symbol strike 改写
            symbol_cases.append(
                f"WHEN {like} THEN "
                f"substr(ticker, 1, length(ticker) - 8) || "
                f"lpad(CAST(CAST(ROUND("
                f"CAST(substr(ticker, length(ticker) - 7) AS BIGINT) * {pf}"
                f") AS BIGINT) AS VARCHAR), 8, '0')"
            )
            open_cases.append(f"WHEN {like} THEN ROUND(CAST(open AS DOUBLE) * {pf}, 2)")
            high_cases.append(f"WHEN {like} THEN ROUND(CAST(high AS DOUBLE) * {pf}, 2)")
            low_cases.append(f"WHEN {like} THEN ROUND(CAST(low AS DOUBLE) * {pf}, 2)")
            close_cases.append(f"WHEN {like} THEN ROUND(CAST(close AS DOUBLE) * {pf}, 2)")
            vol_cases.append(
                f"WHEN {like} THEN CAST(ROUND("
                f"TRY_CAST(CAST(volume AS VARCHAR) AS BIGINT) * {vf}) AS BIGINT)"
            )

        symbol_expr = "CASE " + " ".join(symbol_cases) + " ELSE ticker END"
        open_expr = "CASE " + " ".join(open_cases) + " ELSE CAST(open AS DOUBLE) END"
        high_expr = "CASE " + " ".join(high_cases) + " ELSE CAST(high AS DOUBLE) END"
        low_expr = "CASE " + " ".join(low_cases) + " ELSE CAST(low AS DOUBLE) END"
        close_expr = "CASE " + " ".join(close_cases) + " ELSE CAST(close AS DOUBLE) END"
        vol_expr = ("CASE " + " ".join(vol_cases)
                    + " ELSE TRY_CAST(CAST(volume AS VARCHAR) AS BIGINT) END")

        sql = f"""
            INSERT OR IGNORE INTO option_bars
                (date, symbol, open, high, low, close, volume, transactions)
            SELECT
                CAST('{date_str}' AS DATE),
                {symbol_expr},
                {open_expr},
                {high_expr},
                {low_expr},
                {close_expr},
                {vol_expr},
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


def compute_split_factor(ticker: str, date_str: str) -> float:
    """计算指定 ticker 在指定日期的前复权累积因子。

    前复权：把旧价格调到当前价格基准。
    因子 = ∏(split_from / split_to)，对所有 exec_date > date_str 的拆股事件累乘。
    拆股当天及之后的数据无需调整，因子 = 1.0。

    Args:
        ticker:   股票代码
        date_str: 日期 "YYYY-MM-DD"

    Returns:
        累积因子（浮点数），无拆股时返回 1.0
    """
    con = _connect()
    try:
        rows = con.execute(
            "SELECT split_from, split_to FROM splits "
            "WHERE ticker = ? AND exec_date > CAST(? AS DATE) "
            "ORDER BY exec_date",
            [ticker, date_str],
        ).fetchall()
    finally:
        con.close()
    factor = 1.0
    for split_from, split_to in rows:
        factor *= split_from / split_to
    return factor


def delete_ticker_data(ticker: str) -> None:
    """清空指定 ticker 的 equity_bars、option_bars 和 option_month sync_log。

    用于拆股后的全量重拉前清理。
    sync_log 只清 option_month 记录（期权 CSV 包含所有 ticker，需重新入库以应用新因子）。
    equity 的同步范围由 ensure_synced 的 need_purge 逻辑控制，无需清 sync_log。
    """
    con = _connect()
    try:
        con.execute("DELETE FROM equity_bars WHERE ticker = ?", [ticker])
        con.execute(
            "DELETE FROM option_bars WHERE symbol LIKE ?",
            [f"O:{ticker}%"],
        )
        # 清 option_month sync_log，强制重新下载并以新因子入库
        con.execute("DELETE FROM sync_log WHERE data_type = 'option_month'")
    finally:
        con.close()
    logger.info(f"[data_store] 已清空 {ticker} 的 equity_bars + option_bars + option_month sync_log")


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
