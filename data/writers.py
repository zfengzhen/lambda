"""DuckDB 写入操作。"""
import logging
from pathlib import Path

from config import TICKER
from data.store import get_connection

logger = logging.getLogger(__name__)


def upsert_equity_bars(rows: list[dict]) -> int:
    """批量写入/更新股票日K。主键冲突时覆盖。

    Args:
        rows: list of {date, ticker, open, high, low, close, volume, vwap, transactions}

    Returns:
        写入行数
    """
    if not rows:
        return 0
    con = get_connection()
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
    """批量写入拆股记录（主键冲突时忽略）。"""
    if not rows:
        return 0
    con = get_connection()
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


def upsert_ticker_iv(rows: list[dict]) -> int:
    """批量写入/更新 ticker IV。主键冲突时覆盖。"""
    if not rows:
        return 0
    con = get_connection()
    try:
        con.executemany(
            """
            INSERT INTO ticker_iv (date, ticker, iv)
            VALUES (?, ?, ?)
            ON CONFLICT (date, ticker) DO UPDATE SET iv = excluded.iv
            """,
            [(r["date"], r["ticker"], r["iv"]) for r in rows],
        )
    finally:
        con.close()
    return len(rows)


def insert_option_bars_from_csv(csv_path: "Path", date_str: str) -> int:
    """从 gzip CSV 文件批量写入 option_bars（TQQQ 合约专用）。

    自动检测拆股记录，对拆股前的历史数据做前复权调整：
    - 价格字段乘以 price_factor，ROUND 到 2 位小数
    - volume 乘以 1/price_factor，转 BIGINT
    - OCC symbol 末 8 位 strike 编码同步调整

    Returns:
        写入行数
    """
    from data.queries import compute_split_factor

    where_sql = f"WHERE ticker LIKE 'O:{TICKER}%'"
    factor = compute_split_factor(date_str)

    if factor == 1.0:
        # 无拆股调整
        sql = f"""
            INSERT OR IGNORE INTO option_bars
                (date, symbol, open, high, low, close, volume, transactions,
                 strike, expiration, option_type)
            SELECT
                CAST('{date_str}' AS DATE),
                ticker,
                CAST(open AS DOUBLE),
                CAST(high AS DOUBLE),
                CAST(low  AS DOUBLE),
                CAST(close AS DOUBLE),
                TRY_CAST(CAST(volume AS VARCHAR) AS BIGINT),
                TRY_CAST(CAST(transactions AS VARCHAR) AS BIGINT),
                CAST(substr(ticker, length(ticker) - 7) AS DOUBLE) / 1000.0,
                CAST('20' || substr(ticker, length(ticker) - 14, 2) || '-'
                     || substr(ticker, length(ticker) - 12, 2) || '-'
                     || substr(ticker, length(ticker) - 10, 2) AS DATE),
                substr(ticker, length(ticker) - 8, 1)
            FROM read_csv('{str(csv_path)}', compression='gzip', header=true,
                auto_detect=true)
            {where_sql}
        """
    else:
        # 有拆股调整：应用因子
        pf = factor
        vf = 1.0 / pf
        like = f"ticker LIKE 'O:{TICKER}%'"

        symbol_expr = (
            f"CASE WHEN {like} THEN "
            f"substr(ticker, 1, length(ticker) - 8) || "
            f"lpad(CAST(CAST(ROUND("
            f"CAST(substr(ticker, length(ticker) - 7) AS BIGINT) * {pf}"
            f") AS BIGINT) AS VARCHAR), 8, '0') "
            f"ELSE ticker END"
        )
        open_expr = f"CASE WHEN {like} THEN ROUND(CAST(open AS DOUBLE) * {pf}, 2) ELSE CAST(open AS DOUBLE) END"
        high_expr = f"CASE WHEN {like} THEN ROUND(CAST(high AS DOUBLE) * {pf}, 2) ELSE CAST(high AS DOUBLE) END"
        low_expr = f"CASE WHEN {like} THEN ROUND(CAST(low AS DOUBLE) * {pf}, 2) ELSE CAST(low AS DOUBLE) END"
        close_expr = f"CASE WHEN {like} THEN ROUND(CAST(close AS DOUBLE) * {pf}, 2) ELSE CAST(close AS DOUBLE) END"
        vol_expr = (
            f"CASE WHEN {like} THEN CAST(ROUND("
            f"TRY_CAST(CAST(volume AS VARCHAR) AS BIGINT) * {vf}) AS BIGINT) "
            f"ELSE TRY_CAST(CAST(volume AS VARCHAR) AS BIGINT) END"
        )
        strike_expr = (
            f"CASE WHEN {like} THEN "
            f"CAST(substr(ticker, length(ticker) - 7) AS DOUBLE) / 1000.0 * {pf} "
            f"ELSE CAST(substr(ticker, length(ticker) - 7) AS DOUBLE) / 1000.0 END"
        )

        sql = f"""
            INSERT OR IGNORE INTO option_bars
                (date, symbol, open, high, low, close, volume, transactions,
                 strike, expiration, option_type)
            SELECT
                CAST('{date_str}' AS DATE),
                {symbol_expr},
                {open_expr},
                {high_expr},
                {low_expr},
                {close_expr},
                {vol_expr},
                TRY_CAST(CAST(transactions AS VARCHAR) AS BIGINT),
                {strike_expr},
                CAST('20' || substr(ticker, length(ticker) - 14, 2) || '-'
                     || substr(ticker, length(ticker) - 12, 2) || '-'
                     || substr(ticker, length(ticker) - 10, 2) AS DATE),
                substr(ticker, length(ticker) - 8, 1)
            FROM read_csv('{str(csv_path)}', compression='gzip', header=true,
                auto_detect=true)
            {where_sql}
        """

    con = get_connection()
    try:
        con.execute("BEGIN")
        con.execute(sql)
        written = con.execute(
            "SELECT COUNT(*) FROM option_bars WHERE date = CAST(? AS DATE)",
            [date_str],
        ).fetchone()[0]
        con.execute("COMMIT")
        logger.info(f"[writers] {date_str}: {written:,} 行写入 option_bars")
        return written
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def write_sync_log(date: str, data_type: str, rows_written: int,
                   status: str, message: str = None) -> None:
    """写入一条同步记录（TQQQ 专用）。"""
    con = get_connection()
    try:
        con.execute(
            """
            INSERT INTO sync_log (ts, data_type, date, ticker, rows_written, status, message)
            VALUES (now(), ?, ?, ?, ?, ?, ?)
            """,
            [data_type, date, TICKER, rows_written, status, message],
        )
    finally:
        con.close()


def delete_all_data() -> None:
    """清空 TQQQ 的 equity_bars、option_bars、option_month sync_log 和 ticker_iv。

    用于拆股后的全量重拉前清理。
    """
    con = get_connection()
    try:
        con.execute("DELETE FROM equity_bars WHERE ticker = ?", [TICKER])
        con.execute(
            "DELETE FROM option_bars WHERE symbol LIKE ?",
            [f"O:{TICKER}%"],
        )
        con.execute(
            "DELETE FROM sync_log WHERE data_type = 'option_month' AND ticker = ?",
            [TICKER],
        )
        con.execute("DELETE FROM ticker_iv WHERE ticker = ?", [TICKER])
    finally:
        con.close()
    logger.info(f"[writers] 已清空 {TICKER} 的全部数据")


def backfill_option_bars_columns() -> int:
    """回填存量 option_bars 的 strike/expiration/option_type 列。

    只更新 strike IS NULL 的行，从 symbol 解析。幂等操作。
    """
    con = get_connection()
    try:
        result = con.execute(
            "SELECT COUNT(*) FROM option_bars WHERE strike IS NULL"
        ).fetchone()
        null_count = result[0] if result else 0
        if null_count == 0:
            return 0

        con.execute("""
            UPDATE option_bars SET
                strike = CAST(substr(symbol, length(symbol) - 7) AS DOUBLE) / 1000.0,
                expiration = CAST(
                    '20' || substr(symbol, length(symbol) - 14, 2) || '-'
                    || substr(symbol, length(symbol) - 12, 2) || '-'
                    || substr(symbol, length(symbol) - 10, 2)
                    AS DATE),
                option_type = substr(symbol, length(symbol) - 8, 1)
            WHERE strike IS NULL
                AND symbol LIKE 'O:%' AND length(symbol) >= 17
        """)
        logger.info(f"[writers] 存量回填完成：{null_count} 行")
        return null_count
    finally:
        con.close()
