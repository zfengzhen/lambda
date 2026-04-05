"""DuckDB 查询操作（只读）。"""
import logging

import duckdb

from config import TICKER, DB_PATH
from data.store import get_connection

logger = logging.getLogger(__name__)


def query_equity_bars(from_date: str, to_date: str) -> list[dict]:
    """查询 TQQQ 在日期范围内的日K数据。

    Returns:
        [{date, ticker, open, high, low, close, volume, vwap, transactions}] 按日期升序
    """
    con = get_connection()
    try:
        rows = con.execute(
            """
            SELECT date, ticker, open, high, low, close, volume, vwap, transactions
            FROM equity_bars
            WHERE ticker = ? AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            [TICKER, from_date, to_date],
        ).fetchall()
    finally:
        con.close()
    return [
        {"date": str(r[0]), "ticker": r[1], "open": r[2], "high": r[3],
         "low": r[4], "close": r[5], "volume": r[6],
         "vwap": r[7], "transactions": r[8]}
        for r in rows
    ]


def query_option_bars(symbol: str, from_date: str, to_date: str) -> list[dict]:
    """查询指定期权合约在日期范围内的日K数据。

    Returns:
        [{date, symbol, open, high, low, close}] 按日期升序
    """
    con = get_connection()
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


def query_option_on_date(entry_date: str, expiry_date: str,
                         strike: float) -> dict | None:
    """查询最接近目标行权价的 Put 期权在入场日的价格。

    在候选到期日（精确日 ±3 天）中查找所有可用 Put 合约，
    取 strike ≤ 目标值且最接近的（向下匹配）。

    Returns:
        {symbol, date, open, high, low, close, volume, vwap} 或 None
    """
    from datetime import datetime, timedelta

    base = datetime.strptime(expiry_date, "%Y-%m-%d")
    candidates = [expiry_date]
    for offset in range(1, 4):
        candidates.append((base - timedelta(days=offset)).strftime("%Y-%m-%d"))
        candidates.append((base + timedelta(days=offset)).strftime("%Y-%m-%d"))

    patterns = []
    for exp in candidates:
        yy, mm, dd = exp[2:4], exp[5:7], exp[8:10]
        patterns.append(f"O:{TICKER}{yy}{mm}{dd}P%")

    con = get_connection()
    try:
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

    def _extract_strike(symbol: str) -> float:
        return int(symbol[-8:]) / 1000.0

    # 取 strike ≤ 目标值且最接近的合约（向下匹配，确保实际 OTM ≥ 策略 OTM）
    below = [r for r in rows if _extract_strike(r[0]) <= strike]
    if below:
        best = max(below, key=lambda r: _extract_strike(r[0]))
    else:
        best = min(rows, key=lambda r: abs(_extract_strike(r[0]) - strike))

    vol = best[6] or 0
    vwap = round((best[4] + best[5] + best[3]) / 3, 4) if vol > 0 else best[5]
    return {
        "symbol": best[0], "date": str(best[1]),
        "open": best[2], "high": best[3], "low": best[4], "close": best[5],
        "volume": vol, "vwap": round(vwap, 4),
    }


def query_ticker_iv(from_date: str, to_date: str) -> list[dict]:
    """查询 TQQQ 的 IV 数据。

    Returns:
        [{date, ticker, iv}] 按日期升序
    """
    con = get_connection()
    try:
        rows = con.execute(
            "SELECT date, ticker, iv FROM ticker_iv "
            "WHERE ticker = ? AND date BETWEEN ? AND ? ORDER BY date",
            [TICKER, from_date, to_date],
        ).fetchall()
    finally:
        con.close()
    return [{"date": str(r[0]), "ticker": r[1], "iv": r[2]} for r in rows]


def query_splits() -> list[dict]:
    """查询 TQQQ 的所有拆股记录，按执行日期升序。

    Returns:
        [{ticker, exec_date, split_from, split_to}]
    """
    con = get_connection()
    try:
        rows = con.execute(
            "SELECT ticker, exec_date, split_from, split_to "
            "FROM splits WHERE ticker = ? ORDER BY exec_date",
            [TICKER],
        ).fetchall()
    finally:
        con.close()
    return [
        {"ticker": r[0], "exec_date": str(r[1]),
         "split_from": r[2], "split_to": r[3]}
        for r in rows
    ]


def get_latest_equity_date() -> str | None:
    """返回 TQQQ 在 equity_bars 中的最新日期，无数据返回 None。"""
    con = get_connection()
    try:
        result = con.execute(
            "SELECT MAX(date) FROM equity_bars WHERE ticker = ?",
            [TICKER],
        ).fetchone()
    finally:
        con.close()
    if result and result[0] is not None:
        return str(result[0])
    return None


def get_latest_iv_date() -> str | None:
    """返回 TQQQ 在 ticker_iv 中的最新日期，无数据返回 None。"""
    con = get_connection()
    try:
        result = con.execute(
            "SELECT MAX(date) FROM ticker_iv WHERE ticker = ?", [TICKER]
        ).fetchone()
    finally:
        con.close()
    if result and result[0] is not None:
        return str(result[0])
    return None


def get_latest_option_date() -> str | None:
    """返回 option_bars 中 TQQQ 合约的最新日期，无数据返回 None。"""
    con = get_connection()
    try:
        result = con.execute(
            "SELECT MAX(date) FROM option_bars WHERE symbol LIKE ?",
            [f"O:{TICKER}%"],
        ).fetchone()
    finally:
        con.close()
    if result and result[0] is not None:
        return str(result[0])
    return None


def get_earliest_option_date() -> str | None:
    """返回 option_bars 中 TQQQ 合约的最早日期，无数据返回 None。"""
    con = get_connection()
    try:
        result = con.execute(
            "SELECT MIN(date) FROM option_bars WHERE symbol LIKE ?",
            [f"O:{TICKER}%"],
        ).fetchone()
    finally:
        con.close()
    if result and result[0] is not None:
        return str(result[0])
    return None


def get_option_dates_in_range(from_date: str, to_date: str) -> list[str]:
    """返回日期范围内有 TQQQ 期权数据的所有日期。"""
    con = get_connection()
    try:
        rows = con.execute(
            "SELECT DISTINCT date FROM option_bars "
            "WHERE symbol LIKE ? AND date BETWEEN ? AND ? ORDER BY date",
            [f"O:{TICKER}%", from_date, to_date],
        ).fetchall()
    finally:
        con.close()
    return [str(r[0]) for r in rows]


def query_option_bars_for_iv(date_str: str) -> list[dict]:
    """查询指定日期的 TQQQ 全部期权数据，用于 IV 计算。"""
    con = get_connection()
    try:
        bars = con.execute(
            "SELECT date, symbol, open, high, low, close, volume, "
            "transactions, strike, expiration, option_type "
            "FROM option_bars "
            "WHERE symbol LIKE ? AND date = ?",
            [f"O:{TICKER}%", date_str],
        ).fetchall()
    finally:
        con.close()
    return [
        {"date": str(r[0]), "symbol": r[1], "open": r[2], "high": r[3],
         "low": r[4], "close": r[5], "volume": r[6], "transactions": r[7],
         "strike": r[8], "expiration": str(r[9]) if r[9] else None,
         "option_type": r[10]}
        for r in bars
    ]


def is_synced(date_str: str, data_type: str) -> bool:
    """检查指定日期和类型是否已在 sync_log 中有 ok 记录。

    对 option_month 类型自动按 TQQQ 过滤。
    """
    if not DB_PATH.exists():
        return False
    con = get_connection()
    try:
        if data_type == "option_month":
            result = con.execute(
                "SELECT COUNT(*) FROM sync_log "
                "WHERE date=? AND data_type=? AND ticker=? AND status='ok'",
                [date_str, data_type, TICKER],
            ).fetchone()[0]
        else:
            result = con.execute(
                "SELECT COUNT(*) FROM sync_log "
                "WHERE date=? AND data_type=? AND status='ok'",
                [date_str, data_type],
            ).fetchone()[0]
    except duckdb.CatalogException:
        return False
    finally:
        con.close()
    return result > 0


def compute_split_factor(date_str: str) -> float:
    """计算 TQQQ 在指定日期的前复权累积因子。

    因子 = ∏(split_from / split_to)，对所有 exec_date > date_str 的拆股事件累乘。
    """
    con = get_connection()
    try:
        rows = con.execute(
            "SELECT split_from, split_to FROM splits "
            "WHERE ticker = ? AND exec_date > CAST(? AS DATE) "
            "ORDER BY exec_date",
            [TICKER, date_str],
        ).fetchall()
    finally:
        con.close()
    factor = 1.0
    for split_from, split_to in rows:
        factor *= split_from / split_to
    return factor
