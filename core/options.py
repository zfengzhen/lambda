"""OCC 期权合约解析与匹配。"""
import re

from config import TICKER

# OCC symbol 格式: O:{TICKER}{YYMMDD}{P|C}{STRIKE_8DIGITS}
_OCC_RE = re.compile(r"^O:([A-Z]+)(\d{6})([PC])(\d{8})$")


def parse_occ_symbol(symbol: str) -> dict:
    """解析 OCC 期权 symbol，返回 ticker/expiration/option_type/strike。

    Args:
        symbol: OCC 格式，如 "O:TQQQ260424P00030000"

    Returns:
        {"ticker": str, "expiration": "YYYY-MM-DD", "option_type": "P"|"C", "strike": float}

    Raises:
        ValueError: 格式不合法时
    """
    m = _OCC_RE.match(symbol)
    if not m:
        raise ValueError(f"Invalid OCC symbol: {symbol!r}")
    ticker, date6, opt_type, strike8 = m.groups()
    yy, mm, dd = date6[:2], date6[2:4], date6[4:6]
    expiration = f"20{yy}-{mm}-{dd}"
    strike = int(strike8) / 1000.0
    return {
        "ticker": ticker,
        "expiration": expiration,
        "option_type": opt_type,
        "strike": strike,
    }


def build_occ_symbol(expiry_date: str, strike: float,
                     option_type: str = "P") -> str:
    """构建 OCC 期权合约代码。

    Args:
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
    return f"O:{TICKER}{yy}{mm}{dd}{option_type}{strike_int:08d}"


def extract_strike(symbol: str) -> float:
    """从 OCC symbol 末 8 位提取 strike（千分之一美元单位）。"""
    return int(symbol[-8:]) / 1000.0


def extract_expiry(symbol: str) -> str:
    """从 OCC symbol 提取到期日 YYYY-MM-DD。"""
    p_idx = symbol.index("P") if "P" in symbol else symbol.index("C")
    date6 = symbol[p_idx - 6:p_idx]
    return f"20{date6[0:2]}-{date6[2:4]}-{date6[4:6]}"


def format_strike_str(strike: float) -> str:
    """格式化 strike 显示：整数显示整数（50），有小数显示小数（50.5）。"""
    return str(int(strike)) if strike == int(strike) else str(strike)


def match_option_contract(entry_date: str, expiry_date: str,
                          strike: float) -> dict | None:
    """查询匹配的期权合约，返回完整合约信息。

    封装 data.queries.query_option_on_date + OCC 解析。

    Returns:
        {symbol, display_symbol, occ_strike, occ_expiry, dte, price, vwap,
         open, high, low, close, volume} 或 None
    """
    from datetime import datetime
    from data.queries import query_option_on_date

    opt = query_option_on_date(entry_date, expiry_date, strike)
    if not opt:
        return None

    occ = opt["symbol"]
    occ_expiry = extract_expiry(occ)
    occ_strike = extract_strike(occ)
    dte = (datetime.strptime(occ_expiry, "%Y-%m-%d")
           - datetime.strptime(entry_date, "%Y-%m-%d")).days
    strike_str = format_strike_str(occ_strike)

    return {
        "symbol": occ,
        "display_symbol": f"{TICKER} {occ_expiry} P{strike_str}",
        "occ_strike": occ_strike,
        "occ_expiry": occ_expiry,
        "dte": dte,
        "price": round(opt["close"], 2),
        "vwap": round(opt["vwap"], 4),
        "open": opt["open"],
        "high": opt["high"],
        "low": opt["low"],
        "close": opt["close"],
        "volume": opt["volume"],
    }
