"""标的级隐含波动率（IV）计算模块。

从 option_bars 数据反算 IV，VIX 风格加权汇总为标的级指标。
"""
import re

# OCC symbol 格式: O:{TICKER}{YYMMDD}{P|C}{STRIKE_8DIGITS}
# 末尾固定 15 位: 6(日期) + 1(类型) + 8(strike) = 15
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
