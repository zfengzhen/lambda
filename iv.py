"""标的级隐含波动率（IV）计算模块。

从 option_bars 数据反算 IV，VIX 风格加权汇总为标的级指标。
"""
import re
import math
from scipy.stats import norm

RISK_FREE_RATE = 0.05

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


def _bs_price(spot: float, strike: float, tte: float,
              r: float, sigma: float, option_type: str) -> float:
    """Black-Scholes 正向定价。"""
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma**2) * tte) / (sigma * math.sqrt(tte))
    d2 = d1 - sigma * math.sqrt(tte)
    if option_type == "C":
        return spot * norm.cdf(d1) - strike * math.exp(-r * tte) * norm.cdf(d2)
    else:
        return strike * math.exp(-r * tte) * norm.cdf(-d2) - spot * norm.cdf(-d1)


def bs_implied_vol(price: float, spot: float, strike: float,
                   tte: float, r: float, option_type: str,
                   max_iter: int = 100, tol: float = 1e-6) -> float:
    """二分法反算隐含波动率。

    Args:
        price:       期权市场价格
        spot:        标的现价
        strike:      行权价
        tte:         剩余到期时间（年化，交易日/252）
        r:           无风险利率
        option_type: "P" 或 "C"
        max_iter:    最大迭代次数
        tol:         收敛容差

    Returns:
        隐含波动率（年化）；无法收敛时返回 float('nan')
    """
    if price <= 0 or tte <= 0 or spot <= 0 or strike <= 0:
        return float("nan")

    lo, hi = 0.01, 5.0
    for _ in range(max_iter):
        mid = (lo + hi) / 2
        calc = _bs_price(spot, strike, tte, r, mid, option_type)
        if abs(calc - price) < tol:
            return mid
        if calc > price:
            hi = mid
        else:
            lo = mid
    # 未收敛
    return float("nan")
