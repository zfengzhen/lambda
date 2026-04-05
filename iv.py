"""标的级隐含波动率（IV）计算模块。

从 option_bars 数据反算 IV，VIX 风格加权汇总为标的级指标。
"""
import re
import math
import datetime as _dt
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
        tte:         剩余到期时间（年化，日历天/365）
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


# 排除到期日距当前日 ≤ MIN_DTE 天的合约
MIN_DTE = 7
# IV 过滤范围
_IV_MIN = 0.01
_IV_MAX = 5.0
# 目标期限天数
_TARGET_DAYS = 30


def _atm_iv(bars: list[dict], spot: float, tte: float) -> float:
    """单个到期日的 ATM IV：最接近 spot 的 Put+Call 各 1 档，取平均。"""
    ivs = []
    for opt_type in ("P", "C"):
        typed = [b for b in bars if b["option_type"] == opt_type]
        if not typed:
            continue
        atm = min(typed, key=lambda b: abs(b["strike"] - spot))
        val = bs_implied_vol(atm["close"], spot, atm["strike"],
                             tte, RISK_FREE_RATE, opt_type)
        if not math.isnan(val) and _IV_MIN <= val <= _IV_MAX:
            ivs.append(val)
    return sum(ivs) / len(ivs) if ivs else float("nan")


def compute_ticker_iv(option_bars: list[dict], spot: float,
                      date: str) -> float:
    """30 天 ATM IV：包夹 30 天的近月/远月插值。

    流程：
    1. 按到期日分组，排除 DTE ≤ 7 天的
    2. 选包夹 30 天的两个到期日（近月 DTE≤30，远月 DTE>30）
    3. 各取 ATM Put+Call 平均 IV
    4. 在方差-时间空间线性插值到 30 天

    降级：仅有一侧到期日时，取 DTE 最接近 30 天的单一到期日 ATM IV。

    Args:
        option_bars: option_bars 记录列表，需含 strike/expiration/option_type/close 字段
        spot:        标的当日收盘价
        date:        当前日期 "YYYY-MM-DD"

    Returns:
        标的级 IV（年化）；数据不足返回 float('nan')
    """
    if not option_bars:
        return float("nan")

    current = _dt.date.fromisoformat(date)

    # 按到期日分组，排除 DTE ≤ MIN_DTE
    by_expiry: dict[str, list[dict]] = {}
    expiry_dte: dict[str, int] = {}
    for bar in option_bars:
        exp = bar["expiration"]
        if exp not in expiry_dte:
            expiry_dte[exp] = (_dt.date.fromisoformat(exp) - current).days
        if expiry_dte[exp] > MIN_DTE:
            by_expiry.setdefault(exp, []).append(bar)

    if not by_expiry:
        return float("nan")

    # 分近月（DTE ≤ 30）和远月（DTE > 30）
    near = [(e, expiry_dte[e]) for e in by_expiry if expiry_dte[e] <= _TARGET_DAYS]
    far = [(e, expiry_dte[e]) for e in by_expiry if expiry_dte[e] > _TARGET_DAYS]

    if near and far:
        # 包夹插值：近月取 DTE 最大的，远月取 DTE 最小的
        near_exp, near_dte = max(near, key=lambda x: x[1])
        far_exp, far_dte = min(far, key=lambda x: x[1])
        iv_near = _atm_iv(by_expiry[near_exp], spot, near_dte / 365.0)
        iv_far = _atm_iv(by_expiry[far_exp], spot, far_dte / 365.0)
        if math.isnan(iv_near) or math.isnan(iv_far):
            # 降级：取能算出的那个
            return iv_far if math.isnan(iv_near) else iv_near
        # 方差-时间空间线性插值
        var_near = iv_near ** 2 * (near_dte / 365.0)
        var_far = iv_far ** 2 * (far_dte / 365.0)
        w = (far_dte - _TARGET_DAYS) / (far_dte - near_dte)
        var_30 = w * var_near + (1 - w) * var_far
        return math.sqrt(var_30 / (_TARGET_DAYS / 365.0))

    # 降级：仅有一侧，取 DTE 最接近 30 天的
    all_exps = near + far
    best_exp, best_dte = min(all_exps, key=lambda x: abs(x[1] - _TARGET_DAYS))
    return _atm_iv(by_expiry[best_exp], spot, best_dte / 365.0)
