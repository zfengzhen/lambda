"""IV 计算与同步：Black-Scholes 反算 + 30天 ATM 包夹插值。"""
import math
import datetime as _dt
import logging

from scipy.stats import norm

from config import TICKER, RISK_FREE_RATE, IV_MIN_DTE, IV_TARGET_DAYS
from data.queries import (
    get_latest_iv_date, get_latest_option_date,
    get_earliest_option_date, get_option_dates_in_range,
    query_option_bars_for_iv, query_equity_bars,
)
from data.writers import upsert_ticker_iv

logger = logging.getLogger(__name__)

_IV_MIN = 0.01
_IV_MAX = 5.0


def _bs_price(spot, strike, tte, r, sigma, option_type):
    """Black-Scholes 正向定价。"""
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma**2) * tte) / (sigma * math.sqrt(tte))
    d2 = d1 - sigma * math.sqrt(tte)
    if option_type == "C":
        return spot * norm.cdf(d1) - strike * math.exp(-r * tte) * norm.cdf(d2)
    else:
        return strike * math.exp(-r * tte) * norm.cdf(-d2) - spot * norm.cdf(-d1)


def bs_implied_vol(price, spot, strike, tte, r, option_type,
                   max_iter=100, tol=1e-6):
    """二分法反算隐含波动率。无法收敛返回 float('nan')。"""
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
    return float("nan")


def _atm_iv(bars, spot, tte):
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


def compute_ticker_iv(option_bars, spot, date):
    """30 天 ATM IV：包夹 30 天的近月/远月插值。

    流程：
    1. 按到期日分组，排除 DTE ≤ 7 天
    2. 选包夹 30 天的两个到期日
    3. 在方差-时间空间线性插值到 30 天
    """
    if not option_bars:
        return float("nan")

    current = _dt.date.fromisoformat(date)
    by_expiry = {}
    expiry_dte = {}
    for bar in option_bars:
        exp = bar["expiration"]
        if exp not in expiry_dte:
            expiry_dte[exp] = (_dt.date.fromisoformat(exp) - current).days
        if expiry_dte[exp] > IV_MIN_DTE:
            by_expiry.setdefault(exp, []).append(bar)

    if not by_expiry:
        return float("nan")

    near = [(e, expiry_dte[e]) for e in by_expiry if expiry_dte[e] <= IV_TARGET_DAYS]
    far = [(e, expiry_dte[e]) for e in by_expiry if expiry_dte[e] > IV_TARGET_DAYS]

    if near and far:
        near_exp, near_dte = max(near, key=lambda x: x[1])
        far_exp, far_dte = min(far, key=lambda x: x[1])
        iv_near = _atm_iv(by_expiry[near_exp], spot, near_dte / 365.0)
        iv_far = _atm_iv(by_expiry[far_exp], spot, far_dte / 365.0)
        if math.isnan(iv_near) or math.isnan(iv_far):
            return iv_far if math.isnan(iv_near) else iv_near
        var_near = iv_near ** 2 * (near_dte / 365.0)
        var_far = iv_far ** 2 * (far_dte / 365.0)
        w = (far_dte - IV_TARGET_DAYS) / (far_dte - near_dte)
        var_30 = w * var_near + (1 - w) * var_far
        return math.sqrt(var_30 / (IV_TARGET_DAYS / 365.0))

    all_exps = near + far
    best_exp, best_dte = min(all_exps, key=lambda x: abs(x[1] - IV_TARGET_DAYS))
    return _atm_iv(by_expiry[best_exp], spot, best_dte / 365.0)


def sync_ticker_iv() -> None:
    """计算并存储 TQQQ 的 IV。空表全量，有数据增量。"""
    latest_iv = get_latest_iv_date()
    latest_opt = get_latest_option_date()

    if not latest_opt:
        logger.info("[iv] 无 option_bars 数据，跳过 IV 计算")
        return

    if latest_iv:
        from_date = str(_dt.date.fromisoformat(latest_iv) + _dt.timedelta(days=1))
    else:
        from_date = get_earliest_option_date()
        if not from_date:
            return

    to_date = latest_opt
    if from_date > to_date:
        logger.info("[iv] IV 已是最新")
        return

    logger.info(f"[iv] 计算 IV: {from_date} ~ {to_date}")

    dates = get_option_dates_in_range(from_date, to_date)

    iv_rows = []
    for d in dates:
        option_bars = query_option_bars_for_iv(d)
        eq = query_equity_bars(d, d)
        if not eq:
            continue
        spot = eq[0]["close"]
        result = compute_ticker_iv(option_bars, spot, d)
        if not math.isnan(result):
            iv_rows.append({"date": d, "ticker": TICKER, "iv": result})

    if iv_rows:
        upsert_ticker_iv(iv_rows)
        logger.info(f"[iv] 写入 {len(iv_rows)} 天 IV")
