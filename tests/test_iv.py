# tests/test_iv.py
import os
import pytest
import requests
from iv import parse_occ_symbol


class TestParseOccSymbol:
    def test_standard_put(self):
        result = parse_occ_symbol("O:TQQQ260424P00030000")
        assert result == {
            "ticker": "TQQQ",
            "expiration": "2026-04-24",
            "option_type": "P",
            "strike": 30.0,
        }

    def test_standard_call(self):
        result = parse_occ_symbol("O:QQQ260515C00450000")
        assert result == {
            "ticker": "QQQ",
            "expiration": "2026-05-15",
            "option_type": "C",
            "strike": 450.0,
        }

    def test_fractional_strike(self):
        """strike 38.5 → 00038500"""
        result = parse_occ_symbol("O:TQQQ250131P00038500")
        assert result == {
            "ticker": "TQQQ",
            "expiration": "2025-01-31",
            "option_type": "P",
            "strike": 38.5,
        }

    def test_adjusted_symbol_after_split(self):
        """拆股调整后 strike 19.25 → 00019250"""
        result = parse_occ_symbol("O:TQQQ250131P00019250")
        assert result == {
            "ticker": "TQQQ",
            "expiration": "2025-01-31",
            "option_type": "P",
            "strike": 19.25,
        }

    def test_single_char_ticker(self):
        """单字母 ticker（如 F）"""
        result = parse_occ_symbol("O:F260320C00012000")
        assert result == {
            "ticker": "F",
            "expiration": "2026-03-20",
            "option_type": "C",
            "strike": 12.0,
        }

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            parse_occ_symbol("INVALID")

    def test_missing_prefix_raises(self):
        with pytest.raises(ValueError):
            parse_occ_symbol("TQQQ260424P00030000")


import math
from scipy.stats import norm
from iv import bs_implied_vol


def _bs_price_test(spot, strike, tte, r, sigma, option_type):
    """Black-Scholes 正向定价，用于构造测试输入。"""
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma**2) * tte) / (sigma * math.sqrt(tte))
    d2 = d1 - sigma * math.sqrt(tte)
    if option_type == "C":
        return spot * norm.cdf(d1) - strike * math.exp(-r * tte) * norm.cdf(d2)
    else:
        return strike * math.exp(-r * tte) * norm.cdf(-d2) - spot * norm.cdf(-d1)


class TestBsImpliedVol:
    def test_roundtrip_call(self):
        """已知 IV=0.5，正向算价格，再反算回 IV，误差 < 0.001"""
        spot, strike, tte, r, sigma = 50.0, 50.0, 30/252, 0.05, 0.50
        price = _bs_price_test(spot, strike, tte, r, sigma, "C")
        iv = bs_implied_vol(price, spot, strike, tte, r, "C")
        assert abs(iv - sigma) < 0.001

    def test_roundtrip_put(self):
        spot, strike, tte, r, sigma = 50.0, 55.0, 60/252, 0.05, 0.80
        price = _bs_price_test(spot, strike, tte, r, sigma, "P")
        iv = bs_implied_vol(price, spot, strike, tte, r, "P")
        assert abs(iv - sigma) < 0.001

    def test_deep_otm_returns_nan(self):
        """价格超出 B-S 可达范围（sigma 上界 5.0 对应最高约 12.26），无法收敛返回 NaN"""
        iv = bs_implied_vol(20.0, 50.0, 80.0, 10/252, 0.05, "C")
        assert math.isnan(iv)

    def test_zero_price_returns_nan(self):
        iv = bs_implied_vol(0.0, 50.0, 50.0, 30/252, 0.05, "P")
        assert math.isnan(iv)

    def test_itm_call(self):
        """ITM call: spot=60, strike=50"""
        spot, strike, tte, r, sigma = 60.0, 50.0, 45/252, 0.05, 0.60
        price = _bs_price_test(spot, strike, tte, r, sigma, "C")
        iv = bs_implied_vol(price, spot, strike, tte, r, "C")
        assert abs(iv - sigma) < 0.001

    def test_high_vol(self):
        """高波动率（如 TQQQ 150%）"""
        spot, strike, tte, r, sigma = 30.0, 30.0, 30/252, 0.05, 1.50
        price = _bs_price_test(spot, strike, tte, r, sigma, "P")
        iv = bs_implied_vol(price, spot, strike, tte, r, "P")
        assert abs(iv - sigma) < 0.001


def _make_option_row(ticker, expiry, opt_type, strike, close, date):
    """构造一条 option_bars 风格的 dict（含已解析的结构化字段）。"""
    yy = expiry[2:4]
    mm = expiry[5:7]
    dd = expiry[8:10]
    strike_int = int(round(strike * 1000))
    symbol = f"O:{ticker}{yy}{mm}{dd}{opt_type}{strike_int:08d}"
    return {
        "date": date,
        "symbol": symbol,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 100,
        "transactions": 10,
        "strike": strike,
        "expiration": expiry,
        "option_type": opt_type,
    }


from iv import compute_ticker_iv


class TestComputeTickerIv:
    def test_bracket_interpolation(self):
        """包夹 30 天的近月/远月插值，验证 IV 在合理范围"""
        date = "2026-04-03"
        spot = 50.0
        # 近月 DTE=25（≤30），远月 DTE=39（>30），包夹 30 天
        expiry_near = "2026-04-28"
        expiry_far = "2026-05-12"

        bars = []
        sigma = 0.60
        r = 0.05
        for exp, dte_days in [(expiry_near, 25), (expiry_far, 39)]:
            tte = dte_days / 365
            for s in [48.0, 49.0, 50.0, 51.0, 52.0]:
                for opt_type in ["P", "C"]:
                    price = _bs_price_test(spot, s, tte, r, sigma, opt_type)
                    bars.append(_make_option_row("TEST", exp, opt_type, s, price, date))

        iv = compute_ticker_iv(bars, spot, date)
        assert abs(iv - 0.60) < 0.05

    def test_empty_bars_returns_nan(self):
        iv = compute_ticker_iv([], 50.0, "2026-04-03")
        assert math.isnan(iv)

    def test_single_expiry_fallback(self):
        """只有一个到期日时，降级为 ATM IV"""
        date = "2026-04-03"
        spot = 50.0
        expiry = "2026-04-17"  # DTE=14，仅近月
        sigma = 0.80
        r = 0.05
        tte = 14 / 365
        bars = []
        for s in [49.0, 50.0, 51.0]:
            for opt_type in ["P", "C"]:
                price = _bs_price_test(spot, s, tte, r, sigma, opt_type)
                bars.append(_make_option_row("TEST", expiry, opt_type, s, price, date))

        iv = compute_ticker_iv(bars, spot, date)
        assert abs(iv - 0.80) < 0.05

    def test_filters_anomalous_iv(self):
        """含异常合约（深度 OTM 极低价格），不应影响 ATM 结果"""
        date = "2026-04-03"
        spot = 50.0
        expiry = "2026-05-03"  # DTE=30
        sigma = 0.60
        r = 0.05
        tte = 30 / 365
        bars = []
        for s in [49.0, 50.0, 51.0]:
            for opt_type in ["P", "C"]:
                price = _bs_price_test(spot, s, tte, r, sigma, opt_type)
                bars.append(_make_option_row("TEST", expiry, opt_type, s, price, date))
        # 深度 OTM 异常合约，不是 ATM 所以不影响
        bars.append(_make_option_row("TEST", expiry, "C", 80.0, 0.001, date))

        iv = compute_ticker_iv(bars, spot, date)
        assert abs(iv - 0.60) < 0.05


# ── B: 硬编码真实数据回归测试 ──────────────────────────────────

import logging

logger = logging.getLogger(__name__)


class TestBsImpliedVolRegression:
    """用真实市场数据验证 B-S 反算精度。

    数据来源：Massive Option Contract Snapshot API 返回的 implied_volatility
    作为基准，对比我们用同一合约的 close 价格反算的 IV。
    容差 15%（相对误差），因为：
    - Snapshot IV 基于 mid price，我们用 close（可能偏离 mid）
    - 时间戳可能有微小差异
    """

    # (symbol, spot, option_price, api_iv, option_type, dte_calendar_days)
    # 数据来源：Massive Option Contract Snapshot API (2026-04-03)
    CASES = [
        # TQQQ ATM Call, expiry 2026-04-24, snapshot 2026-04-03
        ("O:TQQQ260424C00043000", 43.33, 2.84, 0.681904, "C", 21),
    ]

    @pytest.mark.parametrize("symbol,spot,price,api_iv,opt_type,dte", CASES)
    def test_iv_matches_api(self, symbol, spot, price, api_iv, opt_type, dte):
        tte = dte / 365.0
        strike = parse_occ_symbol(symbol)["strike"]
        our_iv = bs_implied_vol(price, spot, strike, tte, 0.05, opt_type)
        assert not math.isnan(our_iv), f"反算失败: {symbol}"
        rel_error = abs(our_iv - api_iv) / api_iv
        logger.info(
            f"[回归] {symbol}: spot={spot}, strike={strike}, price={price:.4f}, "
            f"dte={dte}, api_iv={api_iv:.4f}, our_iv={our_iv:.4f}, "
            f"rel_error={rel_error:.2%}"
        )
        assert rel_error < 0.15, (
            f"{symbol}: our_iv={our_iv:.4f}, api_iv={api_iv:.4f}, "
            f"rel_error={rel_error:.2%}"
        )


# ── A: 在线 Snapshot API 验证 ──────────────────────────────────

def _fetch_snapshot(ticker: str, symbol: str, api_key: str) -> dict | None:
    """调用 Massive Option Contract Snapshot API。网络异常返回 None。"""
    url = f"https://api.massive.com/v3/snapshot/options/{ticker}/{symbol}"
    try:
        resp = requests.get(url, params={"apiKey": api_key}, timeout=15)
    except (requests.ConnectionError, requests.Timeout):
        return None
    if resp.status_code != 200:
        return None
    data = resp.json()
    results = data.get("results")
    if not results or "implied_volatility" not in results:
        return None
    return results


def _fetch_prev_close(ticker: str, api_key: str) -> float | None:
    """获取标的前日收盘价。网络异常返回 None。"""
    url = f"https://api.massive.com/v2/aggs/ticker/{ticker}/prev"
    try:
        resp = requests.get(url, params={"apiKey": api_key}, timeout=15)
    except (requests.ConnectionError, requests.Timeout):
        return None
    if resp.status_code != 200:
        return None
    data = resp.json()
    results = data.get("results", [])
    if not results:
        return None
    return results[0].get("c")


@pytest.mark.online
class TestSnapshotIvValidation:
    """调用 Massive Snapshot API，对比实时 IV 和我们的 B-S 反算。

    需要环境变量 MASSIVE_API_KEY。
    运行方式: pytest tests/test_iv.py -m online -v -s --log-cli-level=INFO
    """

    TICKER = "TQQQ"

    @pytest.fixture(autouse=True)
    def _require_api_key(self):
        self.api_key = os.environ.get("MASSIVE_API_KEY", "")
        if not self.api_key:
            pytest.skip("MASSIVE_API_KEY 未设置")

    def test_single_contract_iv(self):
        """拿一个 ATM 合约，对比 Snapshot IV 和 B-S 反算 IV。"""
        import datetime as dt
        from data_store import build_occ_symbol

        # 取 2-4 周后的周五作为到期日
        today = dt.date.today()
        target = today + dt.timedelta(days=21)
        days_to_friday = (4 - target.weekday()) % 7
        expiry_date = target + dt.timedelta(days=days_to_friday)
        expiry = str(expiry_date)

        # 拿 stock 前日收盘价
        spot = _fetch_prev_close(self.TICKER, self.api_key)
        if spot is None:
            pytest.skip(f"无法获取 {self.TICKER} 价格（网络超时或无数据）")

        # 构造 ATM Call
        strike = round(spot)
        symbol = build_occ_symbol(self.TICKER, expiry, float(strike), "C")

        # 调 Snapshot API
        snapshot = _fetch_snapshot(self.TICKER, symbol, self.api_key)
        if not snapshot:
            pytest.skip(f"Snapshot 无数据或网络超时: {symbol}")

        api_iv = snapshot["implied_volatility"]
        # 优先用 day close（和我们的计算基准一致）
        day = snapshot.get("day", {})
        price = day.get("close", 0)
        if price <= 0:
            # fallback: last_quote mid
            quote = snapshot.get("last_quote") or {}
            bid, ask = quote.get("bid", 0), quote.get("ask", 0)
            price = (bid + ask) / 2 if bid > 0 and ask > 0 else 0

        if price <= 0 or api_iv <= 0:
            pytest.skip(f"价格或 IV 无效: price={price}, iv={api_iv}")

        dte = (expiry_date - today).days
        tte = dte / 365.0
        our_iv = bs_implied_vol(price, spot, float(strike), tte, 0.05, "C")

        assert not math.isnan(our_iv), f"B-S 反算失败: price={price}, spot={spot}"
        rel_error = abs(our_iv - api_iv) / api_iv

        logger.info(
            f"\n  [在线验证] {symbol}\n"
            f"  spot={spot}, strike={strike}, price={price:.4f}\n"
            f"  dte={dte}, api_iv={api_iv:.4f}, our_iv={our_iv:.4f}\n"
            f"  rel_error={rel_error:.2%}\n"
            f"  --- 可填入 CASES 的回归数据 ---\n"
            f"  (\"{symbol}\", {spot}, {price}, {api_iv:.6f}, \"C\", {dte}),"
        )

        assert rel_error < 0.15, (
            f"IV 偏差过大: our={our_iv:.4f} vs api={api_iv:.4f}, "
            f"error={rel_error:.2%}"
        )
