# tests/test_iv.py
import pytest
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
